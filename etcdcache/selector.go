package etcdcache

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apiserver/pkg/storage"
	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/common/store"
)

func ConvertPredicate(l store.Requirements, f store.Requirements) (storage.SelectionPredicate, error) {
	if err := store.ValidateSelectorRequirements(l, f); err != nil {
		return storage.SelectionPredicate{}, commonerrors.NewBadRequest(err.Error())
	}
	labelssel := labels.Everything()
	fieldsel := fields.Everything()
	if l != nil {
		newlabelssel, err := requirementsToLabelsSelector(l)
		if err != nil {
			return storage.SelectionPredicate{}, err
		}
		labelssel = newlabelssel
	}
	if f != nil {
		newfieldsel, err := requirementsToFieldsSelector(f)
		if err != nil {
			return storage.SelectionPredicate{}, err
		}
		fieldsel = newfieldsel
	}
	fieldkeys := requirementKeys(f)
	return storage.SelectionPredicate{
		Label:    labelssel,
		Field:    fieldsel,
		GetAttrs: GetAttrsFunc(fieldkeys),
	}, nil
}

func requirementsToLabelsSelector(reqs store.Requirements) (labels.Selector, error) {
	return requirementLabelSelector{requirements: reqs}, nil
}

type requirementLabelSelector struct {
	requirements store.Requirements
}

func (s requirementLabelSelector) Matches(values labels.Labels) bool {
	labelsMap := make(map[string]string, len(requirementKeys(s.requirements)))
	for _, key := range requirementKeys(s.requirements) {
		if values.Has(key) {
			labelsMap[key] = values.Get(key)
		}
	}
	return selector.RequirementsMatchLabels(s.requirements, labelsMap)
}

func (s requirementLabelSelector) Empty() bool { return len(s.requirements) == 0 }

func (s requirementLabelSelector) String() string { return s.requirements.String() }

func (s requirementLabelSelector) Add(requirements ...labels.Requirement) labels.Selector {
	result := requirementLabelSelector{requirements: cloneRequirements(s.requirements)}
	for _, requirement := range requirements {
		result.requirements = append(result.requirements, selector.Requirement{
			Key:      requirement.Key(),
			Operator: selector.Operator(requirement.Operator()),
			Values:   store.StringsToAny(requirement.Values().List()),
		})
	}
	return result
}

func (s requirementLabelSelector) Requirements() (labels.Requirements, bool) { return nil, true }

func (s requirementLabelSelector) DeepCopySelector() labels.Selector {
	return requirementLabelSelector{requirements: cloneRequirements(s.requirements)}
}

func (s requirementLabelSelector) RequiresExactMatch(key string) (string, bool) {
	return requirementsExactMatch(s.requirements, key)
}

var _ labels.Selector = requirementLabelSelector{}

func requirementsToFieldsSelector(reqs store.Requirements) (fields.Selector, error) {
	if requirementOperatorExists(reqs, selector.Contains, selector.Like) {
		return nil, commonerrors.NewUnsupported("etcd cache does not support Contains or Like field requirements")
	}
	return requirementFieldSelector{requirements: stringifyRequirements(reqs)}, nil
}

type requirementFieldSelector struct {
	requirements store.Requirements
}

func (s requirementFieldSelector) Matches(values fields.Fields) bool {
	fieldsMap := make(map[string]string, len(requirementKeys(s.requirements)))
	for _, key := range requirementKeys(s.requirements) {
		if values.Has(key) {
			fieldsMap[key] = values.Get(key)
		}
	}
	return selector.RequirementsMatchLabels(s.requirements, fieldsMap)
}

func (s requirementFieldSelector) Empty() bool {
	return len(s.requirements) == 0
}

func (s requirementFieldSelector) RequiresExactMatch(field string) (string, bool) {
	return requirementsExactMatch(s.requirements, field)
}

func (s requirementFieldSelector) Transform(fn fields.TransformFunc) (fields.Selector, error) {
	transformed, err := transformRequirements(s.requirements, fn)
	if err != nil {
		return nil, err
	}
	return requirementFieldSelector{requirements: transformed}, nil
}

func (s requirementFieldSelector) Requirements() fields.Requirements { return nil }

func (s requirementFieldSelector) String() string { return s.requirements.String() }

func (s requirementFieldSelector) DeepCopySelector() fields.Selector {
	return requirementFieldSelector{requirements: cloneRequirements(s.requirements)}
}

var _ fields.Selector = requirementFieldSelector{}

func requirementKeys(requirements store.Requirements) []string {
	seen := map[string]struct{}{}
	var collect func(store.Requirements)
	collect = func(current store.Requirements) {
		for _, requirement := range current {
			if requirement.Key != "" {
				seen[requirement.Key] = struct{}{}
			}
			collect(requirement.Requirements)
		}
	}
	collect(requirements)
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func requirementOperatorExists(requirements store.Requirements, operators ...selector.Operator) bool {
	for _, requirement := range requirements {
		if slices.Contains(operators, requirement.Operator) || requirementOperatorExists(requirement.Requirements, operators...) {
			return true
		}
	}
	return false
}

func cloneRequirements(requirements store.Requirements) store.Requirements {
	cloned := make(store.Requirements, len(requirements))
	for index, requirement := range requirements {
		cloned[index] = requirement
		cloned[index].Values = append([]any(nil), requirement.Values...)
		cloned[index].Requirements = cloneRequirements(requirement.Requirements)
	}
	return cloned
}

func stringifyRequirements(requirements store.Requirements) store.Requirements {
	result := cloneRequirements(requirements)
	for index := range result {
		result[index].Requirements = stringifyRequirements(result[index].Requirements)
		for valueIndex, value := range result[index].Values {
			result[index].Values[valueIndex] = store.AnyToString(value)
		}
	}
	return result
}

func requirementsExactMatch(requirements store.Requirements, key string) (string, bool) {
	for _, requirement := range requirements {
		switch requirement.Operator {
		case selector.Equals, selector.DoubleEquals:
			if requirement.Key == key {
				return store.AnyToString(requirement.Values[0]), true
			}
		case selector.And:
			if value, found := requirementsExactMatch(requirement.Requirements, key); found {
				return value, true
			}
		}
	}
	return "", false
}

func transformRequirements(requirements store.Requirements, fn fields.TransformFunc) (store.Requirements, error) {
	transformed := cloneRequirements(requirements)
	for index := range transformed {
		requirement := &transformed[index]
		if len(requirement.Requirements) != 0 {
			children, err := transformRequirements(requirement.Requirements, fn)
			if err != nil {
				return nil, err
			}
			requirement.Requirements = children
			continue
		}
		if requirement.Key == "" {
			continue
		}
		value := ""
		if len(requirement.Values) == 1 {
			value = store.AnyToString(requirement.Values[0])
		}
		field, transformedValue, err := fn(requirement.Key, value)
		if err != nil {
			return nil, err
		}
		if field == "" && transformedValue == "" {
			*requirement = selector.Requirement{Operator: selector.All}
			continue
		}
		requirement.Key = field
		if len(requirement.Values) == 1 {
			requirement.Values = []any{transformedValue}
		}
	}
	return transformed, nil
}

type fieldPresenceTerm struct {
	field  string
	exists bool
}

func (p fieldPresenceTerm) DeepCopySelector() fields.Selector {
	return p
}

func (p fieldPresenceTerm) Empty() bool {
	return false
}

func (p fieldPresenceTerm) Matches(values fields.Fields) bool {
	return values.Has(p.field) == p.exists
}

func (p fieldPresenceTerm) Requirements() fields.Requirements {
	operator := selection.DoesNotExist
	if p.exists {
		operator = selection.Exists
	}
	return fields.Requirements{{Field: p.field, Operator: operator}}
}

func (p fieldPresenceTerm) RequiresExactMatch(string) (string, bool) {
	return "", false
}

func (p fieldPresenceTerm) String() string {
	if p.exists {
		return p.field
	}
	return "!" + p.field
}

func (p fieldPresenceTerm) Transform(fn fields.TransformFunc) (fields.Selector, error) {
	field, _, err := fn(p.field, "")
	if err != nil {
		return nil, err
	}
	if field == "" {
		return fields.Everything(), nil
	}
	p.field = field
	return p, nil
}

var _ fields.Selector = fieldPresenceTerm{}

type fieldComparisonTerm struct {
	requirement selector.Requirement
}

func (term fieldComparisonTerm) DeepCopySelector() fields.Selector {
	term.requirement.Values = append([]any(nil), term.requirement.Values...)
	return term
}

func (term fieldComparisonTerm) Empty() bool {
	return false
}

func (term fieldComparisonTerm) Matches(values fields.Fields) bool {
	fieldsMap := map[string]string{}
	if values.Has(term.requirement.Key) {
		fieldsMap[term.requirement.Key] = values.Get(term.requirement.Key)
	}
	return selector.RequirementMatchLabels(term.requirement, fieldsMap)
}

func (term fieldComparisonTerm) Requirements() fields.Requirements {
	return fields.Requirements{{
		Field:    term.requirement.Key,
		Operator: selection.Operator(term.requirement.Operator),
		Value:    store.AnyToString(term.requirement.Values[0]),
	}}
}

func (term fieldComparisonTerm) RequiresExactMatch(string) (string, bool) {
	return "", false
}

func (term fieldComparisonTerm) String() string {
	return term.requirement.String()
}

func (term fieldComparisonTerm) Transform(fn fields.TransformFunc) (fields.Selector, error) {
	field, value, err := fn(term.requirement.Key, store.AnyToString(term.requirement.Values[0]))
	if err != nil {
		return nil, err
	}
	if field == "" && value == "" {
		return fields.Everything(), nil
	}
	term.requirement.Key = field
	term.requirement.Values = []any{value}
	return term, nil
}

var _ fields.Selector = fieldComparisonTerm{}

func OneTermInSelector(key string, values []string) fields.Selector {
	return inTerm{field: key, values: values}
}

type inTerm struct {
	field  string
	values []string
}

// DeepCopySelector implements [fields.Selector].
func (i inTerm) DeepCopySelector() fields.Selector {
	valuesCopy := make([]string, len(i.values))
	copy(valuesCopy, i.values)
	return inTerm{field: i.field, values: valuesCopy}
}

// Empty implements [fields.Selector].
func (i inTerm) Empty() bool {
	return len(i.values) == 0
}

// Matches implements [fields.Selector].
func (i inTerm) Matches(fields fields.Fields) bool {
	return slices.Contains(i.values, fields.Get(i.field))
}

// Requirements implements [fields.Selector].
func (i inTerm) Requirements() fields.Requirements {
	return fields.Requirements{
		fields.Requirement{Field: i.field, Operator: selection.In, Value: strings.Join(i.values, ",")},
	}
}

// RequiresExactMatch implements [fields.Selector].
func (i inTerm) RequiresExactMatch(field string) (value string, found bool) {
	if i.field != field || len(i.values) != 1 {
		return "", false
	}
	return i.values[0], true
}

// String implements [fields.Selector].
func (i inTerm) String() string {
	return fmt.Sprintf("%s in (%s)", i.field, strings.Join(i.values, ","))
}

// Transform implements [fields.Selector].
func (i inTerm) Transform(fn fields.TransformFunc) (fields.Selector, error) {
	newfield, _, err := fn(i.field, "")
	if err != nil {
		return nil, err
	}
	if len(newfield) == 0 {
		return fields.Everything(), nil
	}
	return inTerm{field: newfield, values: i.values}, nil
}

var _ fields.Selector = inTerm{}

func OneTermNotInSelector(key string, values []string) fields.Selector {
	return notInTerm{field: key, values: values}
}

type notInTerm struct {
	field  string
	values []string
}

// DeepCopySelector implements [fields.Selector].
func (n notInTerm) DeepCopySelector() fields.Selector {
	valuesCopy := make([]string, len(n.values))
	copy(valuesCopy, n.values)
	return notInTerm{field: n.field, values: valuesCopy}
}

// Empty implements [fields.Selector].
func (n notInTerm) Empty() bool {
	return len(n.values) == 0
}

// Matches implements [fields.Selector].
func (n notInTerm) Matches(fields fields.Fields) bool {
	return !slices.Contains(n.values, fields.Get(n.field))
}

// Requirements implements [fields.Selector].
func (n notInTerm) Requirements() fields.Requirements {
	return fields.Requirements{
		fields.Requirement{Field: n.field, Operator: selection.NotIn, Value: strings.Join(n.values, ",")},
	}
}

// RequiresExactMatch implements [fields.Selector].
func (n notInTerm) RequiresExactMatch(string) (string, bool) {
	return "", false
}

// String implements [fields.Selector].
func (n notInTerm) String() string {
	return fmt.Sprintf("%s notin (%s)", n.field, strings.Join(n.values, ","))
}

// Transform implements [fields.Selector].
func (n notInTerm) Transform(fn fields.TransformFunc) (fields.Selector, error) {
	newfield, _, err := fn(n.field, "")
	if err != nil {
		return nil, err
	}
	if len(newfield) == 0 {
		return fields.Everything(), nil
	}
	return notInTerm{field: newfield, values: n.values}, nil
}

var _ fields.Selector = notInTerm{}
