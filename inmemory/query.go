package inmemory

import (
	"math/big"
	"slices"
	"strings"
	"time"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

// indexPlan describes one safe scan. The complete selector remains authoritative.
type indexPlan struct {
	index   *objectIndex
	bounds  indexRange
	prefix  int
	ranged  bool
	ordered bool
	reverse bool
}

type indexConstraint struct {
	equal  *indexValue
	bounds indexRange
}

// scalarKind reports the common non-null type, or false for mixed/compound data.
func (f indexField) scalarKind() (indexKind, bool) {
	kind := indexNull
	for candidate := indexBool; candidate < indexKindCount; candidate++ {
		if f.kinds[candidate] == 0 {
			continue
		}
		if kind != indexNull || candidate == indexCompound {
			return indexNull, false
		}
		kind = candidate
	}
	return kind, true
}

// operand converts only comparisons equivalent to the stored column order.
func (f indexField) operand(input any) (indexValue, bool) {
	value := newIndexValue(input)
	if value.kind == indexNull {
		return value, true
	}
	kind, homogeneous := f.scalarKind()
	if !homogeneous {
		return indexValue{}, false
	}
	if kind == indexString {
		if value.kind != indexString {
			return indexValue{}, false
		}
		// Selector interprets numeric, boolean and time strings by value. Their
		// equivalent spellings need not occupy one lexical interval.
		if _, ok := new(big.Rat).SetString(value.text); ok {
			return indexValue{}, false
		}
		if value.text == "true" || value.text == "false" {
			return indexValue{}, false
		}
		if _, err := time.Parse(time.RFC3339Nano, value.text); err != nil {
			return value, true
		}
		return indexValue{}, false
	}
	if kind == value.kind {
		return value, true
	}
	if value.kind != indexString {
		return indexValue{}, false
	}
	switch kind {
	case indexNumber:
		number, ok := new(big.Rat).SetString(value.text)
		return indexValue{kind: indexNumber, number: number}, ok
	case indexTime:
		at, err := time.Parse(time.RFC3339Nano, value.text)
		if err != nil {
			return indexValue{}, false
		}
		return indexValue{kind: indexTime, instant: at}, true
	case indexBool:
		if value.text == "true" || value.text == "false" {
			return indexValue{kind: indexBool, text: value.text}, true
		}
	}
	return indexValue{}, false
}

func (c *indexConstraint) add(operator selector.Operator, value indexValue) {
	switch operator {
	case selector.Equals, selector.DoubleEquals:
		c.equal = &value
	case selector.GreaterThan, selector.GreaterThanOrEqual:
		inclusive := operator == selector.GreaterThanOrEqual
		if bound := c.bounds.lower; bound != nil {
			order := value.compare(bound.values[0])
			if order < 0 || order == 0 && inclusive {
				return
			}
		}
		c.bounds.lower = &indexBound{values: []indexValue{value}, inclusive: inclusive}
	case selector.LessThan, selector.LessThanOrEqual:
		inclusive := operator == selector.LessThanOrEqual
		if bound := c.bounds.upper; bound != nil {
			order := value.compare(bound.values[0])
			if order > 0 || order == 0 && inclusive {
				return
			}
		}
		c.bounds.upper = &indexBound{values: []indexValue{value}, inclusive: inclusive}
	}
}

func (f indexField) constraint(requirements store.Requirements) indexConstraint {
	var result indexConstraint
	for _, requirement := range requirements {
		switch requirement.Operator {
		case selector.Equals, selector.DoubleEquals, selector.GreaterThan, selector.GreaterThanOrEqual, selector.LessThan, selector.LessThanOrEqual:
			if value, ok := f.operand(requirement.Values[0]); ok {
				result.add(requirement.Operator, value)
			}
		case selector.In:
			values := make([]indexValue, 0, len(requirement.Values))
			for _, input := range requirement.Values {
				value, ok := f.operand(input)
				if !ok {
					break
				}
				values = append(values, value)
			}
			if len(values) == 0 || len(values) != len(requirement.Values) {
				continue
			}
			slices.SortFunc(values, indexValue.compare)
			if values[0].compare(values[len(values)-1]) == 0 {
				result.add(selector.Equals, values[0])
			} else {
				result.add(selector.GreaterThanOrEqual, values[0])
				result.add(selector.LessThanOrEqual, values[len(values)-1])
			}
		}
	}
	return result
}

func collectConstraints(requirements store.Requirements, labels bool, into map[string]store.Requirements) {
	for _, requirement := range requirements {
		switch requirement.Operator {
		case selector.And:
			collectConstraints(requirement.Requirements, labels, into)
		case selector.Equals, selector.DoubleEquals, selector.In, selector.GreaterThan, selector.GreaterThanOrEqual, selector.LessThan, selector.LessThanOrEqual:
			name := requirement.Key
			if labels {
				// A label is one literal map key. Only simple keys have an equivalent
				// dotted model path; dotted or slash-containing keys remain residuals.
				if strings.Contains(name, ".") || !store.FieldPathPattern.MatchString(name) {
					continue
				}
				name = "labels." + name
			}
			into[name] = append(into[name], requirement)
		}
	}
}

func (i *objectIndex) plan(scopes []store.Scope, options store.ListOptions, predicates map[string]store.Requirements, sorts []meta.SortField) indexPlan {
	constraints := make([]indexConstraint, len(i.fields))
	for n, field := range i.fields {
		if field.scope >= 0 {
			if field.scope < len(scopes) {
				value := newIndexValue(scopes[field.scope].Name)
				constraints[n].equal = &value
			} else if !options.IncludeSubScopes {
				constraints[n].equal = &indexValue{}
			}
		} else {
			constraints[n] = field.constraint(predicates[i.definition.Fields[n]])
		}
	}
	plan := indexPlan{index: i}
	var prefix []indexValue
	for _, constraint := range constraints {
		if constraint.equal != nil {
			prefix = append(prefix, *constraint.equal)
			plan.prefix++
			continue
		}
		if constraint.bounds.lower != nil || constraint.bounds.upper != nil {
			plan.ranged = true
			plan.bounds = constraint.bounds
		}
		// A lexicographic interval can constrain only the first non-equality column.
		break
	}
	if plan.bounds.lower != nil {
		plan.bounds.lower = &indexBound{values: append(slices.Clone(prefix), plan.bounds.lower.values...), inclusive: plan.bounds.lower.inclusive}
	} else if len(prefix) > 0 {
		plan.bounds.lower = &indexBound{values: prefix, inclusive: true}
	}
	if plan.bounds.upper != nil {
		plan.bounds.upper = &indexBound{values: append(slices.Clone(prefix), plan.bounds.upper.values...), inclusive: plan.bounds.upper.inclusive}
	} else if len(prefix) > 0 {
		plan.bounds.upper = &indexBound{values: prefix, inclusive: true}
	}
	plan.ordered, plan.reverse = i.order(sorts, constraints, !options.IncludeSubScopes)
	return plan
}

func (i *objectIndex) order(sorts []meta.SortField, constraints []indexConstraint, exactScope bool) (bool, bool) {
	column := 0
	direction := meta.SortDirection("")
	for _, sort := range sorts {
		position := slices.Index(i.definition.Fields, sort.Field)
		if position >= 0 && i.fields[position].scope < 0 && constraints[position].equal != nil {
			continue
		}
		for column < len(constraints) && constraints[column].equal != nil {
			column++
		}
		if column == len(i.fields) || i.definition.Fields[column] != sort.Field || i.fields[column].scope >= 0 {
			return false, false
		}
		if _, scalar := i.fields[column].scalarKind(); !scalar {
			return false, false
		}
		if direction != "" && direction != sort.Direction {
			return false, false
		}
		direction = sort.Direction
		// Within an exact scope, ID already breaks every tie, including the UID
		// tie-break appended by Store pagination.
		if sort.Field == "id" && exactScope {
			return true, direction == meta.SortDirectionDesc
		}
		column++
	}
	return true, direction == meta.SortDirectionDesc
}

func (c *collection) plan(scopes []store.Scope, options store.ListOptions, sorts []meta.SortField) indexPlan {
	predicates := map[string]store.Requirements{}
	collectConstraints(options.FieldRequirements, false, predicates)
	collectConstraints(options.LabelRequirements, true, predicates)
	best := indexPlan{ordered: len(sorts) == 0}
	bestColumns := 0
	for _, index := range c.indexes {
		plan := index.plan(scopes, options, predicates, sorts)
		columns := plan.prefix
		if plan.ranged {
			columns++
		}
		useful := columns > 0 || len(sorts) > 0 && plan.ordered
		if !useful {
			continue
		}
		if best.index == nil || columns > bestColumns || columns == bestColumns && (plan.ordered && !best.ordered || plan.ordered == best.ordered && plan.prefix > best.prefix) {
			best = plan
			bestColumns = columns
		}
	}
	return best
}
