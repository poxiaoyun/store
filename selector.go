package store

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"xiaoshiai.cn/common/selector"
)

// Requirements is the Store-facing alias for shared selector requirements.
type Requirements = selector.Requirements

// NewCreationRangeRequirement constructs the non-zero inclusive creation-time
// bounds in start-to-end order.
func NewCreationRangeRequirement(start, end time.Time) Requirements {
	result := make(Requirements, 0, 2)
	if !start.IsZero() {
		result = append(result, selector.NewRequirement("creationTimestamp", selector.GreaterThanOrEqual, start))
	}
	if !end.IsZero() {
		result = append(result, selector.NewRequirement("creationTimestamp", selector.LessThanOrEqual, end))
	}
	return result
}

// LabelsSelectorToReqirements converts a flat Kubernetes label selector.
func LabelsSelectorToReqirements(source labels.Selector) Requirements {
	requirements, _ := source.Requirements()
	result := make(Requirements, 0, len(requirements))
	for _, requirement := range requirements {
		values := requirement.Values().List()
		operands := make([]any, len(values))
		for index, value := range values {
			operands[index] = value
		}
		result = append(result, selector.Requirement{
			Key:      requirement.Key(),
			Operator: selector.Operator(requirement.Operator()),
			Values:   operands,
		})
	}
	return result
}

// FieldsSelectorToReqirements converts a flat Kubernetes field selector.
func FieldsSelectorToReqirements(source fields.Selector) Requirements {
	requirements := source.Requirements()
	result := make(Requirements, 0, len(requirements))
	for _, requirement := range requirements {
		result = append(result, selector.Requirement{
			Key:      requirement.Field,
			Operator: selector.Operator(requirement.Operator),
			Values:   []any{requirement.Value},
		})
	}
	return result
}

// ValidateSelectorRequirements verifies label and field requirement trees.
func ValidateSelectorRequirements(labelRequirements, fieldRequirements Requirements) error {
	if err := labelRequirements.Validate(); err != nil {
		return fmt.Errorf("label requirements: %w", err)
	}
	if err := fieldRequirements.Validate(); err != nil {
		return fmt.Errorf("field requirements: %w", err)
	}
	return nil
}

// MatchLabelReqirements reports whether an object's labels satisfy every
// requirement. The requirements must have passed Requirements.Validate before
// repeated use.
func MatchLabelReqirements(obj Object, requirements Requirements) bool {
	if obj == nil {
		return false
	}
	return selector.RequirementsMatchLabels(requirements, obj.GetLabels())
}

// MatchLabels reports whether an object has every exact label pair.
func MatchLabels(obj Object, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}
	target := obj.GetLabels()
	if len(target) == 0 {
		return false
	}
	for key, value := range labels {
		if target[key] != value {
			return false
		}
	}
	return true
}

// MatchUnstructuredFieldRequirments reports whether an object satisfies every
// field requirement. The requirements must have passed Requirements.Validate
// before repeated use.
func MatchUnstructuredFieldRequirments(obj *Unstructured, requirements Requirements) bool {
	if len(requirements) == 0 {
		return true
	}
	if obj == nil {
		return false
	}
	return requirements.Match(func(key string) (any, bool) {
		return GetNestedField(obj.Object, strings.Split(key, ".")...)
	})
}
