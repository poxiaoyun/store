// Package selector defines, parses and evaluates recursive selection expressions.
package selector

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"xiaoshiai.cn/common/meta"
)

// Requirements is an implicit conjunction of recursive selection requirements.
// An empty value matches every object.
type Requirements []Requirement

// RequirementEqual constructs one equality leaf requirement.
func RequirementEqual(key string, value any) Requirement {
	return Requirement{
		Key:      key,
		Operator: Equals,
		Values:   []any{value},
	}
}

// NewRequirement constructs one leaf requirement. Composite and constant
// requirements are constructed with Requirement fields directly.
func NewRequirement(key string, operator Operator, values ...any) Requirement {
	return Requirement{Key: key, Operator: operator, Values: values}
}

// Operator defines one requirement node operation.
type Operator string

const (
	// None matches no value and is the zero-value operator.
	None Operator = ""
	// All matches every value.
	All Operator = "all"
	// And requires every child requirement to match.
	And Operator = "and"
	// Or requires at least one child requirement to match.
	Or Operator = "or"
	// Not negates its single child requirement.
	Not Operator = "not"
	// DoesNotExist matches when Key is absent.
	DoesNotExist Operator = "!"
	// Equals matches when Key exists and equals the single value.
	Equals Operator = "="
	// DoubleEquals has the same matching semantics as Equals.
	DoubleEquals Operator = "=="
	// In matches when Key exists and equals any supplied value.
	In Operator = "in"
	// NotEquals matches a missing Key or a value unequal to the single value.
	NotEquals Operator = "!="
	// NotIn with values matches a missing Key or a value unequal to every supplied value.
	// An empty value set matches no object.
	NotIn Operator = "notin"
	// Exists matches when Key is present, including when its value is empty.
	Exists Operator = "exists"
	// GreaterThan matches when Key compares greater than the single value.
	GreaterThan Operator = "gt"
	// LessThan matches when Key compares less than the single value.
	LessThan Operator = "lt"
	// GreaterThanOrEqual matches when Key compares greater than or equal to the single value.
	GreaterThanOrEqual Operator = "gte"
	// LessThanOrEqual matches when Key compares less than or equal to the single value.
	LessThanOrEqual Operator = "lte"
	// Contains requires a string to contain every substring or a collection to contain every value.
	Contains Operator = "contains"
	// Like matches when a string contains the single substring.
	Like Operator = "like"
)

// Requirement is one recursive boolean selection condition. Its zero value is
// None and matches no object.
type Requirement struct {
	// Operator determines whether this is a constant, composite, or leaf node.
	Operator Operator
	// Key identifies a label or field and is set only on leaf nodes.
	Key string
	// Values contains only scalar operands required by a leaf operator. Supported
	// values are nil, strings, booleans, integers, finite floats, and times.
	Values []any
	// Requirements contains children only for And, Or, and Not. Not requires
	// exactly one child; And and Or may contain any number of children.
	Requirements Requirements
}

// Validate verifies the operator-specific field shape of requirement and all
// nested requirements.
func (r Requirement) Validate() error {
	switch r.Operator {
	case None, All:
		if r.Key != "" || len(r.Values) != 0 || len(r.Requirements) != 0 {
			return fmt.Errorf("requirement %q cannot carry a key, values, or children", r.Operator)
		}
	case And, Or:
		if r.Key != "" || len(r.Values) != 0 {
			return fmt.Errorf("requirement %q cannot carry a key or values", r.Operator)
		}
		if err := r.Requirements.Validate(); err != nil {
			return fmt.Errorf("requirement %q: %w", r.Operator, err)
		}
	case Not:
		if r.Key != "" || len(r.Values) != 0 || len(r.Requirements) != 1 {
			return fmt.Errorf("requirement %q requires exactly one child and no key or values", r.Operator)
		}
		if err := r.Requirements[0].Validate(); err != nil {
			return fmt.Errorf("requirement %q child: %w", r.Operator, err)
		}
	case Exists, DoesNotExist:
		if r.Key == "" || len(r.Values) != 0 || len(r.Requirements) != 0 {
			return fmt.Errorf("requirement %q requires one key and no values or children", r.Operator)
		}
	case Equals, DoubleEquals, NotEquals, GreaterThan, LessThan, GreaterThanOrEqual, LessThanOrEqual, Like:
		if r.Key == "" || len(r.Values) != 1 || len(r.Requirements) != 0 {
			return fmt.Errorf("requirement %q requires one key, one value, and no children", r.Operator)
		}
	case In, NotIn, Contains:
		if r.Key == "" || len(r.Requirements) != 0 {
			return fmt.Errorf("requirement %q requires one key and no children", r.Operator)
		}
	default:
		return fmt.Errorf("unsupported requirement operator %q", r.Operator)
	}
	for index, value := range r.Values {
		if err := validateRequirementValue(value); err != nil {
			return fmt.Errorf("requirement %q value %d: %w", r.Operator, index, err)
		}
	}
	return nil
}

func validateRequirementValue(value any) error {
	ref := indirectRequirementValue(value)
	if !ref.IsValid() {
		return nil
	}
	switch ref.Interface().(type) {
	case time.Time, meta.Time:
		return nil
	}
	switch ref.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return nil
	case reflect.Float32, reflect.Float64:
		value := ref.Float()
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("non-finite float %v", value)
		}
		return nil
	default:
		return fmt.Errorf("unsupported type %T", value)
	}
}

// Validate verifies every requirement in the implicit top-level conjunction.
func (r Requirements) Validate() error {
	for index, requirement := range r {
		if err := requirement.Validate(); err != nil {
			return fmt.Errorf("requirement %d: %w", index, err)
		}
	}
	return nil
}

// RequirementsFromMap constructs an equality requirement for every map entry.
func RequirementsFromMap(kvs map[string]string) Requirements {
	var reqs Requirements
	for k, v := range kvs {
		reqs = append(reqs, RequirementEqual(k, v))
	}
	return reqs
}
