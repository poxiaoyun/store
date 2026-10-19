package selector

import (
	"math"
	"math/big"
	"reflect"
	"strings"
	"time"

	"xiaoshiai.cn/common/meta"
)

// RequirementsMatchLabels reports whether labels satisfy every requirement.
// The requirements must have passed Requirements.Validate before repeated use.
func RequirementsMatchLabels(r Requirements, labels map[string]string) bool {
	return requirementsMatchLookup(r, func(key string) (any, bool) {
		value, exists := labels[key]
		return value, exists
	})
}

// RequirementMatchLabels reports whether labels satisfy requirement. The
// requirement must have passed Requirement.Validate before repeated use.
func RequirementMatchLabels(r Requirement, obj map[string]string) bool {
	return requirementMatchesLookup(r, func(key string) (any, bool) {
		value, exists := obj[key]
		return value, exists
	})
}

// Match reports whether every top-level requirement matches values returned by
// lookup. Requirements must have passed Validate before repeated evaluation.
func (r Requirements) Match(lookup func(key string) (value any, exists bool)) bool {
	return requirementsMatchLookup(r, lookup)
}

// Match reports whether requirement matches values returned by lookup. The
// requirement must have passed Validate before repeated evaluation.
func (r Requirement) Match(lookup func(key string) (value any, exists bool)) bool {
	return requirementMatchesLookup(r, lookup)
}

func requirementsMatchLookup(requirements Requirements, lookup func(string) (any, bool)) bool {
	for _, requirement := range requirements {
		if !requirementMatchesLookup(requirement, lookup) {
			return false
		}
	}
	return true
}

func requirementMatchesLookup(requirement Requirement, lookup func(string) (any, bool)) bool {
	switch requirement.Operator {
	case None:
		return false
	case All:
		return true
	case And:
		return requirementsMatchLookup(requirement.Requirements, lookup)
	case Or:
		for _, child := range requirement.Requirements {
			if requirementMatchesLookup(child, lookup) {
				return true
			}
		}
		return false
	case Not:
		return !requirementMatchesLookup(requirement.Requirements[0], lookup)
	default:
		value, exists := lookup(requirement.Key)
		return requirementMatches(value, exists, requirement)
	}
}

func requirementMatches(value any, exists bool, requirement Requirement) bool {
	values := requirement.Values
	switch requirement.Operator {
	case DoesNotExist:
		return !exists
	case Exists:
		return exists
	case Equals, DoubleEquals:
		return exists && len(values) == 1 && requirementValueEqual(value, values[0])
	case NotEquals:
		return len(values) == 1 && (!exists || !requirementValueEqual(value, values[0]))
	case In:
		return exists && len(values) > 0 && requirementMatchIn(value, values...)
	case NotIn:
		return len(values) > 0 && (!exists || !requirementMatchIn(value, values...))
	case GreaterThan, LessThan, GreaterThanOrEqual, LessThanOrEqual:
		if !exists || len(values) != 1 {
			return false
		}
		comparison, comparable := compareRequirementValues(value, values[0])
		if !comparable {
			return false
		}
		switch requirement.Operator {
		case GreaterThan:
			return comparison > 0
		case LessThan:
			return comparison < 0
		case GreaterThanOrEqual:
			return comparison >= 0
		case LessThanOrEqual:
			return comparison <= 0
		}
	case Contains:
		return exists && requirementContains(value, values)
	case Like:
		return exists && len(values) == 1 && requirementStringContains(value, values[0])
	}
	return false
}

func requirementValueEqual(a, b any) bool {
	if reflect.DeepEqual(a, b) {
		return true
	}
	comparison, comparable := compareRequirementValues(a, b)
	return comparable && comparison == 0
}

func compareRequirementValues(a, b any) (int, bool) {
	if timeA, okA := requirementTime(a); okA {
		if timeB, okB := requirementTime(b); okB {
			return timeA.Compare(timeB), true
		}
	}

	if numberA, okA := requirementNumber(a); okA {
		if numberB, okB := requirementNumber(b); okB {
			return numberA.Cmp(numberB), true
		}
	}

	if boolA, okA := requirementBool(a); okA {
		if boolB, okB := requirementBool(b); okB {
			switch {
			case boolA == boolB:
				return 0, true
			case !boolA:
				return -1, true
			default:
				return 1, true
			}
		}
	}

	valueA := indirectRequirementValue(a)
	valueB := indirectRequirementValue(b)
	if !valueA.IsValid() || !valueB.IsValid() || valueA.Kind() != valueB.Kind() {
		return 0, false
	}
	switch valueA.Kind() {
	case reflect.String:
		return strings.Compare(valueA.String(), valueB.String()), true
	case reflect.Bool:
		switch {
		case valueA.Bool() == valueB.Bool():
			return 0, true
		case !valueA.Bool():
			return -1, true
		default:
			return 1, true
		}
	}
	return 0, false
}

func requirementBool(value any) (bool, bool) {
	ref := indirectRequirementValue(value)
	if !ref.IsValid() {
		return false, false
	}
	switch value := ref.Interface().(type) {
	case bool:
		return value, true
	case string:
		switch value {
		case "true":
			return true, true
		case "false":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

func indirectRequirementValue(value any) reflect.Value {
	ref := reflect.ValueOf(value)
	for ref.IsValid() && (ref.Kind() == reflect.Interface || ref.Kind() == reflect.Ptr) {
		if ref.IsNil() {
			return reflect.Value{}
		}
		ref = ref.Elem()
	}
	return ref
}

func requirementTime(value any) (time.Time, bool) {
	ref := indirectRequirementValue(value)
	if !ref.IsValid() {
		return time.Time{}, false
	}
	switch value := ref.Interface().(type) {
	case time.Time:
		return value, true
	case meta.Time:
		return value.Time, true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, false
		}
		return parsed, true
	default:
		return time.Time{}, false
	}
}

func requirementNumber(value any) (*big.Rat, bool) {
	ref := indirectRequirementValue(value)
	if !ref.IsValid() {
		return nil, false
	}
	number := new(big.Rat)
	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		number.SetInt64(ref.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		number.SetInt(new(big.Int).
			SetUint64(ref.Uint()))
	case reflect.Float32, reflect.Float64:
		value := ref.Float()
		if math.IsNaN(value) || math.IsInf(value, 0) || number.SetFloat64(value) == nil {
			return nil, false
		}
	case reflect.String:
		if _, ok := number.SetString(ref.String()); !ok {
			return nil, false
		}
	default:
		return nil, false
	}
	return number, true
}

func requirementContains(value any, expected []any) bool {
	if len(expected) == 0 {
		return false
	}
	ref := indirectRequirementValue(value)
	if !ref.IsValid() {
		return false
	}
	switch ref.Kind() {
	case reflect.String:
		for _, item := range expected {
			if !requirementStringContains(ref.String(), item) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		for _, item := range expected {
			found := false
			for i := 0; i < ref.Len(); i++ {
				if requirementValueEqual(ref.Index(i).
					Interface(), item) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func requirementStringContains(value, expected any) bool {
	ref := indirectRequirementValue(value)
	if !ref.IsValid() || ref.Kind() != reflect.String {
		return false
	}
	expectedRef := indirectRequirementValue(expected)
	if !expectedRef.IsValid() || expectedRef.Kind() != reflect.String {
		return false
	}
	return strings.Contains(ref.String(), expectedRef.String())
}

func requirementMatchIn(val any, in ...any) bool {
	for _, candidate := range in {
		if requirementValueEqual(val, candidate) {
			return true
		}
	}
	return false
}
