package selector

import (
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"xiaoshiai.cn/common/meta"
)

// String returns the canonical selector expression parsed by ParseRequirements.
// Callers constructing requirements directly must validate them before serialization.
func (r Requirements) String() string {
	var sb strings.Builder
	for i, requirement := range r {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(requirement.String())
	}
	return sb.String()
}

// String returns the canonical selector expression for one valid requirement node.
func (r Requirement) String() string {
	switch r.Operator {
	case None:
		return "none()"
	case All:
		return "all()"
	case And:
		if len(r.Requirements) == 0 {
			return "and()"
		}
		return "(" + r.Requirements.join(" && ") + ")"
	case Or:
		if len(r.Requirements) == 0 {
			return "or()"
		}
		return "(" + r.Requirements.join(" || ") + ")"
	case Not:
		if len(r.Requirements) != 1 {
			return "!(invalid)"
		}
		return "!(" + r.Requirements[0].String() + ")"
	}
	var sb strings.Builder
	sb.Grow(
		// length of r.key
		len(r.Key) +
			// length of 'r.operator' + 2 spaces for the worst case ('in' and 'notin')
			len(r.Operator) + 2 +
			// length of 'r.strValues' slice times. Heuristically 5 chars per word
			+5*len(r.Values))
	if r.Operator == DoesNotExist {
		sb.WriteString("!")
	}
	sb.WriteString(formatRequirementToken(r.Key, false))

	switch r.Operator {
	case Equals:
		sb.WriteString("=")
	case DoubleEquals:
		sb.WriteString("==")
	case NotEquals:
		sb.WriteString("!=")
	case In:
		sb.WriteString(" in ")
	case NotIn:
		sb.WriteString(" notin ")
	case GreaterThan:
		sb.WriteString(">")
	case LessThan:
		sb.WriteString("<")
	case GreaterThanOrEqual:
		sb.WriteString(">=")
	case LessThanOrEqual:
		sb.WriteString("<=")
	case Contains:
		sb.WriteString(" contains ")
	case Like:
		sb.WriteString(" like ")
	case Exists, DoesNotExist:
		return sb.String()
	}

	switch r.Operator {
	case In, NotIn, Contains:
		sb.WriteString("(")
	}
	if len(r.Values) == 1 {
		sb.WriteString(formatRequirementValue(r.Values[0]))
	} else {
		strValues := make([]string, 0, len(r.Values))
		for _, val := range r.Values {
			strValues = append(strValues, formatRequirementValue(val))
		}
		sort.Strings(strValues)
		sb.WriteString(strings.Join(strValues, ","))
	}
	switch r.Operator {
	case In, NotIn, Contains:
		sb.WriteString(")")
	}
	return sb.String()
}

func formatRequirementValue(value any) string {
	ref := indirectRequirementValue(value)
	if !ref.IsValid() {
		return "null"
	}
	switch value := ref.Interface().(type) {
	case time.Time:
		return value.Format(time.RFC3339Nano)
	case meta.Time:
		return value.Time.Format(time.RFC3339Nano)
	}
	text := requirementValueString(ref)
	if ref.Kind() == reflect.String {
		return formatRequirementToken(text, text == "null")
	}
	return formatRequirementToken(text, false)
}

func requirementValueString(value reflect.Value) string {
	switch value.Kind() {
	case reflect.String:
		return value.String()
	case reflect.Bool:
		return strconv.FormatBool(value.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(value.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(value.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(value.Float(), 'f', -1, value.Type().
			Bits())
	default:
		panic("unsupported validated requirement value")
	}
}

func formatRequirementToken(value string, forceQuote bool) string {
	if forceQuote || value == "" || !utf8.ValidString(value) {
		return strconv.Quote(value)
	}
	for _, character := range value {
		if unicode.IsSpace(character) || unicode.IsControl(character) || strings.ContainsRune(",()!<>=&|\"\\", character) {
			return strconv.Quote(value)
		}
	}
	return value
}

func (r Requirements) join(separator string) string {
	var sb strings.Builder
	for index, requirement := range r {
		if index > 0 {
			sb.WriteString(separator)
		}
		sb.WriteString(requirement.String())
	}
	return sb.String()
}
