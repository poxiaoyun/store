package sql

import (
	"fmt"
	"strings"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
)

// QueryState accumulates the bind values of one statement and renders their
// dialect placeholders, so a statement can be built once for every backend.
type QueryState struct {
	dialect Dialect
	args    []any
}

func (q *QueryState) Bind(value any) string {
	q.args = append(q.args, value)
	return q.dialect.Placeholder(len(q.args))
}

// Args returns the bound values in the order their placeholders appear.
func (q *QueryState) Args() []any {
	return q.args
}

// BindAll binds every value and returns their placeholders in order.
func (q *QueryState) BindAll(values []any) []string {
	placeholders := make([]string, 0, len(values))
	for _, value := range values {
		placeholders = append(placeholders, q.Bind(value))
	}
	return placeholders
}

// ConditionBuilder joins non-empty conditions with AND.
type ConditionBuilder struct {
	conditions []string
}

func (b *ConditionBuilder) Add(condition string) {
	if condition == "" {
		return
	}
	b.conditions = append(b.conditions, condition)
}

func (b *ConditionBuilder) Where() string {
	if len(b.conditions) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(b.conditions, " AND ")
}

// fieldExpression renders one field as a SQL expression. A nested path compares
// through its JSON text, because a row stores the containing document as a
// single column.
func (s *Storage) fieldExpression(field string) (string, error) {
	if !IsNestedField(field) {
		return s.core.dialect.Quote(field), nil
	}
	path := JSONPath(field)
	return s.core.dialect.JSONText(s.core.dialect.Quote(path[0]), path[1:])
}

// keyExpression renders one requirement key as a SQL expression: a field key
// addresses a column or a nested JSON path, while a label key addresses an entry
// inside the labels document.
type keyExpression func(key string) (string, error)

// labelExpression renders one label key as an entry of the labels document.
func (s *Storage) labelExpression(key string) (string, error) {
	return s.core.dialect.JSONText(s.core.dialect.Quote("labels"), []string{key})
}

// requirementExpression renders one requirement tree and binds its values.
func (s *Storage) requirementExpression(q *QueryState, requirement selector.Requirement, resolve keyExpression) (string, error) {
	switch requirement.Operator {
	case selector.None:
		return "1 = 0", nil
	case selector.All:
		return "1 = 1", nil
	case selector.And, selector.Or:
		separator := " AND "
		if requirement.Operator == selector.Or {
			separator = " OR "
		}
		rendered := make([]string, 0, len(requirement.Requirements))
		for _, nested := range requirement.Requirements {
			expression, err := s.requirementExpression(q, nested, resolve)
			if err != nil {
				return "", err
			}
			rendered = append(rendered, expression)
		}
		if len(rendered) == 0 {
			if requirement.Operator == selector.And {
				return "1 = 1", nil
			}
			return "1 = 0", nil
		}
		return "(" + strings.Join(rendered, separator) + ")", nil
	case selector.Not:
		if len(requirement.Requirements) == 0 {
			return "1 = 1", nil
		}
		expression, err := s.requirementExpression(q, requirement.Requirements[0], resolve)
		if err != nil {
			return "", err
		}
		return "NOT (" + expression + ")", nil
	case selector.In, selector.NotIn:
		// An empty value list restricts the key to no value at all, so the
		// requirement matches nothing whichever operator forms it.
		if len(requirement.Values) == 0 {
			return "1 = 0", nil
		}
	}

	field, err := resolve(requirement.Key)
	if err != nil {
		return "", err
	}
	// A nested path compares as its JSON text, so bind values in that form.
	values := requirement.Values
	if IsNestedField(requirement.Key) {
		values = TextArgs(values)
	}
	switch requirement.Operator {
	case selector.Equals, selector.DoubleEquals:
		return "(" + field + " IS NOT NULL AND " + field + " = " + q.Bind(first(values)) + ")", nil
	case selector.NotEquals:
		return "(" + field + " IS NULL OR " + field + " != " + q.Bind(first(values)) + ")", nil
	case selector.In:
		return "(" + field + " IS NOT NULL AND " + field + " IN (" + strings.Join(q.BindAll(values), ", ") + "))", nil
	case selector.NotIn:
		return "(" + field + " IS NULL OR " + field + " NOT IN (" + strings.Join(q.BindAll(values), ", ") + "))", nil
	case selector.DoesNotExist:
		return field + " IS NULL", nil
	case selector.Exists:
		return field + " IS NOT NULL", nil
	case selector.GreaterThan:
		return "(" + field + " IS NOT NULL AND " + field + " > " + q.Bind(first(values)) + ")", nil
	case selector.LessThan:
		return "(" + field + " IS NOT NULL AND " + field + " < " + q.Bind(first(values)) + ")", nil
	case selector.GreaterThanOrEqual:
		return "(" + field + " IS NOT NULL AND " + field + " >= " + q.Bind(first(values)) + ")", nil
	case selector.LessThanOrEqual:
		return "(" + field + " IS NOT NULL AND " + field + " <= " + q.Bind(first(values)) + ")", nil
	case selector.Like:
		return "(" + field + " IS NOT NULL AND " + field + " LIKE " + q.Bind("%"+fmt.Sprintf("%v", first(values))+"%") + ")", nil
	case selector.Contains:
		return "", errors.NewUnsupported("SQL store does not support Contains requirements")
	default:
		return "", errors.NewUnsupported(fmt.Sprintf("SQL store does not support requirement operator %q", requirement.Operator))
	}
}

// first returns the first value of a requirement, which the compared operators
// always carry. A malformed requirement is a programmer error.
func first(values []any) any {
	return values[0]
}

// whereClause renders the scope, object identity and requirements of one read
// or write.
func (s *Storage) whereClause(q *QueryState, id string, labels, fields store.Requirements) (string, error) {
	builder := &ConditionBuilder{}
	for _, scope := range s.scopes {
		field := store.ScopeResourceToFieldName(scope.Resource)
		builder.Add(s.core.dialect.Quote(field) + " = " + q.Bind(scope.Name))
	}
	if id != "" {
		builder.Add(s.core.dialect.Quote("id") + " = " + q.Bind(id))
	}
	if err := s.addRequirementConditions(builder, q, labels, fields); err != nil {
		return "", err
	}
	return builder.Where(), nil
}

// addRequirementConditions appends validated label and field requirements.
func (s *Storage) addRequirementConditions(builder *ConditionBuilder, q *QueryState, labels, fields store.Requirements) error {
	if err := labels.Validate(); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	if err := fields.Validate(); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	for _, requirement := range labels {
		expression, err := s.requirementExpression(q, requirement, s.labelExpression)
		if err != nil {
			return err
		}
		builder.Add(expression)
	}
	for _, requirement := range fields {
		expression, err := s.requirementExpression(q, requirement, s.fieldExpression)
		if err != nil {
			return err
		}
		builder.Add(expression)
	}
	return nil
}

// searchConditions renders the case-insensitive substring search over the
// requested fields.
func (s *Storage) searchConditions(q *QueryState, search string, searchFields []string) (string, error) {
	if search == "" {
		return "", nil
	}
	if len(searchFields) == 0 {
		searchFields = []string{"id", "name"}
	}
	matches := make([]string, 0, len(searchFields))
	pattern := "%" + strings.ToLower(search) + "%"
	for _, field := range searchFields {
		expression, err := s.fieldExpression(field)
		if err != nil {
			return "", err
		}
		matches = append(matches, "LOWER("+expression+") LIKE "+q.Bind(pattern))
	}
	return "(" + strings.Join(matches, " OR ") + ")", nil
}

// selectColumns renders the projection of one read. Requested fields are
// projected literally; a read that must validate preconditions always adds the
// identity columns those checks need.
func (s *Storage) selectColumns(model any, requested []string, withPreconditions bool) (string, error) {
	names := requested
	if len(names) == 0 {
		names = s.core.codec.Fields(model)
	}
	columns := make([]string, 0, len(names)+3)
	for _, name := range names {
		expression, err := s.fieldExpression(name)
		if err != nil {
			return "", err
		}
		columns = append(columns, expression)
	}
	if len(requested) > 0 && withPreconditions {
		for _, name := range []string{"id", "uid", "resourceVersion"} {
			columns = append(columns, s.core.dialect.Quote(name))
		}
	}
	return strings.Join(columns, ", "), nil
}

// orderBy renders the requested ordering. A nested field orders by its JSON
// text, matching its comparison semantics.
func (s *Storage) orderBy(sort string) (string, error) {
	sorts := store.ParseSorts(sort)
	if len(sorts) == 0 {
		return "", nil
	}
	terms := make([]string, 0, len(sorts))
	for _, sort := range sorts {
		expression, err := s.fieldExpression(sort.Field)
		if err != nil {
			return "", err
		}
		direction := "ASC"
		if sort.Direction != meta.SortDirectionAsc {
			direction = "DESC"
		}
		terms = append(terms, expression+" "+direction)
	}
	return "ORDER BY " + strings.Join(terms, ", "), nil
}

// window renders the row window of a read. The limit binds before the offset, so
// the placeholders stay in the dialect's expected order.
func (s *Storage) window(q *QueryState, offset, limit int) string {
	limitPlaceholder := ""
	if limit > 0 {
		limitPlaceholder = q.Bind(limit)
	}
	offsetPlaceholder := ""
	if offset > 0 {
		offsetPlaceholder = q.Bind(offset)
	}
	return s.core.dialect.Window(limitPlaceholder, offsetPlaceholder)
}
