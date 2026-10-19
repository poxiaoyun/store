package sql

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// ColumnSpec is one planned table column.
type ColumnSpec struct {
	Name string
	Type reflect.Type
	// Size is the declared bounded length of a string column. Only an indexed
	// string needs one.
	Size int
	// Indexed reports that a unique or secondary index names this column
	// directly, which makes the backend bound its type.
	Indexed bool
}

// tablePlan is the planned layout of one resource table.
type tablePlan struct {
	// Table is the resource table name.
	Table string
	// Columns are the planned columns in model declaration order.
	Columns []ColumnSpec
	// Indexes are the declared secondary indexes; the primary key owns ID uniqueness.
	Indexes []store.Index
}

// planTable derives the table layout of one registered resource from its model
// and schema declaration. Column names are the model's representation names,
// so DDL and statement building always agree on one name.
func (s *Storage) planTable(resource string, definition store.ResourceSchema) (tablePlan, error) {
	fields, err := s.core.codec.ModelFields(definition.Object)
	if err != nil {
		return tablePlan{}, err
	}
	for _, name := range definition.GenerationFields {
		supported := false
		for _, field := range fields {
			if field.Name == name && !documentType(field.Type) {
				supported = true
			}
		}
		if !supported {
			return tablePlan{}, errors.NewUnsupported("SQL Generation tracking requires top-level scalar fields")
		}
	}
	columns := make([]ColumnSpec, 0, len(fields))
	known := make(map[string]struct{}, len(fields))
	specs := make(map[string]store.FieldSpec, len(definition.FieldSpecs))
	for _, spec := range definition.FieldSpecs {
		specs[spec.Name] = spec
	}
	for _, field := range fields {
		columns = append(columns, ColumnSpec{
			Name:    field.Name,
			Type:    field.Type,
			Size:    specs[field.Name].Size,
			Indexed: field.Name == "id",
		})
		known[field.Name] = struct{}{}
	}
	for _, spec := range definition.FieldSpecs {
		if _, ok := known[spec.Name]; !ok {
			return tablePlan{}, fmt.Errorf("resource %q declares a field spec for unknown field %q", resource, spec.Name)
		}
	}
	for _, scopeKey := range definition.ScopeKeys {
		if _, ok := known[scopeKey]; !ok {
			return tablePlan{}, fmt.Errorf("resource %q declares scope key %q without a matching field", resource, scopeKey)
		}
	}
	indexes := make([]store.Index, 0, len(definition.Indexes))
	for _, index := range definition.Indexes {
		if definition.IsPrimaryIndex(index) {
			continue
		}
		indexes = append(indexes, index)
		for _, indexed := range index.Fields {
			name := indexed
			if IsNestedField(indexed) {
				// A nested path is indexed through a JSON expression or a
				// generated column, so the JSON column itself stays unbounded.
				name = JSONPath(indexed)[0]
			}
			if _, ok := known[name]; !ok {
				return tablePlan{}, fmt.Errorf("resource %q index %q references unknown field %q", resource, index.Name, indexed)
			}
			if IsNestedField(indexed) {
				continue
			}
			for i := range columns {
				if columns[i].Name == name {
					columns[i].Indexed = true
				}
			}
		}
	}
	plan := tablePlan{
		Table:   resource,
		Columns: columns,
		Indexes: indexes,
	}
	return plan, nil
}

// ensureSchema creates and reconciles every registered resource table.
func (s *Storage) ensureSchema(ctx context.Context) error {
	for _, resource := range s.core.schema.Resources() {
		definition, err := s.core.schema.Resource(resource)
		if err != nil {
			return err
		}
		if err := s.ensureResource(ctx, resource, definition); err != nil {
			return err
		}
	}
	return nil
}

// ensureResource creates the table when it is missing, adds columns that a
// newer model introduced, and creates planned indexes that do not exist yet.
// It never drops or retypes a column: removing a field requires an explicit
// migration outside the Store.
func (s *Storage) ensureResource(ctx context.Context, resource string, definition store.ResourceSchema) error {
	plan, err := s.planTable(resource, definition)
	if err != nil {
		return err
	}
	createTable, err := s.core.dialect.CreateTable(plan.Table, plan.Columns)
	if err != nil {
		return err
	}
	if _, err := s.core.exec.ExecContext(ctx, createTable); err != nil {
		return s.core.dialect.MapError(err, resource, "")
	}
	existingColumns, err := s.core.dialect.Columns(ctx, s.core.exec, plan.Table)
	if err != nil {
		return err
	}
	for _, column := range plan.Columns {
		if _, ok := existingColumns[column.Name]; ok {
			continue
		}
		addColumn, err := s.core.dialect.AddColumn(plan.Table, column)
		if err != nil {
			return err
		}
		if _, err := s.core.exec.ExecContext(ctx, addColumn); err != nil {
			return s.core.dialect.MapError(err, resource, "")
		}
	}
	return s.ensureIndexes(ctx, plan)
}

func (s *Storage) ensureIndexes(ctx context.Context, plan tablePlan) error {
	existing, err := s.core.dialect.Indexes(ctx, s.core.exec, plan.Table)
	if err != nil {
		return err
	}
	var primary []string
	for _, index := range existing {
		if index.Primary {
			primary = index.Columns
			break
		}
	}
	if !slices.Equal(primary, []string{"id"}) {
		return fmt.Errorf("table %q requires primary key (id); migrate the existing table before constructing the Store", plan.Table)
	}
	for _, index := range plan.Indexes {
		_, ok := existing[index.Name]
		if ok {
			if err := s.core.dialect.ValidateIndex(ctx, s.core.exec, plan.Table, index); err != nil {
				return err
			}
			continue
		}
		statements, err := s.core.dialect.IndexDDL(plan.Table, index)
		if err != nil {
			return err
		}
		for _, statement := range statements {
			if _, err := s.core.exec.ExecContext(ctx, statement); err != nil {
				return s.core.dialect.MapError(err, plan.Table, index.Name)
			}
		}
	}
	return nil
}
