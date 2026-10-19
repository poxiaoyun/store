package sql

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

func TestPlanTableRejectsComplexGenerationTracking(t *testing.T) {
	storage := &Storage{core: &core{codec: NewModelCodec()}}
	for _, field := range []string{"extra", "link.href"} {
		_, err := storage.planTable("planobjects", store.ResourceSchema{Object: &planObject{}, GenerationFields: []string{field}})
		if !errors.IsUnsupported(err) {
			t.Fatalf("tracking %s = %v, want Unsupported", field, err)
		}
	}
}

type planLink struct {
	Href string `json:"href"`
}

type planObject struct {
	store.ObjectMeta `json:",inline"`
	Value            string            `json:"value"`
	Extra            map[string]string `json:"extra,omitempty"`
	Moment           meta.Time         `json:"moment"`
	Link             *planLink         `json:"link,omitempty"`
}

func (*planObject) ResourceName() string { return "planobjects" }

func TestPlanTableDerivesColumns(t *testing.T) {
	definition := store.ResourceSchema{
		Object:     &planObject{},
		ScopeKeys:  []string{"value"},
		Indexes:    []store.Index{{Name: "planobjects_value", Fields: []string{"value"}, Unique: true}},
		FieldSpecs: []store.FieldSpec{{Name: "value", Size: 64}},
	}
	storage := &Storage{core: &core{codec: NewModelCodec()}}
	plan, err := storage.planTable("planobjects", definition)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Table != "planobjects" {
		t.Fatalf("table = %s, want planobjects", plan.Table)
	}
	columns := map[string]ColumnSpec{}
	for _, column := range plan.Columns {
		columns[column.Name] = column
	}
	for _, name := range []string{"id", "resourceVersion", "creationTimestamp", "value", "extra", "moment", "link"} {
		if _, ok := columns[name]; !ok {
			t.Fatalf("plan misses column %q: %#v", name, plan.Columns)
		}
	}
	if !columns["value"].Indexed {
		t.Fatal("indexed field was not marked indexed")
	}
	if got := columns["value"].Size; got != 64 {
		t.Fatalf("declared size = %d, want 64", got)
	}
	if columns["extra"].Indexed || columns["extra"].Size != 0 {
		t.Fatalf("unindexed field kept index attributes: %#v", columns["extra"])
	}
	if got := columns["moment"].Type; got != reflect.TypeFor[meta.Time]() {
		t.Fatalf("moment column type = %v", got)
	}
}

func TestPlanTableLeavesJSONColumnUnboundedForNestedIndex(t *testing.T) {
	definition := store.ResourceSchema{
		Object:  &planObject{},
		Indexes: []store.Index{{Name: "planobjects_link_href", Fields: []string{"link.href"}}},
	}
	storage := &Storage{core: &core{codec: NewModelCodec()}}
	plan, err := storage.planTable("planobjects", definition)
	if err != nil {
		t.Fatal(err)
	}
	for _, column := range plan.Columns {
		if column.Name != "link" {
			continue
		}
		if column.Indexed || column.Size != 0 {
			t.Fatalf("nested index changed its parent column: %#v", column)
		}
		return
	}
	t.Fatal("plan misses the link column")
}

func TestPlanTableRejectsUnknownDeclarations(t *testing.T) {
	storage := &Storage{core: &core{codec: NewModelCodec()}}
	tests := []struct {
		name       string
		definition store.ResourceSchema
	}{
		{
			name:       "unknown scope key",
			definition: store.ResourceSchema{Object: &planObject{}, ScopeKeys: []string{"missing"}},
		},
		{
			name:       "unknown index field",
			definition: store.ResourceSchema{Object: &planObject{}, Indexes: []store.Index{{Name: "missing", Fields: []string{"missing"}}}},
		},
		{
			name:       "unknown field spec",
			definition: store.ResourceSchema{Object: &planObject{}, FieldSpecs: []store.FieldSpec{{Name: "missing", Size: 8}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := storage.planTable("planobjects", test.definition); err == nil {
				t.Fatal("PlanTable() returned no error")
			}
		})
	}
}
