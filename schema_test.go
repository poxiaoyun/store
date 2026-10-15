package store

import "testing"

type schemaTestObject struct {
	ObjectMeta `json:"metadata,omitempty"`
}

func (*schemaTestObject) ResourceName() string { return "users" }

type schemaSnapshotObject struct {
	ObjectMeta `json:"metadata,omitempty"`
}

func (*schemaSnapshotObject) ResourceName() string { return "groups" }

func TestSchemaRegister(t *testing.T) {
	object := &schemaTestObject{ObjectMeta: ObjectMeta{Resource: "users"}}
	schema := NewSchema()
	err := schema.Register(object, ResourceSchema{
		ScopeKeys: []string{"tenant"},
		Indexes: []Index{
			{Fields: []string{"email"}, Unique: true},
			{Name: "phone", Fields: []string{"phone"}, Unique: true, Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	definition, err := schema.Resource("users")
	if err != nil {
		t.Fatalf("Resource() error = %v", err)
	}
	if len(definition.Indexes) != 3 {
		t.Fatalf("len(Indexes) = %d, want 3", len(definition.Indexes))
	}
	if !definition.IsPrimaryIndex(definition.Indexes[0]) {
		t.Fatalf("first index = %#v, want primary index", definition.Indexes[0])
	}
	if got := definition.Indexes[1].Name; got != "email_tenant" {
		t.Fatalf("generated index name = %q, want email_tenant", got)
	}
	if got := definition.Indexes[1].Fields; len(got) != 2 || got[0] != "email" || got[1] != "tenant" {
		t.Fatalf("index fields = %v, want [email tenant]", got)
	}
	created, err := schema.NewObject("users")
	if err != nil {
		t.Fatalf("NewObject() error = %v", err)
	}
	if _, ok := created.(*schemaTestObject); !ok {
		t.Fatalf("NewObject() = %T, want *schemaTestObject", created)
	}
}

func TestSchemaValidation(t *testing.T) {
	tests := []struct {
		name       string
		definition ResourceSchema
	}{
		{name: "duplicate scope", definition: ResourceSchema{ScopeKeys: []string{"tenant", "tenant"}}},
		{name: "empty fields", definition: ResourceSchema{Indexes: []Index{{Name: "empty"}}}},
		{name: "duplicate field", definition: ResourceSchema{Indexes: []Index{{Fields: []string{"email", "email"}}}}},
		{name: "nullable non unique", definition: ResourceSchema{Indexes: []Index{{Fields: []string{"email"}, Nullable: true}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object := &schemaTestObject{ObjectMeta: ObjectMeta{Resource: "users"}}
			if err := NewSchema().Register(object, test.definition); err == nil {
				t.Fatal("Register() error = nil")
			}
		})
	}
}

func TestSchemaClone(t *testing.T) {
	schema := NewSchema()
	object := &schemaTestObject{ObjectMeta: ObjectMeta{Resource: "users"}}
	if err := schema.Register(object, ResourceSchema{Indexes: []Index{{Fields: []string{"email"}}}}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	clone, err := schema.Clone()
	if err != nil {
		t.Fatalf("Clone() error = %v", err)
	}
	definition, err := clone.Resource("users")
	if err != nil {
		t.Fatalf("Resource() error = %v", err)
	}
	definition.Indexes[1].Fields[0] = "changed"
	original, _ := schema.Resource("users")
	if original.Indexes[1].Fields[0] != "email" {
		t.Fatal("Clone() shares index fields with original schema")
	}
}

func TestSchemaSnapshot(t *testing.T) {
	schema := NewSchema()
	object := &schemaTestObject{ObjectMeta: ObjectMeta{Resource: "users"}}
	if err := schema.Register(object, ResourceSchema{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	snapshot := schema.Snapshot()
	if err := snapshot.Register(
		&schemaSnapshotObject{},
		ResourceSchema{},
	); err != nil {
		t.Fatalf("snapshot Register() error = %v", err)
	}
	if _, err := schema.Resource("groups"); err == nil {
		t.Fatal("Snapshot() shares registered resources with original schema")
	}
}

func TestSchemaExplicitPrimaryReplacesDefault(t *testing.T) {
	schema := NewSchema()
	object := &schemaTestObject{ObjectMeta: ObjectMeta{Resource: "users"}}
	if err := schema.Register(object, ResourceSchema{
		ScopeKeys: []string{"tenant"},
		Indexes: []Index{
			{Fields: []string{"email"}},
			{Name: "id_tenant", Fields: []string{"id"}, Unique: true},
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	definition, _ := schema.Resource("users")
	primaryCount := 0
	for _, index := range definition.Indexes {
		if definition.IsPrimaryIndex(index) {
			primaryCount++
		}
	}
	if primaryCount != 1 {
		t.Fatalf("primary index count = %d, want 1", primaryCount)
	}
}
