package rest

import (
	"context"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/rest/api"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/storetest"
)

type optionCaptureStore struct {
	store.Store
	patchOptions      store.PatchOptions
	getOptions        store.GetOptions
	listOptions       store.ListOptions
	patchBatchOptions store.PatchBatchOptions
	deleteOptions     store.DeleteOptions
}

func (s *optionCaptureStore) Get(_ context.Context, id string, object store.Object, opts ...store.GetOption) error {
	s.getOptions = store.ApplyGetOptions(opts)
	object.SetID(id)
	return nil
}

type remoteSchemaObject struct {
	store.ObjectMeta `json:",inline"`
}

func (*remoteSchemaObject) ResourceName() string { return "remote-schema-tests" }

type remoteSchemaMutationObject struct {
	store.ObjectMeta `json:",inline"`
}

func (*remoteSchemaMutationObject) ResourceName() string { return "remote-schema-mutations" }

func (s *optionCaptureStore) Scope(...store.Scope) store.Store {
	return s
}

func (s *optionCaptureStore) List(_ context.Context, list store.ObjectList, opts ...store.ListOption) error {
	options := store.ApplyListOptions(opts)
	s.listOptions = options
	list.SetContinue("next-token")
	return nil
}

func (s *optionCaptureStore) PatchBatch(_ context.Context, _ store.ObjectList, _ store.PatchBatch, opts ...store.PatchBatchOption) error {
	s.patchBatchOptions = store.ApplyPatchBatchOptions(opts)
	return nil
}

func (s *optionCaptureStore) Delete(_ context.Context, _ store.Object, opts ...store.DeleteOption) error {
	s.deleteOptions = store.ApplyDeleteOptions(opts)
	return nil
}

func newCaptureRemoteStore(t *testing.T, underlying store.Store) *Client {
	t.Helper()
	serverAPI := NewServer(underlying)
	handler := api.New().
		Group(serverAPI.Group()).
		Build()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	return NewRemoteStore(serverURL, store.NewSchema(), store.Capabilities{})
}

type pingStore struct {
	store.Store
	called bool
}

func (s *pingStore) Ping(context.Context) error {
	s.called = true
	return nil
}

func TestRemoteStorePing(t *testing.T) {
	underlying := &pingStore{}
	remote := newCaptureRemoteStore(t, underlying)
	if err := remote.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if !underlying.called {
		t.Fatal("Ping() was not delegated to the server store")
	}
}

func TestRemoteStoreSchemaSnapshot(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&remoteSchemaObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	serverURL, err := url.Parse("https://store.example")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	remote := NewRemoteStore(serverURL, schema, store.Capabilities{})
	if err := schema.Register(&remoteSchemaMutationObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("source schema Register() error = %v", err)
	}

	scoped := remote.Scope(store.Scope{Resource: "tenants", Name: "tenant-a"})
	snapshot := scoped.Schema()
	if err := snapshot.Register(&remoteSchemaMutationObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("Schema().Register() error = %v", err)
	}
	remoteSchema := remote.Schema()
	if got := remoteSchema.Resources(); !reflect.DeepEqual(got, []string{"remote-schema-tests"}) {
		t.Fatalf("Schema().Resources() = %v, want [remote-schema-tests]", got)
	}
}

func TestRemoteStoreListPassesCursor(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	list := &store.List[store.Unstructured]{Resource: "widgets"}
	labelRequirement := selector.RequirementEqual("environment", "production")
	fieldRequirement := selector.RequirementEqual("enabled", "true")

	if err := remote.List(context.Background(), list,
		store.WithContinue("current-token", 2),
		store.WithLabelRequirements(labelRequirement),
		store.WithFieldRequirements(fieldRequirement),
		store.WithFields("id", "name"),
		store.WithResourceVersion(7),
		store.WithSubScopes(),
	); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if got := underlying.listOptions.Continue; got != "current-token" {
		t.Fatalf("ListOptions.Continue = %v, want %q", got, "current-token")
	}
	if got := underlying.listOptions.Limit; got != 2 {
		t.Fatalf("ListOptions.Limit = %d, want 2", got)
	}
	if !reflect.DeepEqual(underlying.listOptions.Fields, []string{"id", "name"}) {
		t.Fatalf("ListOptions.Fields = %#v", underlying.listOptions.Fields)
	}
	if underlying.listOptions.ResourceVersion == nil || *underlying.listOptions.ResourceVersion != 7 {
		t.Fatalf("ListOptions.ResourceVersion = %#v", underlying.listOptions.ResourceVersion)
	}
	if !underlying.listOptions.IncludeSubScopes {
		t.Fatal("ListOptions.IncludeSubScopes = false, want true")
	}
	if !reflect.DeepEqual(underlying.listOptions.LabelRequirements, store.Requirements{labelRequirement}) {
		t.Fatalf("ListOptions.LabelRequirements = %#v", underlying.listOptions.LabelRequirements)
	}
	if !reflect.DeepEqual(underlying.listOptions.FieldRequirements, store.Requirements{fieldRequirement}) {
		t.Fatalf("ListOptions.FieldRequirements = %#v", underlying.listOptions.FieldRequirements)
	}
	if got := list.Continue; got != "next-token" {
		t.Fatalf("List.Continue = %q, want %q", got, "next-token")
	}

}

func TestRemoteStoreListPreservesWindow(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	list := &store.List[store.Unstructured]{Resource: "widgets"}

	if err := remote.List(context.Background(), list, store.WithOffset(4), store.WithLimit(2)); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if underlying.listOptions.Offset != 4 || underlying.listOptions.Limit != 2 {
		t.Fatalf("ListOptions window = %#v", underlying.listOptions)
	}
}

func TestRemoteStoreListPassesRecursiveRequirements(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	list := &store.List[store.Unstructured]{Resource: "widgets"}
	labelRequirement := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("visibility", "public"),
			selector.RequirementEqual("owner", "alice"),
		},
	}
	fieldRequirement := selector.Requirement{
		Operator: selector.And,
		Requirements: store.Requirements{
			selector.NewRequirement("rank", selector.GreaterThan, int64(1)),
			{
				Operator: selector.Not,
				Requirements: store.Requirements{
					selector.RequirementEqual("state", "blocked"),
				},
			},
		},
	}
	if err := remote.List(t.Context(), list,
		store.WithLabelRequirements(labelRequirement),
		store.WithFieldRequirements(fieldRequirement),
	); err != nil {
		t.Fatal(err)
	}
	if got, want := underlying.listOptions.LabelRequirements.String(), (store.Requirements{labelRequirement}).String(); got != want {
		t.Fatalf("LabelRequirements = %q, want %q", got, want)
	}
	if got, want := underlying.listOptions.FieldRequirements.String(), (store.Requirements{fieldRequirement}).String(); got != want {
		t.Fatalf("FieldRequirements = %q, want %q", got, want)
	}
}

func TestRemoteStoreListEscapesSelectorValue(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "selector delimiters", key: "description", value: "public, but (restricted) && private"},
		{name: "quotes and backslashes", key: "message", value: `a "quoted" \ value`},
		{name: "URL reserved characters", key: "location", value: "a+b%#?/&"},
		{name: "Unicode whitespace", key: "note", value: "alpha\u3000beta"},
		{name: "control characters", key: "message", value: "line1\nline2\t\x00"},
		{name: "special key", key: "owner,name", value: "alice"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			underlying := &optionCaptureStore{}
			remote := newCaptureRemoteStore(t, underlying)
			list := &store.List[store.Unstructured]{Resource: "widgets"}
			requirement := selector.RequirementEqual(test.key, test.value)
			if err := remote.List(t.Context(), list, store.WithFieldRequirements(requirement)); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(underlying.listOptions.FieldRequirements, store.Requirements{requirement}) {
				t.Fatalf("FieldRequirements = %#v, want %#v", underlying.listOptions.FieldRequirements, store.Requirements{requirement})
			}
		})
	}
}

func TestRemoteStoreListRejectsInvalidRequirements(t *testing.T) {
	remote := newCaptureRemoteStore(t, &optionCaptureStore{})
	list := &store.List[store.Unstructured]{Resource: "widgets"}
	err := remote.List(t.Context(), list,
		store.WithFieldRequirements(selector.Requirement{Operator: selector.Not}),
	)
	if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
		t.Fatalf("List() error = %v, want BadRequest", err)
	}
}

func TestRemoteStoreGetPassesProtocolOptions(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	object := &store.Unstructured{}
	object.SetResource("widgets")
	labelRequirement := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("visibility", "public"),
			selector.RequirementEqual("owner", "alice"),
		},
	}
	fieldRequirement := selector.Requirement{
		Operator: selector.Not,
		Requirements: store.Requirements{
			selector.RequirementEqual("state", "blocked"),
		},
	}

	if err := remote.Get(context.Background(), "widget-1", object,
		store.WithResourceVersion(7),
		store.WithFields("id", "name"),
		store.WithLabelRequirements(labelRequirement),
		store.WithFieldRequirements(fieldRequirement),
	); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if underlying.getOptions.ResourceVersion == nil || *underlying.getOptions.ResourceVersion != 7 {
		t.Fatalf("GetOptions.ResourceVersion = %#v", underlying.getOptions.ResourceVersion)
	}
	if !reflect.DeepEqual(underlying.getOptions.Fields, []string{"id", "name"}) {
		t.Fatalf("GetOptions.Fields = %#v", underlying.getOptions.Fields)
	}
	if !reflect.DeepEqual(underlying.getOptions.LabelRequirements, store.Requirements{labelRequirement}) {
		t.Fatalf("LabelRequirements = %#v", underlying.getOptions.LabelRequirements)
	}
	if !reflect.DeepEqual(underlying.getOptions.FieldRequirements, store.Requirements{fieldRequirement}) {
		t.Fatalf("FieldRequirements = %#v", underlying.getOptions.FieldRequirements)
	}
}

func TestRemoteStorePatchBatchPassesSelectors(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	list := &store.List[store.Unstructured]{Resource: "widgets"}
	labelRequirement := selector.RequirementEqual("environment", "test")
	fieldRequirement := selector.RequirementEqual("enabled", "true")

	err := remote.PatchBatch(context.Background(), list,
		store.MapMergePatchBacth{"enabled": false},
		store.WithLabelRequirements(labelRequirement),
		store.WithFieldRequirements(fieldRequirement),
	)
	if err != nil {
		t.Fatalf("PatchBatch() error = %v", err)
	}
	if !reflect.DeepEqual(underlying.patchBatchOptions.LabelRequirements, store.Requirements{labelRequirement}) {
		t.Fatalf("LabelRequirements = %#v, want %#v",
			underlying.patchBatchOptions.LabelRequirements, store.Requirements{labelRequirement})
	}
	if !reflect.DeepEqual(underlying.patchBatchOptions.FieldRequirements, store.Requirements{fieldRequirement}) {
		t.Fatalf("FieldRequirements = %#v, want %#v",
			underlying.patchBatchOptions.FieldRequirements, store.Requirements{fieldRequirement})
	}
}

func TestRemoteStoreDeletePassesConditions(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	object := &store.Unstructured{}
	object.SetResource("widgets")
	object.SetID("widget-1")
	labelRequirement := selector.RequirementEqual("environment", "test")
	fieldRequirement := selector.RequirementEqual("enabled", "true")

	if err := remote.Delete(context.Background(), object,
		store.WithLabelRequirements(labelRequirement),
		store.WithFieldRequirements(fieldRequirement),
		store.WithUID("uid-1"),
		store.WithResourceVersion(7),
	); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !reflect.DeepEqual(underlying.deleteOptions.LabelRequirements, store.Requirements{labelRequirement}) {
		t.Fatalf("LabelRequirements = %#v", underlying.deleteOptions.LabelRequirements)
	}
	if !reflect.DeepEqual(underlying.deleteOptions.FieldRequirements, store.Requirements{fieldRequirement}) {
		t.Fatalf("FieldRequirements = %#v", underlying.deleteOptions.FieldRequirements)
	}
	if underlying.deleteOptions.Preconditions == nil ||
		*underlying.deleteOptions.Preconditions.UID != "uid-1" ||
		*underlying.deleteOptions.Preconditions.ResourceVersion != 7 {
		t.Fatalf("Preconditions = %#v", underlying.deleteOptions.Preconditions)
	}
}

func Test_decodePath(t *testing.T) {
	tests := []struct {
		rpath string
		want  store.ResourcedObjectReference
	}{
		{
			rpath: "/scope1/name/scope2/name/resource/name",
			want: store.ResourcedObjectReference{
				ID:       "name",
				Resource: "resource",
				Scopes: []store.Scope{
					{Resource: "scope1", Name: "name"},
					{Resource: "scope2", Name: "name"},
				},
			},
		},
		{
			rpath: "/scope1/name/scope2/name/resource/",
			want: store.ResourcedObjectReference{
				Resource: "resource",
				Scopes: []store.Scope{
					{Resource: "scope1", Name: "name"},
					{Resource: "scope2", Name: "name"},
				},
			},
		},
		{
			rpath: "/scope1/name/scope2/name/resource",
			want: store.ResourcedObjectReference{
				Resource: "resource",
				Scopes: []store.Scope{
					{Resource: "scope1", Name: "name"},
					{Resource: "scope2", Name: "name"},
				},
			},
		},
		{
			rpath: "/resource",
			want: store.ResourcedObjectReference{
				Resource: "resource",
				Scopes:   []store.Scope{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.rpath, func(t *testing.T) {
			if got := decodePath(tt.rpath); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRemoteGetPreservesExactPreconditionsAndReadVersion(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	uid := ""
	exactVersion := int64(0)
	object := &store.Unstructured{}
	object.SetResource("widgets")
	if err := remote.Get(t.Context(), "one", object,
		store.WithResourceVersion(7),
		store.WithPreconditions(store.Preconditions{UID: &uid, ResourceVersion: &exactVersion}),
	); err != nil {
		t.Fatal(err)
	}
	options := underlying.getOptions
	if options.ResourceVersion == nil || *options.ResourceVersion != 7 || options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != "" || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != 0 {
		t.Fatalf("read and exact versions were not preserved independently: %#v", options)
	}
}

func (s *optionCaptureStore) Patch(_ context.Context, object store.Object, _ store.Patch, options ...store.PatchOption) error {
	s.patchOptions = store.ApplyPatchOptions(options)
	return nil
}

func TestRemotePatchPassesExactPreconditions(t *testing.T) {
	underlying := &optionCaptureStore{}
	remote := newCaptureRemoteStore(t, underlying)
	object := &store.Unstructured{}
	object.SetResource("widgets")
	object.SetID("one")
	zero := int64(0)
	uid := "expected"
	if err := remote.Patch(t.Context(), object, store.MapMergePatch{"name": "updated"}, store.WithPreconditions(store.Preconditions{UID: &uid, ResourceVersion: &zero})); err != nil {
		t.Fatal(err)
	}
	conditions := underlying.patchOptions.Preconditions
	if conditions == nil || conditions.ResourceVersion == nil || *conditions.ResourceVersion != 0 || conditions.UID == nil || *conditions.UID != uid {
		t.Fatalf("Patch preconditions: %#v", conditions)
	}
}

func TestRemoteStoreConformance(t *testing.T) {
	capabilities := store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
	storetest.Run(t, storetest.Fixture{
		Capabilities:     capabilities,
		Continue:         true,
		LabelSelector:    true,
		FieldSelector:    true,
		Search:           true,
		Sort:             true,
		SubScopes:        true,
		DeleteBatch:      true,
		PatchBatch:       true,
		SecondaryIndexes: true,
		UniqueIndexes:    true,

		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			underlying, err := inmemory.New(schema)
			if err != nil {
				return nil, err
			}
			handler := api.New().
				Group(NewServer(underlying).
					Group()).
				Build()
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			endpoint, err := url.Parse(server.URL)
			if err != nil {
				return nil, err
			}
			return NewRemoteStore(endpoint, schema, capabilities), nil
		},
	})
}
