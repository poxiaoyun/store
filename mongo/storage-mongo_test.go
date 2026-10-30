package mongo

import (
	"context"
	"encoding/json"
	"net"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"k8s.io/apimachinery/pkg/api/resource"
	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/common/meta"
	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
	"xiaoshiai.cn/store/storetest"
)

type TestObject struct {
	store.ObjectMeta `json:",inline"`
	Status           TestObjectStatus `json:"status"`
}

type TestObjectStatus struct {
	Val   string   `json:"val"`
	Int   int      `json:"int"`
	Slice []string `json:"slice"`
}

type fieldPatchObject struct {
	store.ObjectMeta `json:",inline"`
	Balance          int64             `json:"balance"`
	Note             string            `json:"note,omitempty"`
	Nested           *fieldPatchNested `json:"nested,omitempty"`
	Status           fieldPatchStatus  `json:"status"`
	At               meta.Time         `json:"at,omitempty"`
	Amount           fieldPatchAmount  `json:"amount,omitempty"`
}

// fieldPatchAmount marshals as a decimal string, like an exact amount. Storage
// must keep it numeric even when a patch supplies the typed value.
type fieldPatchAmount int64

func (a fieldPatchAmount) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatInt(int64(a), 10))
}

func (a *fieldPatchAmount) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return err
	}
	*a = fieldPatchAmount(value)
	return nil
}

type fieldPatchNested struct {
	Count int    `json:"count"`
	Keep  string `json:"keep"`
}

type fieldPatchStatus struct {
	Phase string `json:"phase,omitempty"`
}

func TestMongoStorageDottedScopeRoundTrip(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	root := NewIntegrationStorage(t, database, &TestObject{})
	scopes := []store.Scope{{Resource: "iam.organization", Name: "one"}, {Resource: "moha.repository", Name: "private"}}
	storage := root.Scope(scopes...)
	created := &TestObject{ObjectMeta: store.ObjectMeta{ID: "policy"}}
	if err := storage.Create(t.Context(), created); err != nil {
		t.Fatal(err)
	}
	// A BSON document's field order is not part of a Scope component's
	// identity. Unstructured objects can serialize the two keys in this order.
	if _, err := database.Collection("testobjects").
		UpdateOne(t.Context(), bson.D{{Key: "id", Value: created.ID}}, bson.D{{Key: "$set", Value: bson.D{{Key: "scopes", Value: bson.A{
			bson.D{{Key: "name", Value: "one"}, {Key: "resource", Value: "iam.organization"}},
			bson.D{{Key: "name", Value: "private"}, {Key: "resource", Value: "moha.repository"}},
		}}}}}); err != nil {
		t.Fatal(err)
	}
	got := &TestObject{}
	if err := storage.Get(t.Context(), created.ID, got); err != nil {
		t.Fatalf("Get immediately after Create in identical dotted scope: %v", err)
	}
	if !reflect.DeepEqual(got.Scopes, scopes) {
		t.Fatalf("Scopes = %#v, want %#v", got.Scopes, scopes)
	}
}

func TestMongoStorageScopeDerivationIsIndependent(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	root := NewIntegrationStorage(t, database, &TestObject{})
	path := []store.Scope{
		{Resource: "organizations", Name: "one"},
		{Resource: "projects", Name: "one"},
		{Resource: "repositories", Name: "one"},
	}
	parent := root.Scope(path[0])
	parent = parent.Scope(path[1])
	parent = parent.Scope(path[2])
	left := parent.Scope(store.Scope{Resource: "branches", Name: "left"})
	right := parent.Scope(store.Scope{Resource: "branches", Name: "right"})
	object := &TestObject{ObjectMeta: store.ObjectMeta{ID: "left"}}
	if err := left.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	expected := append(slices.Clone(path), store.Scope{Resource: "branches", Name: "left"})
	if !slices.Equal(object.Scopes, expected) {
		t.Fatalf("deriving right changed left scope: %#v", object.Scopes)
	}
	object.Scopes[0].Name = "mutated"
	got := &TestObject{}
	if err := left.Get(t.Context(), "left", got); err != nil || !slices.Equal(got.Scopes, expected) {
		t.Fatalf("mutating returned metadata changed Store scope: %#v, %v", got, err)
	}
	if err := right.Get(t.Context(), "left", &TestObject{}); !commonerrors.IsNotFound(err) {
		t.Fatalf("sibling derived scope leaked left object: %v", err)
	}
}

type scopedCodecObject struct {
	store.ObjectMeta `json:",inline"`
	Quantity         resource.Quantity `json:"quantity"`
	ObservedAt       meta.Time         `json:"observedAt"`
	Address          net.IP            `json:"address,omitempty"`
	Revision         int64             `json:"revision,omitempty"`
	Status           TestObjectStatus  `json:"status"`
}

func TestMongoStorageScopedObjectCodecs(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	root := NewStatusIntegrationStorage(t, database, &scopedCodecObject{})
	scopes := []store.Scope{{Resource: "iam.organization", Name: "one"}}
	storage := root.Scope(scopes...)
	object := &scopedCodecObject{
		ObjectMeta: store.ObjectMeta{ID: "typed", Labels: map[string]string{"example.com/key": "value"}},
		Quantity:   resource.MustParse("2Gi"),
		ObservedAt: meta.Time{Time: time.Date(2026, time.September, 14, 0, 0, 0, 0, time.UTC)},
		Address:    net.ParseIP("192.0.2.10"),
		Revision:   9007199254740993,
	}
	if err := storage.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	got := &scopedCodecObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.ID != object.ID || got.UID == "" || got.Generation != 1 || got.ResourceVersion < 1 || !slices.Equal(got.Scopes, scopes) || got.Labels["example.com/key"] != "value" {
		t.Fatalf("embedded metadata was not preserved: %#v", got.ObjectMeta)
	}
	if got.Quantity.Cmp(object.Quantity) != 0 || !got.ObservedAt.Time.Equal(object.ObservedAt.Time) || !got.Address.Equal(object.Address) || got.Revision != object.Revision {
		t.Fatalf("BSON codecs changed business values: %#v", got)
	}
	got.Status.Val = "ready"
	got.Quantity = resource.MustParse("1Gi")
	if err := storage.Status().
		Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Val != "ready" || got.Quantity.Cmp(object.Quantity) != 0 || got.Revision != object.Revision || !slices.Equal(got.Scopes, scopes) {
		t.Fatalf("status update changed typed business fields or scopes: %#v", got)
	}
}

func TestMongoStorageScopeHierarchy(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	root := NewIntegrationStorage(t, database, &TestObject{})
	organization := store.Scope{Resource: "iam.organization", Name: "one"}
	repository := store.Scope{Resource: "moha.repository", Name: "private"}
	leaf := store.Scope{Resource: "moha.repository", Name: "nested"}
	type scopedFixture struct {
		id     string
		scopes []store.Scope
	}
	objects := []scopedFixture{
		{id: "root"},
		{id: "organization", scopes: []store.Scope{organization}},
		{id: "repository", scopes: []store.Scope{organization, repository}},
		{id: "nested", scopes: []store.Scope{organization, repository, leaf}},
		{id: "sibling", scopes: []store.Scope{{Resource: organization.Resource, Name: "other"}, repository}},
		{id: "reordered", scopes: []store.Scope{repository, organization}},
	}
	for _, target := range objects {
		storage := root.Scope(target.scopes...)
		object := &TestObject{ObjectMeta: store.ObjectMeta{ID: target.id, Description: "original"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create %s: %v", target.id, err)
		}
	}
	for _, target := range objects {
		t.Run(target.id, func(t *testing.T) {
			storage := root.Scope(target.scopes...)
			for _, candidate := range objects {
				got := &TestObject{}
				err := storage.Get(t.Context(), candidate.id, got)
				if candidate.id != target.id {
					if !commonerrors.IsNotFound(err) {
						t.Fatalf("Get crossed from %s to %s: %#v, %v", target.id, candidate.id, got, err)
					}
					continue
				}
				if err != nil || !slices.Equal(got.Scopes, candidate.scopes) {
					t.Fatalf("Get %s lost scope: %#v, %v", candidate.id, got.Scopes, err)
				}
			}
			for _, descendants := range []bool{false, true} {
				listOptions := []store.ListOption{store.WithSort("id+"), store.WithFields("id")}
				var countOptions []store.CountOption
				if descendants {
					listOptions = append(listOptions, store.WithSubScopes())
					countOptions = append(countOptions, store.WithSubScopes())
				}
				list := &store.List[TestObject]{}
				if err := storage.List(t.Context(), list, listOptions...); err != nil {
					t.Fatal(err)
				}
				var expected []string
				for _, candidate := range objects {
					matches := slices.Equal(candidate.scopes, target.scopes)
					if descendants {
						matches = len(candidate.scopes) >= len(target.scopes) && slices.Equal(candidate.scopes[:len(target.scopes)], target.scopes)
					}
					if matches {
						expected = append(expected, candidate.id)
					}
				}
				slices.Sort(expected)
				var got []string
				for _, item := range list.Items {
					got = append(got, item.ID)
					index := slices.IndexFunc(objects, func(candidate scopedFixture) bool {
						return candidate.id == item.ID
					})
					if index < 0 || !slices.Equal(item.Scopes, objects[index].scopes) {
						t.Fatalf("projected List lost object scope: %#v", item)
					}
				}
				if !slices.Equal(got, expected) {
					t.Fatalf("List descendants=%v got %v, want %v", descendants, got, expected)
				}
				count, err := storage.Count(t.Context(), &TestObject{}, countOptions...)
				if err != nil || count != len(expected) {
					t.Fatalf("Count descendants=%v = %d, %v; want %d", descendants, count, err, len(expected))
				}
			}
		})
	}
	parent := root.Scope(organization)
	list := &store.List[TestObject]{}
	if err := parent.List(t.Context(), list, store.WithSubScopes(), store.WithSort("id+"), store.WithOffset(1), store.WithLimit(1)); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].ID != "organization" {
		t.Fatalf("scope filtering must precede pagination and count: %#v", list)
	}
	if err := parent.PatchBatch(t.Context(), &store.List[TestObject]{}, store.RawPatchBatch(store.PatchTypeMergePatch, []byte(`{"description":"patched","scopes":[]}`))); err != nil {
		t.Fatal(err)
	}
	got := &TestObject{}
	if err := parent.Get(t.Context(), "organization", got); err != nil || got.Description != "patched" || !slices.Equal(got.Scopes, []store.Scope{organization}) {
		t.Fatalf("batch patch moved object scope: %#v, %v", got, err)
	}
	child := parent.Scope(repository)
	got = &TestObject{}
	if err := child.Get(t.Context(), "repository", got); err != nil || got.Description != "original" {
		t.Fatalf("parent batch patch reached child: %#v, %v", got, err)
	}
	if err := parent.DeleteBatch(t.Context(), &store.List[TestObject]{}); err != nil {
		t.Fatal(err)
	}
	if err := child.Get(t.Context(), "repository", &TestObject{}); err != nil {
		t.Fatalf("parent batch delete reached child: %v", err)
	}
	got.Description = "updated"
	if err := child.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	if err := child.Patch(t.Context(), got, store.RawPatch(store.PatchTypeMergePatch, []byte(`{"description":"single patch"}`))); err != nil {
		t.Fatal(err)
	}
	if err := child.Delete(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	if err := child.Get(t.Context(), "repository", &TestObject{}); !commonerrors.IsNotFound(err) {
		t.Fatalf("Delete did not remove exact object: %v", err)
	}
}

func TestMongoStorageRejectsUnsupportedNestedRequirement(t *testing.T) {
	requirement := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("name", "exact"),
			selector.NewRequirement("name", selector.Like, "partial"),
		},
	}
	list := &store.List[store.Unstructured]{Resource: "testobjects"}
	err := (&MongoStorage{}).List(t.Context(), list, store.WithFieldRequirements(requirement))
	if !commonerrors.IsUnsupported(err) {
		t.Fatalf("List() error = %v, want Unsupported", err)
	}
}

func TestMongoStorageIntegration(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	storage := NewIntegrationStorage(t, database, &TestObject{})

	objects := []*TestObject{
		{
			ObjectMeta: store.ObjectMeta{
				ID:   "platform",
				Name: "Platform",
				Labels: map[string]string{
					"example.com/team": "platform",
					"$owner":           "alice",
				},
				Annotations: map[string]string{
					"example.com/trace": "trace-1",
					"$reference":        "reference-1",
				},
			},
		},
		{
			ObjectMeta: store.ObjectMeta{
				ID:   "infrastructure",
				Name: "Infrastructure",
				Labels: map[string]string{
					"example.com/team": "infrastructure",
				},
			},
		},
		{
			ObjectMeta: store.ObjectMeta{
				ID:   "unlabeled",
				Name: "Unlabeled",
			},
		},
	}
	for _, object := range objects {
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create(%q) error = %v", object.ID, err)
		}
	}

	stored := &TestObject{}
	if err := storage.Get(t.Context(), "platform", stored); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !reflect.DeepEqual(stored.Labels, objects[0].Labels) {
		t.Fatalf("Get() labels = %#v, want %#v", stored.Labels, objects[0].Labels)
	}
	if !reflect.DeepEqual(stored.Annotations, objects[0].Annotations) {
		t.Fatalf("Get() annotations = %#v, want %#v", stored.Annotations, objects[0].Annotations)
	}

	tests := []struct {
		name        string
		requirement selector.Requirement
		want        []string
	}{
		{
			name:        "equals literal dotted key",
			requirement: selector.RequirementEqual("example.com/team", "platform"),
			want:        []string{"platform"},
		},
		{
			name:        "not equals includes missing key",
			requirement: selector.RequirementNotEqual("example.com/team", "platform"),
			want:        []string{"infrastructure", "unlabeled"},
		},
		{
			name:        "in",
			requirement: selector.RequirementIn("example.com/team", "platform"),
			want:        []string{"platform"},
		},
		{
			name:        "not in includes missing key",
			requirement: selector.RequirementNotIn("example.com/team", "platform"),
			want:        []string{"infrastructure", "unlabeled"},
		},
		{
			name:        "empty in matches nothing",
			requirement: selector.RequirementIn("example.com/team"),
			want:        []string{},
		},
		{
			name:        "empty not in matches nothing",
			requirement: selector.RequirementNotIn("example.com/team"),
			want:        []string{},
		},
		{
			name:        "exists",
			requirement: selector.RequirementExists("example.com/team"),
			want:        []string{"infrastructure", "platform"},
		},
		{
			name:        "does not exist",
			requirement: selector.RequirementDoesNotExist("example.com/team"),
			want:        []string{"unlabeled"},
		},
		{
			name:        "equals literal dollar key",
			requirement: selector.RequirementEqual("$owner", "alice"),
			want:        []string{"platform"},
		},
	}
	for _, current := range tests {
		t.Run(current.name, func(t *testing.T) {
			list := &store.List[TestObject]{}
			if err := storage.List(
				t.Context(),
				list,
				store.WithLabelRequirements(current.requirement),
			); err != nil {
				t.Fatalf("List() error = %v", err)
			}
			got := make([]string, 0, len(list.Items))
			for _, item := range list.Items {
				got = append(got, item.ID)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, current.want) {
				t.Fatalf("List() IDs = %v, want %v", got, current.want)
			}
		})
	}
}

func TestStoreConformance(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	capabilities := store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
	storetest.Run(t, storetest.Fixture{
		Capabilities:     capabilities,
		JSONPatchSetOnly: true,
		Continue:         true,
		LabelSelector:    true,
		FieldSelector:    true,
		Search:           true,
		Sort:             true,
		SubScopes:        true,
		Projection:       true,
		DeleteBatch:      true,
		PatchBatch:       true,
		Transaction:      true,
		SecondaryIndexes: true,
		UniqueIndexes:    true,

		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			database := RequireIntegrationDatabase(t, uri)
			core := &MongoStorageCore{
				schema:       schema,
				db:           database,
				bsonRegistry: GlobalBsonRegistry,
				bsonOptions: &mongooptions.BSONOptions{
					UseJSONStructTags: true,
					OmitZeroStruct:    true,
				},
				collections:    map[string]*mongo.Collection{},
				collectionLock: sync.RWMutex{},
				logger: log.FromContext(t.Context()).
					WithName("mongo-storage"),
			}
			if err := core.initCollections(t.Context()); err != nil {
				return nil, err
			}
			return &MongoStorage{core: core}, nil
		},
	})
}

func TestMongoStorageBatchPatchAdvancesResourceVersion(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	storage := NewIntegrationStorage(t, database, &TestObject{})

	object := &TestObject{ObjectMeta: store.ObjectMeta{ID: "batch-version"}}
	if err := storage.Create(t.Context(), object); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	previous := object.ResourceVersion

	list := &store.List[TestObject]{}
	if err := storage.PatchBatch(
		t.Context(),
		list,
		store.RawPatchBatch(store.PatchTypeMergePatch, []byte(`{"description":"patched"}`)),
		store.WithFieldRequirements(selector.RequirementEqual("id", object.ID)),
	); err != nil {
		t.Fatalf("PatchBatch() error = %v", err)
	}

	got := &TestObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.ResourceVersion <= previous || got.Description != "patched" {
		t.Fatalf("PatchBatch() object = %#v, want description patched and version > %d", got, previous)
	}
}

// RequireIntegrationDatabase creates an isolated database with the store's BSON codecs.
func RequireIntegrationDatabase(t testing.TB, uri string) *mongo.Database {
	t.Helper()
	return testmongodb.RequireDatabase(
		t,
		uri,
		mongooptions.Client().
			SetBSONOptions(&mongooptions.BSONOptions{
				UseJSONStructTags: true,
				OmitZeroStruct:    true,
			}).
			SetRegistry(GlobalBsonRegistry),
	)
}

// NewIntegrationStorage creates a store over a testkit-managed database.
func NewIntegrationStorage(t testing.TB, database *mongo.Database, objects ...store.Object) *MongoStorage {
	t.Helper()
	schema := store.NewSchema()
	for _, object := range objects {
		if err := schema.Register(object, store.ResourceSchema{}); err != nil {
			t.Fatalf("register schema: %v", err)
		}
	}
	return newIntegrationStorage(t, database, schema)
}

func NewStatusIntegrationStorage(t testing.TB, database *mongo.Database, object store.Object) *MongoStorage {
	t.Helper()
	schema := store.NewSchema()
	if err := schema.Register(object, store.ResourceSchema{StatusResource: true}); err != nil {
		t.Fatalf("register schema: %v", err)
	}
	return newIntegrationStorage(t, database, schema)
}

func newIntegrationStorage(t testing.TB, database *mongo.Database, schema *store.Schema) *MongoStorage {
	t.Helper()
	core := &MongoStorageCore{
		schema:       schema,
		db:           database,
		bsonRegistry: GlobalBsonRegistry,
		bsonOptions: &mongooptions.BSONOptions{
			UseJSONStructTags: true,
			OmitZeroStruct:    true,
		},
		collections:    map[string]*mongo.Collection{},
		collectionLock: sync.RWMutex{},
		logger: log.FromContext(t.Context()).
			WithName("mongo-storage"),
	}
	if err := core.initCollections(t.Context()); err != nil {
		t.Fatalf("initialize MongoDB collections: %v", err)
	}
	return &MongoStorage{core: core}
}

func TestSelectorMatchUsesLiteralLabelKeys(t *testing.T) {
	tests := []struct {
		name        string
		requirement selector.Requirement
		want        bson.D
	}{
		{
			name:        "equals",
			requirement: selector.RequirementEqual("example.com/team", "platform"),
			want:        labelExpression("$labels", "example.com/team", "$eq", "platform"),
		},
		{
			name:        "double equals",
			requirement: selector.NewRequirement("example.com/team", selector.DoubleEquals, "platform"),
			want:        labelExpression("$labels", "example.com/team", "$eq", "platform"),
		},
		{
			name:        "not equals",
			requirement: selector.RequirementNotEqual("example.com/team", "platform"),
			want:        labelExpression("$labels", "example.com/team", "$ne", "platform"),
		},
		{
			name:        "in",
			requirement: selector.RequirementIn("example.com/team", "platform", "infrastructure"),
			want:        labelInExpression("$labels", "example.com/team", false, "platform", "infrastructure"),
		},
		{
			name:        "not in",
			requirement: selector.RequirementNotIn("example.com/team", "platform", "infrastructure"),
			want:        labelInExpression("$labels", "example.com/team", true, "platform", "infrastructure"),
		},
		{
			name:        "exists",
			requirement: selector.RequirementExists("example.com/team"),
			want:        labelExpression("$labels", "example.com/team", "$ne", nil),
		},
		{
			name:        "does not exist",
			requirement: selector.RequirementDoesNotExist("example.com/team"),
			want:        labelExpression("$labels", "example.com/team", "$eq", nil),
		},
		{
			name:        "dollar prefix",
			requirement: selector.RequirementEqual("$team", "platform"),
			want:        labelExpression("$labels", "$team", "$eq", "platform"),
		},
		{
			name:        "ordinary key",
			requirement: selector.RequirementEqual("app", "api"),
			want:        labelExpression("$labels", "app", "$eq", "api"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConditionsMatch(
				bson.D{},
				store.Requirements{tt.requirement},
				nil,
				"",
			)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("selector match = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSelectorMatchUsesAndWithFieldRequirements(t *testing.T) {
	labels := store.Requirements{
		{
			Key:      "example.com/team",
			Operator: selector.Equals,
			Values:   []any{"platform"},
		},
		{
			Key:      "app",
			Operator: selector.Exists,
		},
	}
	fields := store.Requirements{
		{
			Key:      "status.phase",
			Operator: selector.Equals,
			Values:   []any{"active"},
		},
	}
	want := bson.D{
		{Key: "$and", Value: bson.A{
			bson.D{{Key: "status.phase", Value: "active"}},
		}},
		{Key: "$expr", Value: bson.D{
			{Key: "$and", Value: bson.A{
				labelPredicate("$labels", "example.com/team", "$eq", "platform"),
				labelPredicate("$labels", "app", "$ne", nil),
			}},
		}},
	}

	got := ConditionsMatch(bson.D{}, labels, fields, "")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selector match = %#v, want %#v", got, want)
	}
}

func TestSelectorMatchSupportsRecursiveFieldRequirements(t *testing.T) {
	requirement := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("visibility", "public"),
			{
				Operator: selector.And,
				Requirements: store.Requirements{
					selector.RequirementEqual("owner", "alice"),
					{
						Operator: selector.Not,
						Requirements: store.Requirements{
							selector.RequirementEqual("state", "blocked"),
						},
					},
				},
			},
		},
	}
	want := bson.D{{Key: "$and", Value: bson.A{
		bson.D{{Key: "$or", Value: bson.A{
			bson.D{{Key: "visibility", Value: "public"}},
			bson.D{{Key: "$and", Value: bson.A{
				bson.D{{Key: "owner", Value: "alice"}},
				bson.D{{Key: "$nor", Value: bson.A{
					bson.D{{Key: "state", Value: "blocked"}},
				}}},
			}}},
		}}},
	}}}

	got := ConditionsMatch(bson.D{}, nil, store.Requirements{requirement}, "")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selector match = %#v, want %#v", got, want)
	}
}

func labelExpression(input, key, operator string, value any) bson.D {
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$and", Value: bson.A{
				labelPredicate(input, key, operator, value),
			}},
		}},
	}
}

func labelPredicate(input, key, operator string, value any) bson.D {
	right := any(bson.D{{Key: "$literal", Value: value}})
	if value == nil {
		right = nil
	}
	return bson.D{
		{Key: operator, Value: bson.A{
			literalLabelValue(input, key),
			right,
		}},
	}
}

func labelInExpression(input, key string, negate bool, values ...any) bson.D {
	predicate := bson.D{
		{Key: "$in", Value: bson.A{
			literalLabelValue(input, key),
			bson.D{{Key: "$literal", Value: bson.A(values)}},
		}},
	}
	if negate {
		predicate = bson.D{{Key: "$not", Value: bson.A{predicate}}}
	}
	return bson.D{
		{Key: "$expr", Value: bson.D{
			{Key: "$and", Value: bson.A{predicate}},
		}},
	}
}

func literalLabelValue(input, key string) bson.D {
	return bson.D{
		{Key: "$ifNull", Value: bson.A{
			bson.D{
				{Key: "$getField", Value: bson.D{
					{Key: "field", Value: bson.D{{Key: "$literal", Value: key}}},
					{Key: "input", Value: input},
				}},
			},
			nil,
		}},
	}
}

func TestMongoWatcherStopDoesNotCloseProducerChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	watcher := &MongoWatcher{
		results: make(chan store.WatchEvent, 1),
		cancel:  cancel,
	}
	watcher.Stop()
	watcher.Stop()

	select {
	case <-ctx.Done():
	default:
		t.Fatal("Stop() did not cancel the watcher context")
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("send after Stop() panicked: %v", recovered)
		}
	}()
	watcher.results <- store.WatchEvent{Type: store.WatchEventBookmark}
}

func TestPartialFilterExpression(t *testing.T) {
	want := bson.M{"$and": []bson.M{
		{"email": bson.M{"$exists": true}},
		{"tenant": bson.M{"$exists": true}},
	}}
	if got := PartialFilterExpression([]string{"email", "tenant"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("PartialFilterExpression() = %#v, want %#v", got, want)
	}
}
