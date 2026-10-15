package etcdcache

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"xiaoshiai.cn/common/store"
)

func TestSortUnstructuredListNumericScores(t *testing.T) {
	items := []StorageObject{
		{Object: map[string]any{"id": "low", "recommendation": map[string]any{"score": int64(10)}}},
		{Object: map[string]any{"id": "high", "recommendation": map[string]any{"score": int64(100)}}},
		{Object: map[string]any{"id": "middle", "recommendation": map[string]any{"score": int64(90)}}},
	}
	SortUnstructuredList(items, store.ParseSorts("recommendation.score-,id+"))
	got := make([]string, len(items))
	for i, item := range items {
		got[i] = GetNestedString(item.Object, "id")
	}
	if want := []string{"high", "middle", "low"}; !slices.Equal(got, want) {
		t.Fatalf("recommendation order = %v, want %v", got, want)
	}
}

type numericSortObject struct {
	store.ObjectMeta `json:",inline"`
	Recommendation   map[string]int64  `json:"recommendation,omitempty"`
	LatestVersion    map[string]string `json:"latestVersion,omitempty"`
}

func TestEtcdCacherNumericSortBeforePagination(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&numericSortObject{}, store.ResourceSchema{
		ScopeKeys: []string{"tenant"},
		Indexes: []store.Index{
			{Fields: []string{"recommendation.score"}},
			{Fields: []string{"latestVersion.publicationTimestamp"}},
		},
	}); err != nil {
		t.Fatal(err)
	}
	storage := newTestStore(t, t.Context(), testserver.RunEtcd(t, nil), schema)
	for _, item := range []struct {
		tenant      string
		id          string
		score       int64
		publication string
	}{
		{tenant: "a", id: "low", score: 10},
		{tenant: "a", id: "high-old", score: 100, publication: "2026-09-01T00:00:00Z"},
		{tenant: "a", id: "high-new", score: 100, publication: "2026-09-02T00:00:00Z"},
		{tenant: "b", id: "middle", score: 90},
		{tenant: "a", id: "zero", score: 0},
		{tenant: "a", id: "unset", score: -1},
	} {
		object := &numericSortObject{
			ObjectMeta:    store.ObjectMeta{ID: item.id},
			LatestVersion: map[string]string{"publicationTimestamp": item.publication},
		}
		if item.score >= 0 {
			object.Recommendation = map[string]int64{"score": item.score}
		}
		if err := storage.Scope(store.Scope{Resource: "tenants", Name: item.tenant}).Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	assertEventually(t, 5*time.Second, func() error {
		list := &store.List[numericSortObject]{}
		if err := storage.List(t.Context(), list, store.WithSubScopes()); err != nil {
			return err
		}
		if len(list.Items) != 6 {
			return fmt.Errorf("cache contains %d objects, want 6", len(list.Items))
		}
		return nil
	})
	const sort = "recommendation.score-,latestVersion.publicationTimestamp-,time-,tenant+,id+"
	for _, tt := range []struct {
		name   string
		tenant string
		page   int
		want   []string
	}{
		{name: "first page", page: 1, want: []string{"high-new", "high-old"}},
		{name: "second page", page: 2, want: []string{"middle", "low"}},
		{name: "unrecommended page", page: 3, want: []string{"zero", "unset"}},
		{name: "tenant first page", tenant: "a", page: 1, want: []string{"high-new", "high-old"}},
		{name: "tenant second page", tenant: "a", page: 2, want: []string{"low", "zero"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var scoped store.Store = storage
			wantTotal := 6
			options := []store.ListOption{store.WithPage(tt.page, 2), store.WithSort(sort)}
			if tt.tenant == "" {
				options = append(options, store.WithSubScopes())
			} else {
				scoped = storage.Scope(store.Scope{Resource: "tenants", Name: tt.tenant})
				wantTotal = 5
			}
			list := &store.List[numericSortObject]{}
			if err := scoped.List(t.Context(), list, options...); err != nil {
				t.Fatal(err)
			}
			got := make([]string, len(list.Items))
			for i, item := range list.Items {
				got[i] = item.ID
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("page order = %v, want %v", got, tt.want)
			}
			if list.Total == nil || *list.Total != wantTotal {
				t.Fatalf("total = %v, want %d", list.Total, wantTotal)
			}
		})
	}
}

func TestSortUnstructuredListPreservesJSONTypes(t *testing.T) {
	tests := []struct {
		name string
		json string
		sort string
		want []string
	}{
		{
			name: "integers ascending",
			json: `[{"id":"100","value":100},{"id":"10","value":10},{"id":"90","value":90}]`,
			sort: "value+",
			want: []string{"10", "90", "100"},
		},
		{
			name: "mixed decimal and integer numbers",
			json: `[{"id":"low","value":9.5},{"id":"high","value":100},{"id":"middle","value":90.5},{"id":"negative","value":-2}]`,
			sort: "value-",
			want: []string{"high", "middle", "low", "negative"},
		},
		{
			name: "large integers retain precision",
			json: `[{"id":"low","value":9007199254740992},{"id":"high","value":9007199254740993},{"id":"decimal","value":9007199254740992.0}]`,
			sort: "value-,id+",
			want: []string{"high", "decimal", "low"},
		},
		{
			name: "unrecommended values follow scores",
			json: `[{"id":"missing"},{"id":"zero","recommendation":{"score":0}},{"id":"null","recommendation":null},{"id":"recommended","recommendation":{"score":10}}]`,
			sort: "recommendation.score-,id+",
			want: []string{"recommended", "zero", "missing", "null"},
		},
		{
			name: "equal scores use secondary fields",
			json: `[{"id":"old","recommendation":{"score":100},"latestVersion":{"publicationTimestamp":"2026-09-01T00:00:00Z"}},{"id":"low","recommendation":{"score":90},"latestVersion":{"publicationTimestamp":"2026-09-24T00:00:00Z"}},{"id":"new","recommendation":{"score":100},"latestVersion":{"publicationTimestamp":"2026-09-02T00:00:00Z"}}]`,
			sort: "recommendation.score-,latestVersion.publicationTimestamp-,id+",
			want: []string{"new", "old", "low"},
		},
		{
			name: "numeric strings remain lexical",
			json: `[{"id":"100","value":"100"},{"id":"10","value":"10"},{"id":"90","value":"90"}]`,
			sort: "value-",
			want: []string{"90", "100", "10"},
		},
		{
			name: "boolean ordering is preserved",
			json: `[{"id":"false","value":false},{"id":"true","value":true}]`,
			sort: "value-",
			want: []string{"true", "false"},
		},
		{
			name: "creation time alias is preserved",
			json: `[{"id":"old","creationTimestamp":"2026-09-01T00:00:00Z"},{"id":"new","creationTimestamp":"2026-09-02T00:00:00Z"}]`,
			sort: "time-",
			want: []string{"new", "old"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []map[string]any
			if err := JsonUnmarshal([]byte(tt.json), &objects); err != nil {
				t.Fatal(err)
			}
			items := make([]StorageObject, len(objects))
			for i, object := range objects {
				items[i] = StorageObject{Object: object}
			}
			SortUnstructuredList(items, store.ParseSorts(tt.sort))
			got := make([]string, len(items))
			for i, item := range items {
				got[i] = GetNestedString(item.Object, "id")
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("order = %v, want %v", got, tt.want)
			}
		})
	}
}
