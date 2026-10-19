package etcdcache

import (
	"slices"
	"testing"

	"xiaoshiai.cn/store"
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
