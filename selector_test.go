package store_test

import (
	"testing"

	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
)

func TestMatchUnstructuredFieldRequirements(t *testing.T) {
	obj := &store.Unstructured{Object: map[string]any{
		"enabled": true,
		"score":   1.5,
		"name":    "example-widget",
		"tags":    []any{"blue", "stable"},
	}}
	tests := []struct {
		name string
		req  selector.Requirement
		want bool
	}{
		{
			name: "textual boolean equals typed boolean",
			req:  selector.RequirementEqual("enabled", "true"),
			want: true,
		},
		{
			name: "fractional greater than",
			req:  selector.NewRequirement("score", selector.GreaterThan, 1.0),
			want: true,
		},
		{
			name: "greater than or equal",
			req:  selector.NewRequirement("score", selector.GreaterThanOrEqual, 1.5),
			want: true,
		},
		{
			name: "less than or equal",
			req:  selector.NewRequirement("score", selector.LessThanOrEqual, 1.5),
			want: true,
		},
		{
			name: "slice contains",
			req:  selector.NewRequirement("tags", selector.Contains, "stable"),
			want: true,
		},
		{
			name: "string contains",
			req:  selector.NewRequirement("name", selector.Contains, "widget"),
			want: true,
		},
		{
			name: "like",
			req:  selector.NewRequirement("name", selector.Like, "widget"),
			want: true,
		},
		{
			name: "missing comparison value",
			req:  selector.NewRequirement("score", selector.GreaterThan),
			want: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := store.MatchUnstructuredFieldRequirments(obj, store.Requirements{test.req}); got != test.want {
				t.Fatalf("MatchUnstructuredFieldRequirments() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRecursiveRequirementMatching(t *testing.T) {
	objects := map[string]*store.Unstructured{
		"public": {Object: map[string]any{"visibility": "public", "owner": "bob"}},
		"owned":  {Object: map[string]any{"visibility": "private", "owner": "alice"}},
		"denied": {Object: map[string]any{"visibility": "private", "owner": "bob"}},
	}
	visible := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("visibility", "public"),
			selector.RequirementEqual("owner", "alice"),
		},
	}
	notDenied := selector.Requirement{
		Operator: selector.Not,
		Requirements: store.Requirements{
			selector.RequirementEqual("owner", "blocked"),
		},
	}
	requirements := store.Requirements{visible, notDenied}

	for name, object := range objects {
		want := name != "denied"
		if got := store.MatchUnstructuredFieldRequirments(object, requirements); got != want {
			t.Fatalf("MatchUnstructuredFieldRequirments(%q) = %v, want %v", name, got, want)
		}
	}
	if store.MatchUnstructuredFieldRequirments(objects["public"], store.Requirements{{}}) {
		t.Fatal("zero Requirement matched an object")
	}
	if !store.MatchUnstructuredFieldRequirments(objects["public"], store.Requirements{{Operator: selector.All}}) {
		t.Fatal("All did not match an object")
	}
	if !store.MatchUnstructuredFieldRequirments(objects["public"], store.Requirements{{Operator: selector.And}}) {
		t.Fatal("empty And did not match an object")
	}
	if store.MatchUnstructuredFieldRequirments(objects["public"], store.Requirements{{Operator: selector.Or}}) {
		t.Fatal("empty Or matched an object")
	}
}
