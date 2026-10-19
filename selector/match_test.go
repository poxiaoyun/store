package selector_test

import (
	"testing"
	"time"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store/selector"
)

func TestRequirementsMatch(t *testing.T) {
	timestamp := time.Date(2026, time.August, 31, 12, 0, 0, 0, time.UTC)
	values := map[string]any{
		"created": meta.Time{Time: timestamp},
		"enabled": true,
		"name":    "api-server",
		"rank":    3,
		"tags":    []string{"blue", "stable"},
	}
	tests := []struct {
		name        string
		requirement selector.Requirement
		want        bool
	}{
		{name: "all", requirement: selector.Requirement{Operator: selector.All}, want: true},
		{name: "none", requirement: selector.Requirement{Operator: selector.None}},
		{name: "number compared with text", requirement: selector.NewRequirement("rank", selector.GreaterThan, "2"), want: true},
		{name: "boolean compared with text", requirement: selector.RequirementEqual("enabled", "true"), want: true},
		{name: "meta time compared with text", requirement: selector.NewRequirement("created", selector.GreaterThanOrEqual, timestamp.Format(time.RFC3339Nano)), want: true},
		{name: "string contains", requirement: selector.NewRequirement("name", selector.Contains, "api", "server"), want: true},
		{name: "collection contains", requirement: selector.NewRequirement("tags", selector.Contains, "stable"), want: true},
		{
			name: "recursive boolean",
			requirement: selector.Requirement{
				Operator: selector.And,
				Requirements: selector.Requirements{
					{
						Operator: selector.Or,
						Requirements: selector.Requirements{
							selector.RequirementEqual("rank", 1),
							selector.RequirementEqual("rank", 3),
						},
					},
					{
						Operator: selector.Not,
						Requirements: selector.Requirements{
							selector.RequirementEqual("enabled", false),
						},
					},
				},
			},
			want: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.requirement.Match(func(key string) (any, bool) {
				value, exists := values[key]
				return value, exists
			})
			if got != test.want {
				t.Fatalf("Match() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestRequirementMatchLabelsMissingKey(t *testing.T) {
	if selector.RequirementMatchLabels(selector.RequirementEqual("missing", ""), nil) {
		t.Fatal("missing label matched equality with an empty string")
	}
	if !selector.RequirementMatchLabels(selector.NewRequirement("missing", selector.NotEquals, "value"), nil) {
		t.Fatal("missing label did not match NotEquals")
	}
	if !selector.RequirementMatchLabels(selector.NewRequirement("missing", selector.NotIn, "value"), nil) {
		t.Fatal("missing label did not match NotIn")
	}
}

func TestRequirementMatchEmptySetValues(t *testing.T) {
	for _, operator := range []selector.Operator{selector.In, selector.NotIn, selector.Contains} {
		t.Run(string(operator), func(t *testing.T) {
			requirement := selector.NewRequirement("owner", operator)
			if requirement.Match(func(string) (any, bool) { return "alice", true }) {
				t.Fatal("Match() = true")
			}
		})
	}
}
