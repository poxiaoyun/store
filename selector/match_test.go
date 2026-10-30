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
		{name: "number compared with text", requirement: selector.RequirementGreaterThan("rank", "2"), want: true},
		{name: "boolean compared with text", requirement: selector.RequirementEqual("enabled", "true"), want: true},
		{name: "meta time compared with text", requirement: selector.RequirementGreaterThanOrEqual("created", timestamp.Format(time.RFC3339Nano)), want: true},
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
	if !selector.RequirementMatchLabels(selector.RequirementNotEqual("missing", "value"), nil) {
		t.Fatal("missing label did not match NotEquals")
	}
	if !selector.RequirementMatchLabels(selector.RequirementNotIn("missing", "value"), nil) {
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

func TestLeafConstructorsMatchBoundaries(t *testing.T) {
	tests := []struct {
		name        string
		requirement selector.Requirement
		want        []bool
	}{
		{"equal", selector.RequirementEqual("rank", 3), []bool{false, true, false, false}},
		{"not equal", selector.RequirementNotEqual("rank", 3), []bool{true, false, true, true}},
		{"greater", selector.RequirementGreaterThan("rank", 3), []bool{false, false, true, false}},
		{"greater or equal", selector.RequirementGreaterThanOrEqual("rank", 3), []bool{false, true, true, false}},
		{"less", selector.RequirementLessThan("rank", 3), []bool{true, false, false, false}},
		{"less or equal", selector.RequirementLessThanOrEqual("rank", 3), []bool{true, true, false, false}},
		{"in", selector.RequirementIn("rank", 2, 4), []bool{true, false, true, false}},
		{"not in", selector.RequirementNotIn("rank", 2, 4), []bool{false, true, false, true}},
		{"empty in", selector.RequirementIn("rank"), []bool{false, false, false, false}},
		{"empty not in", selector.RequirementNotIn("rank"), []bool{false, false, false, false}},
		{"exists", selector.RequirementExists("rank"), []bool{true, true, true, false}},
		{"does not exist", selector.RequirementDoesNotExist("rank"), []bool{false, false, false, true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.requirement.Validate(); err != nil {
				t.Fatal(err)
			}
			for index, value := range []any{2, 3, 4, nil} {
				got := test.requirement.Match(func(key string) (any, bool) {
					return value, key == "rank" && index != 3
				})
				if got != test.want[index] {
					t.Errorf("Match(value=%v, present=%v) = %v, want %v", value, index != 3, got, test.want[index])
				}
			}
		})
	}
}
