package selector_test

import (
	"fmt"
	"math"
	"testing"

	"xiaoshiai.cn/store/selector"
)

func TestRequirementValidate(t *testing.T) {
	valid := selector.Requirement{
		Operator: selector.Or,
		Requirements: selector.Requirements{
			selector.RequirementEqual("visibility", "public"),
			{
				Operator: selector.Not,
				Requirements: selector.Requirements{
					selector.NewRequirement("owner", selector.In, "blocked"),
				},
			},
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if err := (selector.Requirement{}).
		Validate(); err != nil {
		t.Fatalf("zero Requirement Validate() error = %v", err)
	}
	for _, operator := range []selector.Operator{selector.In, selector.NotIn, selector.Contains} {
		t.Run("empty "+string(operator), func(t *testing.T) {
			if err := selector.NewRequirement("owner", operator).
				Validate(); err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}

	invalid := map[string]selector.Requirement{
		"unknown operator": {Operator: selector.Operator("unknown")},
		"constant fields":  {Operator: selector.All, Key: "owner"},
		"and key":          {Operator: selector.And, Key: "owner"},
		"not child count":  {Operator: selector.Not},
		"exists values":    {Operator: selector.Exists, Key: "owner", Values: []any{"alice"}},
		"comparison value": {Operator: selector.Equals, Key: "owner"},
		"unsupported value": selector.RequirementEqual(
			"owner",
			map[string]string{"name": "alice"},
		),
		"non-finite value": selector.RequirementEqual("score", math.NaN()),
		"leaf children": {
			Operator:     selector.Equals,
			Key:          "owner",
			Values:       []any{"alice"},
			Requirements: selector.Requirements{{Operator: selector.All}},
		},
		"nested invalid": {
			Operator: selector.Or,
			Requirements: selector.Requirements{
				{Operator: selector.Equals, Key: "owner"},
			},
		},
	}
	for name, requirement := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := requirement.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestRequirementsFromMapStableOrder(t *testing.T) {
	fields := map[string]string{}
	for index := 15; index >= 0; index-- {
		fields[fmt.Sprintf("field%02d", index)] = fmt.Sprintf("value%d", index)
	}
	requirements := selector.RequirementsFromMap(fields)
	if len(requirements) != len(fields) {
		t.Fatalf("lost filters: %v", requirements)
	}
	for index, requirement := range requirements {
		key := fmt.Sprintf("field%02d", index)
		if requirement.Key != key || requirement.Operator != selector.Equals || len(requirement.Values) != 1 || requirement.Values[0] != fields[key] {
			t.Fatalf("condition %d: %+v", index, requirement)
		}
	}
}
