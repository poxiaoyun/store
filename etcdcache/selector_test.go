package etcdcache

import (
	"testing"

	"k8s.io/apimachinery/pkg/fields"
	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/common/store"
)

func TestConvertPredicateSupportsFieldPresenceOperators(t *testing.T) {
	tests := []struct {
		name     string
		operator selector.Operator
		values   fields.Set
		want     bool
	}{
		{name: "exists with field", operator: selector.Exists, values: fields.Set{"organization": "acme"}, want: true},
		{name: "exists without field", operator: selector.Exists, values: fields.Set{}, want: false},
		{name: "does not exist with field", operator: selector.DoesNotExist, values: fields.Set{"organization": "acme"}, want: false},
		{name: "does not exist without field", operator: selector.DoesNotExist, values: fields.Set{}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			predicate, err := ConvertPredicate(nil, store.Requirements{
				selector.NewRequirement("organization", tt.operator),
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := predicate.Field.Matches(tt.values); got != tt.want {
				t.Fatalf("Field.Matches(%v) = %t, want %t", tt.values, got, tt.want)
			}
		})
	}
}

func TestConvertPredicateSupportsRecursiveRequirements(t *testing.T) {
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
	predicate, err := ConvertPredicate(nil, store.Requirements{requirement})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		fields fields.Set
		want   bool
	}{
		{name: "public", fields: fields.Set{"visibility": "public", "owner": "bob"}, want: true},
		{name: "owned", fields: fields.Set{"visibility": "private", "owner": "alice"}, want: true},
		{name: "blocked", fields: fields.Set{"visibility": "private", "owner": "alice", "state": "blocked"}},
		{name: "denied", fields: fields.Set{"visibility": "private", "owner": "bob"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := predicate.Field.Matches(tt.fields); got != tt.want {
				t.Fatalf("Field.Matches(%v) = %t, want %t", tt.fields, got, tt.want)
			}
		})
	}
}

func TestConvertPredicateRejectsUnsupportedNestedFieldOperator(t *testing.T) {
	requirement := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("name", "exact"),
			selector.NewRequirement("name", selector.Like, "partial"),
		},
	}
	if _, err := ConvertPredicate(nil, store.Requirements{requirement}); !commonerrors.IsUnsupported(err) {
		t.Fatalf("ConvertPredicate() error = %v, want Unsupported", err)
	}
}
