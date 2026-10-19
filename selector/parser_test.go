package selector_test

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/store/selector"
)

func TestParseRequirements(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       selector.Requirements
		wantError  bool
	}{
		{name: "empty", expression: "", want: selector.Requirements{}},
		{
			name:       "flat equality and existence",
			expression: "visibility=public,owner,!blocked",
			want: selector.Requirements{
				selector.RequirementEqual("visibility", "public"),
				selector.NewRequirement("owner", selector.Exists),
				selector.NewRequirement("blocked", selector.DoesNotExist),
			},
		},
		{
			name:       "set operators",
			expression: "owner in (alice,bob),state notin (blocked,deleted),tags contains (blue,stable)",
			want: selector.Requirements{
				selector.NewRequirement("owner", selector.In, "alice", "bob"),
				selector.NewRequirement("state", selector.NotIn, "blocked", "deleted"),
				selector.NewRequirement("tags", selector.Contains, "blue", "stable"),
			},
		},
		{
			name:       "comparisons",
			expression: "rank>1,min>=2,max<10,limit<=20,name like api",
			want: selector.Requirements{
				selector.NewRequirement("rank", selector.GreaterThan, "1"),
				selector.NewRequirement("min", selector.GreaterThanOrEqual, "2"),
				selector.NewRequirement("max", selector.LessThan, "10"),
				selector.NewRequirement("limit", selector.LessThanOrEqual, "20"),
				selector.NewRequirement("name", selector.Like, "api"),
			},
		},
		{
			name:       "recursive boolean expression",
			expression: "(visibility=public || (owner=alice && enabled=true)), !(blocked=true)",
			want: selector.Requirements{
				{
					Operator: selector.Or,
					Requirements: selector.Requirements{
						selector.RequirementEqual("visibility", "public"),
						{
							Operator: selector.And,
							Requirements: selector.Requirements{
								selector.RequirementEqual("owner", "alice"),
								selector.RequirementEqual("enabled", "true"),
							},
						},
					},
				},
				{
					Operator: selector.Not,
					Requirements: selector.Requirements{
						selector.RequirementEqual("blocked", "true"),
					},
				},
			},
		},
		{
			name:       "quoted and null values",
			expression: `description="public, but restricted",literal="null",deleted=null`,
			want: selector.Requirements{
				selector.RequirementEqual("description", "public, but restricted"),
				selector.RequirementEqual("literal", "null"),
				selector.RequirementEqual("deleted", nil),
			},
		},
		{
			name:       "escaped special characters",
			expression: `message="a \"quoted\" \\ value",multiline="line1\nline2\t\x00",special="a,b()!<>=&|",empty="",note="alpha　beta"`,
			want: selector.Requirements{
				selector.RequirementEqual("message", `a "quoted" \ value`),
				selector.RequirementEqual("multiline", "line1\nline2\t\x00"),
				selector.RequirementEqual("special", "a,b()!<>=&|"),
				selector.RequirementEqual("empty", ""),
				selector.RequirementEqual("note", "alpha\u3000beta"),
			},
		},
		{
			name:       "special keys",
			expression: `"owner,name"=alice,"owner\"name\\path"=bob,example.com/team=platform,$owner=alice`,
			want: selector.Requirements{
				selector.RequirementEqual("owner,name", "alice"),
				selector.RequirementEqual(`owner"name\path`, "bob"),
				selector.RequirementEqual("example.com/team", "platform"),
				selector.RequirementEqual("$owner", "alice"),
			},
		},
		{
			name:       "URL reserved characters",
			expression: `location="a+b%#?/&"`,
			want: selector.Requirements{
				selector.RequirementEqual("location", "a+b%#?/&"),
			},
		},
		{
			name:       "Unicode whitespace separators",
			expression: "owner\u3000=\u3000alice,\u3000city=杭州",
			want: selector.Requirements{
				selector.RequirementEqual("owner", "alice"),
				selector.RequirementEqual("city", "杭州"),
			},
		},
		{
			name:       "invalid UTF-8 escape",
			expression: `value="\xff"`,
			want: selector.Requirements{
				selector.RequirementEqual("value", string([]byte{0xff})),
			},
		},
		{
			name:       "constants",
			expression: "none(),all(),and(),or()",
			want: selector.Requirements{
				{Operator: selector.None},
				{Operator: selector.All},
				{Operator: selector.And},
				{Operator: selector.Or},
			},
		},
		{name: "missing closing parenthesis", expression: "(visibility=public || owner=alice", wantError: true},
		{name: "single boolean operator", expression: "visibility=public | owner=alice", wantError: true},
		{
			name:       "empty set values",
			expression: "owner in (),owner notin (),owner contains ()",
			want: selector.Requirements{
				selector.NewRequirement("owner", selector.In),
				selector.NewRequirement("owner", selector.NotIn),
				selector.NewRequirement("owner", selector.Contains),
			},
		},
		{name: "constant with value", expression: "all(value)", wantError: true},
		{name: "invalid quoted escape", expression: `value="\q"`, wantError: true},
		{name: "unterminated quoted value", expression: `value="alice`, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := selector.ParseRequirements(test.expression)
			if test.wantError {
				if err == nil {
					t.Fatal("ParseRequirements() returned no error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ParseRequirements() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func FuzzRequirementStringRoundTrip(f *testing.F) {
	f.Add("owner", "alice")
	f.Add("owner,name", `a "quoted" \ value`)
	f.Add("note", "alpha\u3000beta")
	f.Add("message", "line1\nline2\t\x00")
	f.Add("location", "a+b%#?/&")
	f.Add(string([]byte{0xff}), string([]byte{0xfe}))

	f.Fuzz(func(t *testing.T, key, value string) {
		if key == "" {
			return
		}
		want := selector.Requirements{selector.RequirementEqual(key, value)}
		expression := want.String()
		got, err := selector.ParseRequirements(expression)
		if err != nil {
			t.Fatalf("ParseRequirements(%q) error = %v", expression, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("ParseRequirements(%q) = %#v, want %#v", expression, got, want)
		}
	})
}

func FuzzParseRequirementsCanonicalString(f *testing.F) {
	for _, expression := range []string{
		"",
		"owner=alice",
		"owner in (bob,alice)",
		"(visibility=public || owner=alice), !(blocked=true)",
		`message="a \"quoted\" \\ value"`,
		`value="\xff"`,
		"owner in (",
	} {
		f.Add(expression)
	}

	f.Fuzz(func(t *testing.T, expression string) {
		requirements, err := selector.ParseRequirements(expression)
		if err != nil {
			return
		}
		canonical := requirements.String()
		reparsed, err := selector.ParseRequirements(canonical)
		if err != nil {
			t.Fatalf("ParseRequirements(%q) error = %v after parsing %q", canonical, err, expression)
		}
		if got := reparsed.String(); got != canonical {
			t.Fatalf("canonical String() = %q after parsing %q, want %q", got, expression, canonical)
		}
	})
}
