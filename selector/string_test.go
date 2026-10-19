package selector_test

import (
	"testing"
	"time"

	"xiaoshiai.cn/store/selector"
)

func TestRequirementsString(t *testing.T) {
	timestamp := time.Date(2026, time.August, 30, 12, 0, 0, 123, time.UTC)
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name         string
		requirements selector.Requirements
		want         string
	}{
		{name: "empty", requirements: selector.Requirements{}, want: ""},
		{name: "equals", requirements: selector.Requirements{selector.NewRequirement("owner", selector.Equals, "alice")}, want: "owner=alice"},
		{name: "double equals", requirements: selector.Requirements{selector.NewRequirement("owner", selector.DoubleEquals, "alice")}, want: "owner==alice"},
		{name: "not equals", requirements: selector.Requirements{selector.NewRequirement("owner", selector.NotEquals, "alice")}, want: "owner!=alice"},
		{name: "in", requirements: selector.Requirements{selector.NewRequirement("owner", selector.In, "bob", "alice")}, want: "owner in (alice,bob)"},
		{name: "not in", requirements: selector.Requirements{selector.NewRequirement("owner", selector.NotIn, "bob", "alice")}, want: "owner notin (alice,bob)"},
		{name: "exists", requirements: selector.Requirements{selector.NewRequirement("owner", selector.Exists)}, want: "owner"},
		{name: "does not exist", requirements: selector.Requirements{selector.NewRequirement("owner", selector.DoesNotExist)}, want: "!owner"},
		{name: "greater than", requirements: selector.Requirements{selector.NewRequirement("rank", selector.GreaterThan, 1)}, want: "rank>1"},
		{name: "greater than or equal", requirements: selector.Requirements{selector.NewRequirement("rank", selector.GreaterThanOrEqual, 1)}, want: "rank>=1"},
		{name: "less than", requirements: selector.Requirements{selector.NewRequirement("rank", selector.LessThan, 10)}, want: "rank<10"},
		{name: "less than or equal", requirements: selector.Requirements{selector.NewRequirement("rank", selector.LessThanOrEqual, 10)}, want: "rank<=10"},
		{name: "contains", requirements: selector.Requirements{selector.NewRequirement("tags", selector.Contains, "stable", "blue")}, want: "tags contains (blue,stable)"},
		{name: "like", requirements: selector.Requirements{selector.NewRequirement("name", selector.Like, "api")}, want: "name like api"},
		{name: "nil", requirements: selector.Requirements{selector.RequirementEqual("deleted", nil)}, want: "deleted=null"},
		{name: "time", requirements: selector.Requirements{selector.RequirementEqual("created", timestamp)}, want: "created=2026-08-30T12:00:00.000000123Z"},
		{name: "quoted string", requirements: selector.Requirements{selector.RequirementEqual("description", "public, but restricted")}, want: `description="public, but restricted"`},
		{name: "quoted null string", requirements: selector.Requirements{selector.RequirementEqual("literal", "null")}, want: `literal="null"`},
		{name: "empty string", requirements: selector.Requirements{selector.RequirementEqual("value", "")}, want: `value=""`},
		{name: "quotes and backslashes", requirements: selector.Requirements{selector.RequirementEqual("message", `a "quoted" \ value`)}, want: `message="a \"quoted\" \\ value"`},
		{name: "control characters", requirements: selector.Requirements{selector.RequirementEqual("message", "line1\nline2\t\x00")}, want: `message="line1\nline2\t\x00"`},
		{name: "standalone control characters", requirements: selector.Requirements{selector.RequirementEqual("message", "alpha\x00\x7fbeta")}, want: `message="alpha\x00\x7fbeta"`},
		{name: "selector delimiters", requirements: selector.Requirements{selector.RequirementEqual("value", "a,b()!<>=&|")}, want: `value="a,b()!<>=&|"`},
		{name: "special key", requirements: selector.Requirements{selector.RequirementEqual("owner,name", "alice")}, want: `"owner,name"=alice`},
		{name: "escaped key", requirements: selector.Requirements{selector.RequirementEqual(`owner"name\path`, "alice")}, want: `"owner\"name\\path"=alice`},
		{name: "qualified keys", requirements: selector.Requirements{selector.RequirementEqual("example.com/team", "platform"), selector.RequirementEqual("$owner", "alice")}, want: "example.com/team=platform, $owner=alice"},
		{name: "URL reserved characters", requirements: selector.Requirements{selector.RequirementEqual("location", "a+b%#?/&")}, want: `location="a+b%#?/&"`},
		{name: "Unicode whitespace", requirements: selector.Requirements{selector.RequirementEqual("note", "alpha\u3000beta")}, want: `note="alpha\u3000beta"`},
		{name: "Unicode text", requirements: selector.Requirements{selector.RequirementEqual("city", "杭州")}, want: "city=杭州"},
		{name: "invalid UTF-8", requirements: selector.Requirements{selector.RequirementEqual("value", invalidUTF8)}, want: "value=\"\\xff\""},
		{
			name: "top-level and",
			requirements: selector.Requirements{
				selector.RequirementEqual("visibility", "public"),
				selector.RequirementEqual("enabled", true),
			},
			want: "visibility=public, enabled=true",
		},
		{
			name: "recursive boolean expression",
			requirements: selector.Requirements{
				{
					Operator: selector.Or,
					Requirements: selector.Requirements{
						selector.RequirementEqual("visibility", "public"),
						selector.RequirementEqual("owner", "alice"),
					},
				},
				{
					Operator: selector.Not,
					Requirements: selector.Requirements{
						selector.RequirementEqual("blocked", true),
					},
				},
			},
			want: "(visibility=public || owner=alice), !(blocked=true)",
		},
		{name: "none", requirements: selector.Requirements{{Operator: selector.None}}, want: "none()"},
		{name: "all", requirements: selector.Requirements{{Operator: selector.All}}, want: "all()"},
		{name: "empty and", requirements: selector.Requirements{{Operator: selector.And}}, want: "and()"},
		{name: "empty or", requirements: selector.Requirements{{Operator: selector.Or}}, want: "or()"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.requirements.String(); got != test.want {
				t.Fatalf("Requirements.String() = %q, want %q", got, test.want)
			}
		})
	}
}
