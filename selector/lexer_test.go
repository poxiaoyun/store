package selector

import (
	"reflect"
	"testing"
)

func TestRequirementLexer(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       []requirementToken
	}{
		{
			name:       "operators and grouping",
			expression: `(rank>=1 && rank<10) || !blocked`,
			want: []requirementToken{
				{kind: requirementTokenLeftParenthesis, position: 0},
				{kind: requirementTokenValue, value: "rank", position: 1},
				{kind: requirementTokenGreaterThanOrEqual, position: 5},
				{kind: requirementTokenValue, value: "1", position: 7},
				{kind: requirementTokenAnd, position: 9},
				{kind: requirementTokenValue, value: "rank", position: 12},
				{kind: requirementTokenLessThan, position: 16},
				{kind: requirementTokenValue, value: "10", position: 17},
				{kind: requirementTokenRightParenthesis, position: 19},
				{kind: requirementTokenOr, position: 21},
				{kind: requirementTokenNot, position: 24},
				{kind: requirementTokenValue, value: "blocked", position: 25},
				{kind: requirementTokenEOF, position: 32},
			},
		},
		{
			name:       "quoted value",
			expression: `message="a \"quoted\" value"`,
			want: []requirementToken{
				{kind: requirementTokenValue, value: "message", position: 0},
				{kind: requirementTokenEquals, position: 7},
				{kind: requirementTokenValue, value: `a "quoted" value`, quoted: true, position: 8},
				{kind: requirementTokenEOF, position: 28},
			},
		},
		{
			name:       "unicode whitespace",
			expression: "city\u3000=\u3000杭州",
			want: []requirementToken{
				{kind: requirementTokenValue, value: "city", position: 0},
				{kind: requirementTokenEquals, position: 7},
				{kind: requirementTokenValue, value: "杭州", position: 11},
				{kind: requirementTokenEOF, position: 17},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := requirementLexer{expression: test.expression}
			var got []requirementToken
			for {
				token, err := lexer.next()
				if err != nil {
					t.Fatalf("next() error = %v", err)
				}
				got = append(got, token)
				if token.kind == requirementTokenEOF {
					break
				}
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("tokens = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestRequirementLexerRejectsIncompleteOperatorsAndQuotes(t *testing.T) {
	for _, expression := range []string{"a & b", "a | b", `a="unterminated`} {
		t.Run(expression, func(t *testing.T) {
			lexer := requirementLexer{expression: expression}
			for {
				token, err := lexer.next()
				if err != nil {
					return
				}
				if token.kind == requirementTokenEOF {
					t.Fatal("next() reached EOF without an error")
				}
			}
		})
	}
}

func FuzzRequirementLexer(f *testing.F) {
	for _, expression := range []string{
		"",
		"owner=alice",
		`message="a \"quoted\" value"`,
		"(visibility=public || owner=alice), !blocked",
		"city\u3000=\u3000杭州",
		"owner in ()",
		"a & b",
	} {
		f.Add(expression)
	}

	f.Fuzz(func(t *testing.T, expression string) {
		lexer := requirementLexer{expression: expression}
		for step := 0; step <= len(expression); step++ {
			before := lexer.position
			token, err := lexer.next()
			if err != nil {
				return
			}
			if token.position < before || token.position > len(expression) {
				t.Fatalf("token position = %d after lexer position %d", token.position, before)
			}
			if token.kind == requirementTokenEOF {
				return
			}
			if lexer.position <= before || lexer.position > len(expression) {
				t.Fatalf("lexer did not advance from %d: position = %d, token = %#v", before, lexer.position, token)
			}
		}
		t.Fatal("lexer did not reach EOF")
	})
}
