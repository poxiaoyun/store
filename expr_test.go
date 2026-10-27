package store

import "testing"

func TestExprValidate(t *testing.T) {
	tests := []struct {
		name string
		expr Expr
	}{
		{"literal", Literal(int64(1))},
		{"field", Field("balance")},
		{"nested field", Field("status.balance")},
		{"add", Add(Field("balance"), Literal(int64(1)))},
		{"sub", Sub(Field("balance"), Literal(int64(1)))},
		{"mul", Mul(Field("balance"), Literal(int64(2)))},
		{"eq", Eq(Field("normalSide"), Literal("debit"))},
		{"ne", Ne(Field("normalSide"), Literal("debit"))},
		{"gt", Gt(Field("balance"), Literal(int64(0)))},
		{"gte", Gte(Field("balance"), Literal(int64(0)))},
		{"lt", Lt(Field("balance"), Literal(int64(0)))},
		{"lte", Lte(Field("balance"), Literal(int64(0)))},
		{"and", And(Eq(Field("a"), Literal("x")), Ne(Field("b"), Literal("y")))},
		{"or", Or(Eq(Field("a"), Literal("x")), Ne(Field("b"), Literal("y")))},
		{"not", Not(Field("enabled"))},
		{"cond", Cond(
			Eq(Field("normalSide"), Literal("debit")),
			Add(Field("available"), Literal(int64(10))),
			Sub(Field("available"), Literal(int64(10))),
		)},
		{"ifNull", IfNull(Field("balance"), Literal(int64(0)))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.expr.Validate(); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}

func TestExprValidateInvalid(t *testing.T) {
	tests := []struct {
		name string
		expr Expr
	}{
		{"literal with args", Expr{Op: ExprLiteral, Args: []Expr{Literal(int64(1))}}},
		{"field without path", Expr{Op: ExprField}},
		{"field with args", Expr{Op: ExprField, Path: "a", Args: []Expr{Literal(int64(1))}}},
		{"field with variable path", Expr{Op: ExprField, Path: "$ROOT"}},
		{"field with empty segment", Expr{Op: ExprField, Path: "a..b"}},
		{"field with trailing dot", Expr{Op: ExprField, Path: "a."}},
		{"add without args", exprWithArgs(ExprAdd, 0)},
		{"add with one arg", exprWithArgs(ExprAdd, 1)},
		{"add with three args", exprWithArgs(ExprAdd, 3)},
		{"sub with one arg", exprWithArgs(ExprSub, 1)},
		{"mul with three args", exprWithArgs(ExprMul, 3)},
		{"eq with one arg", exprWithArgs(ExprEq, 1)},
		{"ne with three args", exprWithArgs(ExprNe, 3)},
		{"gt with one arg", exprWithArgs(ExprGt, 1)},
		{"gte with three args", exprWithArgs(ExprGte, 3)},
		{"lt with one arg", exprWithArgs(ExprLt, 1)},
		{"lte with three args", exprWithArgs(ExprLte, 3)},
		{"and with one arg", exprWithArgs(ExprAnd, 1)},
		{"or with one arg", exprWithArgs(ExprOr, 1)},
		{"not without args", exprWithArgs(ExprNot, 0)},
		{"not with two args", exprWithArgs(ExprNot, 2)},
		{"cond with two args", exprWithArgs(ExprCond, 2)},
		{"cond with four args", exprWithArgs(ExprCond, 4)},
		{"ifNull with one arg", exprWithArgs(ExprIfNull, 1)},
		{"ifNull with three args", exprWithArgs(ExprIfNull, 3)},
		{"unknown op", Expr{Op: "bogus"}},
		{"invalid nested arg", Not(Expr{Op: ExprField})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.expr.Validate(); err == nil {
				t.Fatal("Validate() accepted an invalid expression")
			}
		})
	}
}

func TestExprValidateLimits(t *testing.T) {
	deep := Field("a")
	for range maxExprDepth - 1 {
		deep = Not(deep)
	}
	if err := deep.Validate(); err != nil {
		t.Fatalf("expression at max depth: %v", err)
	}
	if err := Not(deep).Validate(); err == nil {
		t.Fatal("expression deeper than the limit was accepted")
	}

	args := make([]Expr, maxExprNodes-1)
	for index := range args {
		args[index] = Literal(index)
	}
	wide := Expr{Op: ExprAnd, Args: args}
	if err := wide.Validate(); err != nil {
		t.Fatalf("expression at max nodes: %v", err)
	}
	wide.Args = append(wide.Args, Literal(0))
	if err := wide.Validate(); err == nil {
		t.Fatal("expression with too many nodes was accepted")
	}
}

func exprWithArgs(op ExprOperator, count int) Expr {
	args := make([]Expr, count)
	for index := range args {
		args[index] = Literal(index)
	}
	return Expr{Op: op, Args: args}
}
