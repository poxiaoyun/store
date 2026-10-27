package store

import (
	"fmt"

	"xiaoshiai.cn/common/errors"
)

// ExprOperator identifies one backend-neutral expression node.
type ExprOperator string

const (
	// ExprLiteral evaluates to Value.
	ExprLiteral ExprOperator = "literal"
	// ExprField evaluates to the stored value at Path.
	ExprField ExprOperator = "field"
	// ExprAdd, ExprSub and ExprMul apply arithmetic to two numeric operands.
	ExprAdd ExprOperator = "add"
	ExprSub ExprOperator = "sub"
	ExprMul ExprOperator = "mul"
	// ExprEq, ExprNe, ExprGt, ExprGte, ExprLt and ExprLte compare two operands.
	ExprEq  ExprOperator = "eq"
	ExprNe  ExprOperator = "ne"
	ExprGt  ExprOperator = "gt"
	ExprGte ExprOperator = "gte"
	ExprLt  ExprOperator = "lt"
	ExprLte ExprOperator = "lte"
	// ExprAnd and ExprOr combine two or more boolean operands.
	ExprAnd ExprOperator = "and"
	ExprOr  ExprOperator = "or"
	// ExprNot negates one boolean operand.
	ExprNot ExprOperator = "not"
	// ExprCond evaluates the first operand and returns the second or third.
	ExprCond ExprOperator = "cond"
	// ExprIfNull returns the first operand unless it is null or missing, in
	// which case it returns the second.
	ExprIfNull ExprOperator = "ifNull"
)

const (
	maxExprDepth = 32
	maxExprNodes = 128
)

// Expr is a backend-neutral expression evaluated against a stored object.
// Literals use Value, field references use Path, and operators use Args.
type Expr struct {
	Op    ExprOperator `json:"op"`
	Path  string       `json:"path,omitempty"`
	Value any          `json:"value,omitempty"`
	Args  []Expr       `json:"args,omitempty"`
}

// Literal returns an expression that always evaluates to value.
func Literal(value any) Expr {
	return Expr{Op: ExprLiteral, Value: value}
}

// Field returns an expression that evaluates to the stored value at path.
func Field(path string) Expr {
	return Expr{Op: ExprField, Path: path}
}

// Add returns an expression that adds a and b.
func Add(a, b Expr) Expr {
	return binaryExpr(ExprAdd, a, b)
}

// Sub returns an expression that subtracts b from a.
func Sub(a, b Expr) Expr {
	return binaryExpr(ExprSub, a, b)
}

// Mul returns an expression that multiplies a and b.
func Mul(a, b Expr) Expr {
	return binaryExpr(ExprMul, a, b)
}

// Eq returns an expression that reports whether a equals b.
func Eq(a, b Expr) Expr {
	return binaryExpr(ExprEq, a, b)
}

// Ne returns an expression that reports whether a differs from b.
func Ne(a, b Expr) Expr {
	return binaryExpr(ExprNe, a, b)
}

// Gt returns an expression that reports whether a is greater than b.
func Gt(a, b Expr) Expr {
	return binaryExpr(ExprGt, a, b)
}

// Gte returns an expression that reports whether a is greater than or equal to b.
func Gte(a, b Expr) Expr {
	return binaryExpr(ExprGte, a, b)
}

// Lt returns an expression that reports whether a is less than b.
func Lt(a, b Expr) Expr {
	return binaryExpr(ExprLt, a, b)
}

// Lte returns an expression that reports whether a is less than or equal to b.
func Lte(a, b Expr) Expr {
	return binaryExpr(ExprLte, a, b)
}

// And returns an expression that reports whether every operand is true.
func And(args ...Expr) Expr {
	return Expr{Op: ExprAnd, Args: args}
}

// Or returns an expression that reports whether any operand is true.
func Or(args ...Expr) Expr {
	return Expr{Op: ExprOr, Args: args}
}

// Not returns an expression that negates arg.
func Not(arg Expr) Expr {
	return Expr{Op: ExprNot, Args: []Expr{arg}}
}

// Cond returns test ? then : otherwise.
func Cond(test, then, otherwise Expr) Expr {
	return Expr{Op: ExprCond, Args: []Expr{test, then, otherwise}}
}

// IfNull returns value unless it is null or missing, in which case it returns
// fallback.
func IfNull(value, fallback Expr) Expr {
	return Expr{Op: ExprIfNull, Args: []Expr{value, fallback}}
}

func binaryExpr(op ExprOperator, a, b Expr) Expr {
	return Expr{Op: op, Args: []Expr{a, b}}
}

// Validate reports whether the expression tree is well formed. Every operator
// has a fixed operand count, the tree may nest at most 32 levels, and it may
// contain at most 128 nodes.
func (e Expr) Validate() error {
	nodes := 0
	return e.validate(1, &nodes)
}

func (e Expr) validate(depth int, nodes *int) error {
	*nodes++
	if depth > maxExprDepth {
		return errors.NewBadRequest("expression is nested too deeply")
	}
	if *nodes > maxExprNodes {
		return errors.NewBadRequest("expression has too many nodes")
	}
	switch e.Op {
	case ExprLiteral:
		if len(e.Args) != 0 {
			return errors.NewBadRequest("literal expression takes no arguments")
		}
		return nil
	case ExprField:
		if e.Path == "" {
			return errors.NewBadRequest("field expression requires a path")
		}
		if !fieldPathPattern.MatchString(e.Path) {
			return errors.NewBadRequest(fmt.Sprintf("field expression has invalid path %q", e.Path))
		}
		if len(e.Args) != 0 {
			return errors.NewBadRequest("field expression takes no arguments")
		}
		return nil
	case ExprAdd, ExprSub, ExprMul, ExprEq, ExprNe, ExprGt, ExprGte, ExprLt, ExprLte:
		if len(e.Args) != 2 {
			return errors.NewBadRequest(fmt.Sprintf("%s expression requires exactly 2 arguments", e.Op))
		}
	case ExprAnd, ExprOr:
		if len(e.Args) < 2 {
			return errors.NewBadRequest(fmt.Sprintf("%s expression requires at least 2 arguments", e.Op))
		}
	case ExprNot:
		if len(e.Args) != 1 {
			return errors.NewBadRequest("not expression requires exactly 1 argument")
		}
	case ExprCond:
		if len(e.Args) != 3 {
			return errors.NewBadRequest("cond expression requires exactly 3 arguments")
		}
	case ExprIfNull:
		if len(e.Args) != 2 {
			return errors.NewBadRequest("ifNull expression requires exactly 2 arguments")
		}
	default:
		return errors.NewBadRequest(fmt.Sprintf("invalid expression op: %s", e.Op))
	}
	for _, arg := range e.Args {
		if err := arg.validate(depth+1, nodes); err != nil {
			return err
		}
	}
	return nil
}
