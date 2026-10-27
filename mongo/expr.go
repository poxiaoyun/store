package mongo

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// exprOperators maps expression operators to their aggregation operator names.
var exprOperators = map[store.ExprOperator]string{
	store.ExprAdd: "$add",
	store.ExprSub: "$subtract",
	store.ExprMul: "$multiply",
	store.ExprEq:  "$eq",
	store.ExprNe:  "$ne",
	store.ExprGt:  "$gt",
	store.ExprGte: "$gte",
	store.ExprLt:  "$lt",
	store.ExprLte: "$lte",
	store.ExprAnd: "$and",
	store.ExprOr:  "$or",
}

// buildExpr compiles a store expression into an aggregation expression that is
// evaluated against the stored document.
func buildExpr(expr store.Expr) (any, error) {
	if err := expr.Validate(); err != nil {
		return nil, err
	}
	return buildExprNode(expr)
}

func buildExprNode(expr store.Expr) (any, error) {
	switch expr.Op {
	case store.ExprLiteral:
		return bson.D{{Key: "$literal", Value: normalizeFieldPatchValue(expr.Value)}}, nil
	case store.ExprField:
		return "$" + expr.Path, nil
	case store.ExprAdd, store.ExprSub, store.ExprMul,
		store.ExprEq, store.ExprNe, store.ExprGt, store.ExprGte, store.ExprLt, store.ExprLte,
		store.ExprAnd, store.ExprOr:
		args, err := buildExprArgs(expr.Args)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: exprOperators[expr.Op], Value: args}}, nil
	case store.ExprNot:
		arg, err := buildExprNode(expr.Args[0])
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: "$not", Value: bson.A{arg}}}, nil
	case store.ExprCond:
		args, err := buildExprArgs(expr.Args)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: "$cond", Value: args}}, nil
	case store.ExprIfNull:
		args, err := buildExprArgs(expr.Args)
		if err != nil {
			return nil, err
		}
		return bson.D{{Key: "$ifNull", Value: args}}, nil
	default:
		return nil, errors.NewBadRequest(fmt.Sprintf("invalid expression op: %s", expr.Op))
	}
}

func buildExprArgs(exprs []store.Expr) (bson.A, error) {
	args := make(bson.A, 0, len(exprs))
	for _, expr := range exprs {
		arg, err := buildExprNode(expr)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}
