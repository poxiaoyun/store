package selector

import "fmt"

// ParseRequirements parses a recursive selector expression.
func ParseRequirements(expr string) (Requirements, error) {
	parser := requirementParser{lexer: requirementLexer{expression: expr}}
	requirements, err := parser.parseRequirements()
	if err != nil {
		return nil, err
	}
	if err := requirements.Validate(); err != nil {
		return nil, err
	}
	return requirements, nil
}

type requirementParser struct {
	lexer       requirementLexer
	lookahead   requirementToken
	lookedAhead bool
}

func (p *requirementParser) parseRequirements() (Requirements, error) {
	token, err := p.peek()
	if err != nil {
		return nil, err
	}
	if token.kind == requirementTokenEOF {
		return Requirements{}, nil
	}

	var requirements Requirements
	for {
		requirement, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		requirements = append(requirements, requirement)
		token, err := p.peek()
		if err != nil {
			return nil, err
		}
		if token.kind == requirementTokenEOF {
			return requirements, nil
		}
		if token.kind != requirementTokenComma {
			return nil, p.unexpected(token, "comma or end of expression")
		}
		p.take()
		token, err = p.peek()
		if err != nil {
			return nil, err
		}
		if token.kind == requirementTokenEOF {
			return nil, p.unexpected(token, "requirement after comma")
		}
	}
}

func (p *requirementParser) parseOr() (Requirement, error) {
	left, err := p.parseAnd()
	if err != nil {
		return Requirement{}, err
	}
	for {
		token, err := p.peek()
		if err != nil {
			return Requirement{}, err
		}
		if token.kind != requirementTokenOr {
			return left, nil
		}
		p.take()
		right, err := p.parseAnd()
		if err != nil {
			return Requirement{}, err
		}
		left = combineRequirements(Or, left, right)
	}
}

func (p *requirementParser) parseAnd() (Requirement, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return Requirement{}, err
	}
	for {
		token, err := p.peek()
		if err != nil {
			return Requirement{}, err
		}
		if token.kind != requirementTokenAnd {
			return left, nil
		}
		p.take()
		right, err := p.parsePrimary()
		if err != nil {
			return Requirement{}, err
		}
		left = combineRequirements(And, left, right)
	}
}

func combineRequirements(operator Operator, left, right Requirement) Requirement {
	children := make(Requirements, 0, 2)
	if left.Operator == operator {
		children = append(children, left.Requirements...)
	} else {
		children = append(children, left)
	}
	if right.Operator == operator {
		children = append(children, right.Requirements...)
	} else {
		children = append(children, right)
	}
	return Requirement{Operator: operator, Requirements: children}
}

func (p *requirementParser) parsePrimary() (Requirement, error) {
	token, err := p.peek()
	if err != nil {
		return Requirement{}, err
	}
	switch token.kind {
	case requirementTokenNot:
		p.take()
		next, err := p.peek()
		if err != nil {
			return Requirement{}, err
		}
		if next.kind != requirementTokenLeftParenthesis {
			key, err := p.parseKey()
			if err != nil {
				return Requirement{}, err
			}
			return Requirement{Operator: DoesNotExist, Key: key}, nil
		}
		p.take()
		child, err := p.parseOr()
		if err != nil {
			return Requirement{}, err
		}
		if err := p.expect(requirementTokenRightParenthesis, "closing parenthesis"); err != nil {
			return Requirement{}, err
		}
		return Requirement{Operator: Not, Requirements: Requirements{child}}, nil
	case requirementTokenLeftParenthesis:
		p.take()
		requirement, err := p.parseOr()
		if err != nil {
			return Requirement{}, err
		}
		if err := p.expect(requirementTokenRightParenthesis, "closing parenthesis"); err != nil {
			return Requirement{}, err
		}
		return requirement, nil
	case requirementTokenValue:
		p.take()
		return p.parseLeafOrConstant(token)
	default:
		return Requirement{}, p.unexpected(token, "requirement")
	}
}

func (p *requirementParser) parseLeafOrConstant(key requirementToken) (Requirement, error) {
	next, err := p.peek()
	if err != nil {
		return Requirement{}, err
	}
	if !key.quoted && next.kind == requirementTokenLeftParenthesis {
		operator, constant := map[string]Operator{"all": All, "none": None, "and": And, "or": Or}[key.value]
		if !constant {
			return Requirement{}, p.unexpected(next, "leaf operator")
		}
		p.take()
		if err := p.expect(requirementTokenRightParenthesis, "closing parenthesis"); err != nil {
			return Requirement{}, err
		}
		return Requirement{Operator: operator}, nil
	}
	if isRequirementBoundary(next.kind) {
		return Requirement{Operator: Exists, Key: key.value}, nil
	}

	operator, err := p.parseLeafOperator()
	if err != nil {
		return Requirement{}, err
	}
	requirement := Requirement{Operator: operator, Key: key.value}
	switch operator {
	case In, NotIn:
		requirement.Values, err = p.parseValueList()
	case Contains:
		next, peekErr := p.peek()
		if peekErr != nil {
			return Requirement{}, peekErr
		}
		if next.kind == requirementTokenLeftParenthesis {
			requirement.Values, err = p.parseValueList()
		} else {
			requirement.Values, err = p.parseSingleValue()
		}
	default:
		requirement.Values, err = p.parseSingleValue()
	}
	return requirement, err
}

func (p *requirementParser) parseLeafOperator() (Operator, error) {
	token, err := p.next()
	if err != nil {
		return None, err
	}
	switch token.kind {
	case requirementTokenEquals:
		return Equals, nil
	case requirementTokenDoubleEquals:
		return DoubleEquals, nil
	case requirementTokenNotEquals:
		return NotEquals, nil
	case requirementTokenGreaterThan:
		return GreaterThan, nil
	case requirementTokenGreaterThanOrEqual:
		return GreaterThanOrEqual, nil
	case requirementTokenLessThan:
		return LessThan, nil
	case requirementTokenLessThanOrEqual:
		return LessThanOrEqual, nil
	case requirementTokenValue:
		switch token.value {
		case "in":
			return In, nil
		case "notin":
			return NotIn, nil
		case "contains":
			return Contains, nil
		case "like":
			return Like, nil
		}
	}
	return None, p.unexpected(token, "leaf operator")
}

func (p *requirementParser) parseSingleValue() ([]any, error) {
	token, err := p.peek()
	if err != nil {
		return nil, err
	}
	if isRequirementBoundary(token.kind) {
		return []any{""}, nil
	}
	value, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return []any{value}, nil
}

func (p *requirementParser) parseValueList() ([]any, error) {
	if err := p.expect(requirementTokenLeftParenthesis, "opening parenthesis"); err != nil {
		return nil, err
	}
	token, err := p.peek()
	if err != nil {
		return nil, err
	}
	if token.kind == requirementTokenRightParenthesis {
		p.take()
		return nil, nil
	}

	var values []any
	for {
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		token, err := p.next()
		if err != nil {
			return nil, err
		}
		switch token.kind {
		case requirementTokenComma:
			continue
		case requirementTokenRightParenthesis:
			return values, nil
		default:
			return nil, p.unexpected(token, "comma or closing parenthesis")
		}
	}
}

func (p *requirementParser) parseValue() (any, error) {
	token, err := p.next()
	if err != nil {
		return nil, err
	}
	if token.kind != requirementTokenValue {
		return nil, p.unexpected(token, "value")
	}
	if token.quoted {
		return token.value, nil
	}
	if token.value == "null" {
		return nil, nil
	}
	return token.value, nil
}

func (p *requirementParser) parseKey() (string, error) {
	token, err := p.next()
	if err != nil {
		return "", err
	}
	if token.kind != requirementTokenValue {
		return "", p.unexpected(token, "key")
	}
	return token.value, nil
}

func (p *requirementParser) expect(kind requirementTokenKind, expected string) error {
	token, err := p.next()
	if err != nil {
		return err
	}
	if token.kind != kind {
		return p.unexpected(token, expected)
	}
	return nil
}

func (p *requirementParser) peek() (requirementToken, error) {
	if p.lookedAhead {
		return p.lookahead, nil
	}
	token, err := p.lexer.next()
	if err != nil {
		return requirementToken{}, err
	}
	p.lookahead = token
	p.lookedAhead = true
	return token, nil
}

func (p *requirementParser) next() (requirementToken, error) {
	token, err := p.peek()
	if err != nil {
		return requirementToken{}, err
	}
	p.lookedAhead = false
	return token, nil
}

func (p *requirementParser) take() {
	p.lookedAhead = false
}

func (p *requirementParser) unexpected(token requirementToken, expected string) error {
	return fmt.Errorf("parse requirements at %d: expected %s", token.position, expected)
}

func isRequirementBoundary(kind requirementTokenKind) bool {
	switch kind {
	case requirementTokenEOF, requirementTokenComma, requirementTokenRightParenthesis,
		requirementTokenAnd, requirementTokenOr:
		return true
	default:
		return false
	}
}
