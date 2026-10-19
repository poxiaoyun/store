package selector

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type requirementTokenKind uint8

const (
	requirementTokenEOF requirementTokenKind = iota
	requirementTokenValue
	requirementTokenComma
	requirementTokenLeftParenthesis
	requirementTokenRightParenthesis
	requirementTokenNot
	requirementTokenEquals
	requirementTokenDoubleEquals
	requirementTokenNotEquals
	requirementTokenGreaterThan
	requirementTokenGreaterThanOrEqual
	requirementTokenLessThan
	requirementTokenLessThanOrEqual
	requirementTokenAnd
	requirementTokenOr
)

type requirementToken struct {
	kind     requirementTokenKind
	value    string
	quoted   bool
	position int
}

type requirementLexer struct {
	expression string
	position   int
}

func (l *requirementLexer) next() (requirementToken, error) {
	for l.position < len(l.expression) {
		character, size := utf8.DecodeRuneInString(l.expression[l.position:])
		if !unicode.IsSpace(character) {
			break
		}
		l.position += size
	}
	if l.position == len(l.expression) {
		return requirementToken{kind: requirementTokenEOF, position: l.position}, nil
	}

	position := l.position
	character := l.expression[l.position]
	l.position++
	switch character {
	case ',':
		return requirementToken{kind: requirementTokenComma, position: position}, nil
	case '(':
		return requirementToken{kind: requirementTokenLeftParenthesis, position: position}, nil
	case ')':
		return requirementToken{kind: requirementTokenRightParenthesis, position: position}, nil
	case '!':
		if l.consume('=') {
			return requirementToken{kind: requirementTokenNotEquals, position: position}, nil
		}
		return requirementToken{kind: requirementTokenNot, position: position}, nil
	case '=':
		if l.consume('=') {
			return requirementToken{kind: requirementTokenDoubleEquals, position: position}, nil
		}
		return requirementToken{kind: requirementTokenEquals, position: position}, nil
	case '>':
		if l.consume('=') {
			return requirementToken{kind: requirementTokenGreaterThanOrEqual, position: position}, nil
		}
		return requirementToken{kind: requirementTokenGreaterThan, position: position}, nil
	case '<':
		if l.consume('=') {
			return requirementToken{kind: requirementTokenLessThanOrEqual, position: position}, nil
		}
		return requirementToken{kind: requirementTokenLessThan, position: position}, nil
	case '&':
		if !l.consume('&') {
			return requirementToken{}, fmt.Errorf("parse requirements at %d: expected &&", position)
		}
		return requirementToken{kind: requirementTokenAnd, position: position}, nil
	case '|':
		if !l.consume('|') {
			return requirementToken{}, fmt.Errorf("parse requirements at %d: expected ||", position)
		}
		return requirementToken{kind: requirementTokenOr, position: position}, nil
	case '"':
		for escaped := false; l.position < len(l.expression); l.position++ {
			current := l.expression[l.position]
			if current == '"' && !escaped {
				l.position++
				value, err := strconv.Unquote(l.expression[position:l.position])
				if err != nil {
					return requirementToken{}, fmt.Errorf("parse requirements at %d: %w", position, err)
				}
				return requirementToken{kind: requirementTokenValue, value: value, quoted: true, position: position}, nil
			}
			if current == '\\' {
				escaped = !escaped
			} else {
				escaped = false
			}
		}
		return requirementToken{}, fmt.Errorf("parse requirements at %d: unterminated quoted value", position)
	}

	l.position = position
	for l.position < len(l.expression) {
		character, size := utf8.DecodeRuneInString(l.expression[l.position:])
		if isRequirementDelimiter(character) {
			break
		}
		l.position += size
	}
	return requirementToken{
		kind:     requirementTokenValue,
		value:    l.expression[position:l.position],
		position: position,
	}, nil
}

func (l *requirementLexer) consume(character byte) bool {
	if l.position == len(l.expression) || l.expression[l.position] != character {
		return false
	}
	l.position++
	return true
}

func isRequirementDelimiter(character rune) bool {
	return unicode.IsSpace(character) || strings.ContainsRune(",()!<>=&|\"", character)
}
