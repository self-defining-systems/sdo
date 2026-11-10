package datacheck

import (
	"fmt"
	"strings"
	"unicode"
)

// validateWhere accepts a postgres condition in a closed grammar: comparisons
// of columns and literals joined by AND, OR, NOT and parentheses, IS [NOT]
// NULL, [NOT] IN (literals), [NOT] LIKE 'text' and BETWEEN. There are no
// function calls, casts, subqueries, comments, semicolons, dollar quotes or
// backslashes, so a condition can only filter rows. The read also runs in a
// read-only transaction with a statement timeout.
func validateWhere(text string) error {
	if strings.ContainsAny(text, ";\\$\"") || strings.Contains(text, "--") || strings.Contains(text, "/*") {
		return fmt.Errorf("semicolons, comments, backslashes, dollar and double quotes are not allowed")
	}
	tokens, err := lexWhere(text)
	if err != nil {
		return err
	}
	p := &whereParser{tokens: tokens}
	if err := p.expr(0); err != nil {
		return err
	}
	if p.pos != len(p.tokens) {
		return fmt.Errorf("unexpected %q", p.tokens[p.pos].text)
	}
	return nil
}

type tokenKind int

const (
	tokIdent tokenKind = iota
	tokKeyword
	tokNumber
	tokString
	tokOperator
	tokPunct
)

type token struct {
	kind tokenKind
	text string
}

var whereKeywords = map[string]bool{"AND": true, "OR": true, "NOT": true, "IS": true, "NULL": true, "TRUE": true, "FALSE": true, "IN": true, "LIKE": true, "ILIKE": true, "BETWEEN": true}

func lexWhere(text string) ([]token, error) {
	var out []token
	runes := []rune(text)
	for i := 0; i < len(runes); {
		r := runes[i]
		switch {
		case unicode.IsSpace(r):
			i++
		case r == '\'':
			j := i + 1
			for ; j < len(runes); j++ {
				if runes[j] == '\'' {
					if j+1 < len(runes) && runes[j+1] == '\'' {
						j++
						continue
					}
					break
				}
			}
			if j >= len(runes) {
				return nil, fmt.Errorf("unterminated string")
			}
			out = append(out, token{tokString, string(runes[i : j+1])})
			i = j + 1
		case unicode.IsDigit(r) || r == '-' && i+1 < len(runes) && unicode.IsDigit(runes[i+1]):
			j := i + 1
			for j < len(runes) && (unicode.IsDigit(runes[j]) || runes[j] == '.') {
				j++
			}
			out = append(out, token{tokNumber, string(runes[i:j])})
			i = j
		case r == '_' || r < unicode.MaxASCII && unicode.IsLetter(r):
			j := i + 1
			for j < len(runes) && (runes[j] == '_' || runes[j] < unicode.MaxASCII && (unicode.IsLetter(runes[j]) || unicode.IsDigit(runes[j]))) {
				j++
			}
			word := string(runes[i:j])
			if whereKeywords[strings.ToUpper(word)] {
				out = append(out, token{tokKeyword, strings.ToUpper(word)})
			} else {
				if j < len(runes) && (runes[j] == '(' || runes[j] == '.' || runes[j] == ':') {
					return nil, fmt.Errorf("%s: function calls, qualified names and casts are not allowed", word)
				}
				out = append(out, token{tokIdent, word})
			}
			i = j
		case strings.ContainsRune("=<>!", r):
			j := i + 1
			for j < len(runes) && strings.ContainsRune("=<>", runes[j]) {
				j++
			}
			op := string(runes[i:j])
			switch op {
			case "=", "<>", "!=", "<", "<=", ">", ">=":
			default:
				return nil, fmt.Errorf("operator %q is not allowed", op)
			}
			out = append(out, token{tokOperator, op})
			i = j
		case r == '(' || r == ')' || r == ',':
			out = append(out, token{tokPunct, string(r)})
			i++
		default:
			return nil, fmt.Errorf("character %q is not allowed", r)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty condition")
	}
	if len(out) > 256 {
		return nil, fmt.Errorf("condition is longer than 256 tokens")
	}
	return out, nil
}

type whereParser struct {
	tokens []token
	pos    int
}

func (p *whereParser) peek() (token, bool) {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos], true
	}
	return token{}, false
}

func (p *whereParser) accept(kind tokenKind, text string) bool {
	if t, ok := p.peek(); ok && t.kind == kind && t.text == text {
		p.pos++
		return true
	}
	return false
}

func (p *whereParser) expect(kind tokenKind, text string) error {
	if !p.accept(kind, text) {
		return p.unexpected("expected " + text)
	}
	return nil
}

func (p *whereParser) unexpected(what string) error {
	if t, ok := p.peek(); ok {
		return fmt.Errorf("%s, got %q", what, t.text)
	}
	return fmt.Errorf("%s at the end", what)
}

// expr := term (OR term)*; term := factor (AND factor)*.
func (p *whereParser) expr(depth int) error {
	if depth > 16 {
		return fmt.Errorf("nested deeper than 16")
	}
	for {
		for {
			if err := p.factor(depth); err != nil {
				return err
			}
			if !p.accept(tokKeyword, "AND") {
				break
			}
		}
		if !p.accept(tokKeyword, "OR") {
			return nil
		}
	}
}

// factor := NOT factor | '(' expr ')' | predicate.
func (p *whereParser) factor(depth int) error {
	if p.accept(tokKeyword, "NOT") {
		return p.factor(depth + 1)
	}
	if p.accept(tokPunct, "(") {
		if err := p.expr(depth + 1); err != nil {
			return err
		}
		return p.expect(tokPunct, ")")
	}
	return p.predicate()
}

// predicate := operand (op operand | IS [NOT] NULL | [NOT] IN (literals) |
// [NOT] LIKE string | [NOT] BETWEEN operand AND operand).
func (p *whereParser) predicate() error {
	if err := p.operand(); err != nil {
		return err
	}
	t, ok := p.peek()
	if !ok {
		return p.unexpected("expected a comparison")
	}
	if t.kind == tokOperator {
		p.pos++
		return p.operand()
	}
	if p.accept(tokKeyword, "IS") {
		p.accept(tokKeyword, "NOT")
		if p.accept(tokKeyword, "NULL") || p.accept(tokKeyword, "TRUE") || p.accept(tokKeyword, "FALSE") {
			return nil
		}
		return p.unexpected("expected NULL, TRUE or FALSE")
	}
	p.accept(tokKeyword, "NOT")
	switch {
	case p.accept(tokKeyword, "IN"):
		if err := p.expect(tokPunct, "("); err != nil {
			return err
		}
		for {
			if err := p.literal(); err != nil {
				return err
			}
			if !p.accept(tokPunct, ",") {
				break
			}
		}
		return p.expect(tokPunct, ")")
	case p.accept(tokKeyword, "LIKE"), p.accept(tokKeyword, "ILIKE"):
		if t, ok := p.peek(); ok && t.kind == tokString {
			p.pos++
			return nil
		}
		return p.unexpected("expected a quoted pattern")
	case p.accept(tokKeyword, "BETWEEN"):
		if err := p.operand(); err != nil {
			return err
		}
		if err := p.expect(tokKeyword, "AND"); err != nil {
			return err
		}
		return p.operand()
	}
	return p.unexpected("expected a comparison")
}

func (p *whereParser) operand() error {
	if t, ok := p.peek(); ok && t.kind == tokIdent {
		p.pos++
		return nil
	}
	return p.literal()
}

func (p *whereParser) literal() error {
	t, ok := p.peek()
	if ok && (t.kind == tokNumber || t.kind == tokString || t.kind == tokKeyword && (t.text == "NULL" || t.text == "TRUE" || t.text == "FALSE")) {
		p.pos++
		return nil
	}
	return p.unexpected("expected a column or a literal")
}
