package storeops

import (
	"fmt"
	"strings"
)

// sqlDialect is how one server splits SQL text into quoted and unquoted parts.
type sqlDialect struct {
	// backslash: a plain string literal reads backslash escapes (mysql). In
	// postgres only an E'' literal does.
	backslash bool
	// backtick: `...` quotes an identifier, and # starts a comment (mysql).
	backtick bool
	// dollar: $tag$...$tag$ quotes a string, E'' reads escapes, and a literal
	// goes on after a line break (postgres).
	dollar bool
}

var (
	postgresDialect = sqlDialect{dollar: true}
	mysqlDialect    = sqlDialect{backslash: true, backtick: true}
)

// sqlSpan is the half-open range of one quoted part, its quotes included.
type sqlSpan struct{ start, end int }

// scannedSQL is a SQL fragment that stays in the clause it fills.
type scannedSQL struct {
	quoted []sqlSpan
	// top is the text with everything inside quotes or parentheses blanked:
	// the clause's own keywords.
	top string
}

// scanSQL reads text as the server does and refuses what could leave the
// clause it fills: outside quotes a statement separator, a comment, a
// backslash (a client command) or a parenthesis that closes one of SDO's; and
// a quote that never ends. Inside a literal or a quoted identifier any
// character but NUL is the value's own.
func scanSQL(what, text string, d sqlDialect) (scannedSQL, error) {
	var scanned scannedSQL
	if strings.IndexByte(text, 0) >= 0 {
		return scanned, fmt.Errorf("%s may not contain NUL", what)
	}
	top := []byte(text)
	depth := 0
	quote := func(start, end int) {
		scanned.quoted = append(scanned.quoted, sqlSpan{start, end})
		blank(top[start:end])
	}
	for i := 0; i < len(text); {
		c := text[i]
		end, quoted := i+1, false
		switch {
		case c == '\'':
			end, quoted = stringEnd(text, i, d.backslash, d.dollar), true
		case c == '"':
			end, quoted = stringEnd(text, i, false, false), true
			// mysql reads "..." as a string with escapes, or under ANSI_QUOTES
			// as an identifier without: a backslash would end it differently.
			if d.backslash && end > 0 && strings.IndexByte(text[i:end], '\\') >= 0 {
				return scanned, fmt.Errorf("%s may not hold a backslash in double quotes; use a single-quoted literal", what)
			}
		case c == '`' && d.backtick:
			end, quoted = stringEnd(text, i, false, false), true
			if end > 0 && strings.IndexByte(text[i:end], '\\') >= 0 {
				return scanned, fmt.Errorf("%s may not hold a backslash in a quoted identifier", what)
			}
		case c == '$' && d.dollar:
			if tag := dollarTag(text[i:]); tag != "" {
				quoted = true
				if n := strings.Index(text[i+len(tag):], tag); n >= 0 {
					end = i + n + 2*len(tag)
				} else {
					end = -1
				}
			}
		case digit(c):
			for end < len(text) && (identifierByte(text[end]) || text[end] == '.') {
				end++
			}
			// Server versions differ in where such a number ends, so in what
			// the quote after it is.
			if end < len(text) && (text[end] == '$' || text[end] == '\'') {
				return scanned, fmt.Errorf("%s needs a space between %q and the quote after it", what, text[i:end])
			}
		case identifierByte(c):
			for end < len(text) && (identifierByte(text[end]) || text[end] == '$') {
				end++
			}
			if d.dollar && (c == 'e' || c == 'E') && end == i+1 && end < len(text) && text[end] == '\'' {
				i = end
				end, quoted = stringEnd(text, i, true, true), true
			}
		case c == ';', c == '\\', c == '#' && d.backtick:
			return scanned, fmt.Errorf("%s may not contain %q outside a quoted literal", what, string(c))
		case strings.HasPrefix(text[i:], "--"), strings.HasPrefix(text[i:], "/*"):
			return scanned, fmt.Errorf("%s may not contain %q outside a quoted literal", what, text[i:i+2])
		case c == '(':
			depth++
		case c == ')':
			if depth == 0 {
				return scanned, fmt.Errorf("%s closes a parenthesis it did not open", what)
			}
			depth--
			top[i] = ' '
		}
		if end < 0 {
			return scanned, fmt.Errorf("%s has a quote that never ends (%s); double a quote inside a literal", what, clip(text[i:]))
		}
		if quoted {
			quote(i, end)
		}
		if depth > 0 {
			blank(top[i:end])
		}
		i = end
	}
	if depth > 0 {
		return scanned, fmt.Errorf("%s leaves a parenthesis open", what)
	}
	scanned.top = string(top)
	return scanned, nil
}

// stringEnd is the index after the literal or quoted identifier that starts at
// start, or -1. A doubled quote is the quote itself; with escapes a backslash
// takes the next byte; with continued a literal that a line break and another
// quote follow goes on in the same mode.
func stringEnd(text string, start int, escapes, continued bool) int {
	quote := text[start]
	for i := start + 1; i < len(text); i++ {
		switch {
		case escapes && text[i] == '\\':
			i++
		case text[i] == quote:
			if i+1 < len(text) && text[i+1] == quote {
				i++
				continue
			}
			rest := strings.TrimLeft(text[i+1:], " \t\n\r\f\v")
			between := text[i+1 : len(text)-len(rest)]
			if !continued || !strings.ContainsAny(between, "\n\r") || rest == "" || rest[0] != quote {
				return i + 1
			}
			i += len(between) + 1
		}
	}
	return -1
}

// dollarTag is the "$tag$" text starts with, or "".
func dollarTag(text string) string {
	for i := 1; i < len(text); i++ {
		switch {
		case text[i] == '$':
			return text[:i+1]
		case !identifierByte(text[i]), i == 1 && digit(text[i]):
			return ""
		}
	}
	return ""
}

func digit(c byte) bool { return c >= '0' && c <= '9' }

func identifierByte(c byte) bool {
	return c == '_' || c >= 0x80 || digit(c) || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z'
}

func blank(b []byte) {
	for i := range b {
		b[i] = ' '
	}
}

// clip shortens text for an error message.
func clip(text string) string {
	if len(text) > 40 {
		text = text[:40] + "..."
	}
	return fmt.Sprintf("%q", text)
}

// mysqlWire writes a carriage return inside a mysql string literal as its
// escape: the mysql client drops one before a line break.
func mysqlWire(text string) string {
	scanned, err := scanSQL("", text, mysqlDialect)
	if err != nil || strings.IndexByte(text, '\r') < 0 {
		return text
	}
	var out strings.Builder
	last := 0
	for _, span := range scanned.quoted {
		if text[span.start] == '`' {
			continue
		}
		out.WriteString(text[last:span.start])
		for i := span.start; i < span.end; i++ {
			switch {
			case text[i] == '\\' && text[i+1] != '\r':
				out.WriteString(text[i : i+2])
				i++
			case text[i] == '\\':
				// "\<CR>" is the carriage return itself.
			case text[i] == '\r':
				out.WriteString(`\r`)
			default:
				out.WriteByte(text[i])
			}
		}
		last = span.end
	}
	return out.String() + text[last:]
}
