package miniyaml

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Decode reads JSON or the small YAML subset described in the fuzzops app-pack docs
// into v (a pointer to a struct). Unknown fields are an error, so a typo in a
// pack file is caught at load time.
func Decode(data []byte, v any) error {
	trim := bytes.TrimSpace(data)
	if len(trim) == 0 {
		return fmt.Errorf("empty file")
	}
	if trim[0] != '{' {
		tree, err := parseYAML(string(data))
		if err != nil {
			return err
		}
		b, err := json.Marshal(tree)
		if err != nil {
			return err
		}
		trim = b
	}
	dec := json.NewDecoder(bytes.NewReader(trim))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return fmt.Errorf("unexpected data after the first document")
	}
	return nil
}

type yline struct {
	num    int
	indent int
	text   string
}

type yparser struct {
	lines []yline
	pos   int
}

func parseYAML(src string) (any, error) {
	p := &yparser{}
	for i, raw := range strings.Split(src, "\n") {
		raw = strings.TrimRight(raw, "\r")
		if strings.Contains(raw, "\t") && strings.TrimSpace(raw) != "" && strings.HasPrefix(raw, "\t") {
			return nil, fmt.Errorf("line %d: tabs are not allowed for indentation", i+1)
		}
		text := strings.TrimRight(stripComment(raw), " ")
		if strings.TrimSpace(text) == "" {
			continue
		}
		ind := len(text) - len(strings.TrimLeft(text, " "))
		p.lines = append(p.lines, yline{num: i + 1, indent: ind, text: text[ind:]})
	}
	if len(p.lines) == 0 {
		return nil, fmt.Errorf("empty file")
	}
	v, err := p.block(p.lines[0].indent)
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.lines) {
		return nil, fmt.Errorf("line %d: unexpected indentation", p.lines[p.pos].num)
	}
	return v, nil
}

// stripComment removes a trailing # comment (a # at line start or after a
// space, outside quotes). A quote only opens at a value start.
func stripComment(s string) string {
	var q byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if q != 0 {
			if q == '"' && c == '\\' {
				i++
			} else if c == q {
				if q == '\'' && i+1 < len(s) && s[i+1] == '\'' {
					i++
				} else {
					q = 0
				}
			}
			continue
		}
		prev := byte(' ')
		if i > 0 {
			prev = s[i-1]
		}
		switch {
		case (c == '"' || c == '\'') && strings.IndexByte(" {[,", prev) >= 0:
			q = c
		case c == '#' && prev == ' ':
			return s[:i]
		}
	}
	return s
}

func isItem(text string) bool { return text == "-" || strings.HasPrefix(text, "- ") }

func (p *yparser) block(indent int) (any, error) {
	if isItem(p.lines[p.pos].text) {
		return p.list(indent)
	}
	return p.mapping(indent)
}

func (p *yparser) list(indent int) (any, error) {
	out := []any{}
	for p.pos < len(p.lines) {
		l := p.lines[p.pos]
		if l.indent != indent || !isItem(l.text) {
			if l.indent > indent {
				return nil, fmt.Errorf("line %d: unexpected indentation", l.num)
			}
			break
		}
		rest := strings.TrimLeft(strings.TrimPrefix(l.text, "-"), " ")
		if rest == "" {
			p.pos++
			if p.pos < len(p.lines) && p.lines[p.pos].indent > indent {
				v, err := p.block(p.lines[p.pos].indent)
				if err != nil {
					return nil, err
				}
				out = append(out, v)
			} else {
				out = append(out, nil)
			}
			continue
		}
		if _, _, ok := splitKey(rest); ok && rest[0] != '{' && rest[0] != '[' {
			// "- key: value": the item is a map whose first entry sits after the dash.
			off := len(l.text) - len(rest)
			p.lines[p.pos] = yline{num: l.num, indent: indent + off, text: rest}
			v, err := p.mapping(indent + off)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
			continue
		}
		v, err := parseValue(rest, l.num)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		p.pos++
	}
	return out, nil
}

func (p *yparser) mapping(indent int) (any, error) {
	out := map[string]any{}
	for p.pos < len(p.lines) {
		l := p.lines[p.pos]
		if l.indent < indent {
			break
		}
		if l.indent > indent {
			return nil, fmt.Errorf("line %d: unexpected indentation", l.num)
		}
		if isItem(l.text) {
			break
		}
		key, rest, ok := splitKey(l.text)
		if !ok {
			return nil, fmt.Errorf("line %d: expected \"key: value\", got %q", l.num, l.text)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("line %d: duplicate key %q", l.num, key)
		}
		p.pos++
		if rest != "" {
			v, err := parseValue(rest, l.num)
			if err != nil {
				return nil, err
			}
			out[key] = v
			continue
		}
		switch {
		case p.pos < len(p.lines) && p.lines[p.pos].indent > indent:
			v, err := p.block(p.lines[p.pos].indent)
			if err != nil {
				return nil, err
			}
			out[key] = v
		case p.pos < len(p.lines) && p.lines[p.pos].indent == indent && isItem(p.lines[p.pos].text):
			v, err := p.list(indent)
			if err != nil {
				return nil, err
			}
			out[key] = v
		default:
			out[key] = nil
		}
	}
	return out, nil
}

// splitKey splits "key: value" at the first ": " (or a trailing ":") outside quotes.
func splitKey(s string) (key, rest string, ok bool) {
	if s == "" {
		return
	}
	if s[0] == '"' || s[0] == '\'' {
		end := closeQuote(s, 0)
		if end < 0 || end+1 >= len(s) || s[end+1] != ':' || (end+2 < len(s) && s[end+2] != ' ') {
			return
		}
		k, err := unquote(s[:end+1])
		if err != nil {
			return
		}
		return k, strings.TrimSpace(s[end+2:]), true
	}
	if s[0] == '{' || s[0] == '[' {
		return
	}
	for i := 0; i < len(s); i++ {
		if s[i] == ':' && (i+1 == len(s) || s[i+1] == ' ') {
			return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:]), true
		}
	}
	return
}

// closeQuote returns the index of the quote closing the one at s[i], or -1.
func closeQuote(s string, i int) int {
	q := s[i]
	for j := i + 1; j < len(s); j++ {
		switch {
		case q == '"' && s[j] == '\\':
			j++
		case s[j] == q:
			if q == '\'' && j+1 < len(s) && s[j+1] == '\'' {
				j++
				continue
			}
			return j
		}
	}
	return -1
}

func unquote(s string) (string, error) {
	if s[0] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), nil
	}
	var out string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return "", fmt.Errorf("bad double-quoted string %s", s)
	}
	return out, nil
}

func parseValue(s string, line int) (any, error) {
	s = strings.TrimSpace(s)
	switch s[0] {
	case '{', '[':
		f := &flow{s: s}
		v, err := f.value()
		if err == nil {
			f.skip()
			if f.i != len(f.s) {
				err = fmt.Errorf("unexpected %q", f.s[f.i:])
			}
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %v", line, err)
		}
		return v, nil
	case '"', '\'':
		end := closeQuote(s, 0)
		if end != len(s)-1 {
			return nil, fmt.Errorf("line %d: text after the closing quote in %s", line, s)
		}
		v, err := unquote(s)
		if err != nil {
			return nil, fmt.Errorf("line %d: %v", line, err)
		}
		return v, nil
	}
	return scalar(s), nil
}

func scalar(s string) any {
	switch s {
	case "true":
		return true
	case "false":
		return false
	case "null", "~":
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && !strings.ContainsAny(s, "nNiI") {
		return f
	}
	return s
}

// flow parses {a: 1, b: [x, y]} and [a, b] on one line.
type flow struct {
	s string
	i int
}

func (f *flow) skip() {
	for f.i < len(f.s) && f.s[f.i] == ' ' {
		f.i++
	}
}

func (f *flow) value() (any, error) {
	f.skip()
	if f.i >= len(f.s) {
		return nil, fmt.Errorf("unexpected end of line")
	}
	switch c := f.s[f.i]; c {
	case '{':
		f.i++
		out := map[string]any{}
		for {
			f.skip()
			if f.i < len(f.s) && f.s[f.i] == '}' {
				f.i++
				return out, nil
			}
			k, err := f.key()
			if err != nil {
				return nil, err
			}
			v, err := f.value()
			if err != nil {
				return nil, err
			}
			out[k] = v
			if err := f.sep('}'); err != nil {
				return nil, err
			}
			if f.s[f.i-1] == '}' {
				return out, nil
			}
		}
	case '[':
		f.i++
		out := []any{}
		for {
			f.skip()
			if f.i < len(f.s) && f.s[f.i] == ']' {
				f.i++
				return out, nil
			}
			v, err := f.value()
			if err != nil {
				return nil, err
			}
			out = append(out, v)
			if err := f.sep(']'); err != nil {
				return nil, err
			}
			if f.s[f.i-1] == ']' {
				return out, nil
			}
		}
	case '"', '\'':
		end := closeQuote(f.s, f.i)
		if end < 0 {
			return nil, fmt.Errorf("unterminated quote")
		}
		v, err := unquote(f.s[f.i : end+1])
		f.i = end + 1
		return v, err
	}
	start := f.i
	for f.i < len(f.s) && !strings.ContainsRune(",}]", rune(f.s[f.i])) {
		f.i++
	}
	return scalar(strings.TrimSpace(f.s[start:f.i])), nil
}

func (f *flow) key() (string, error) {
	f.skip()
	if f.i < len(f.s) && (f.s[f.i] == '"' || f.s[f.i] == '\'') {
		end := closeQuote(f.s, f.i)
		if end < 0 {
			return "", fmt.Errorf("unterminated quote")
		}
		k, err := unquote(f.s[f.i : end+1])
		f.i = end + 1
		f.skip()
		if f.i >= len(f.s) || f.s[f.i] != ':' {
			return "", fmt.Errorf("expected ':' after key %q", k)
		}
		f.i++
		return k, err
	}
	start := f.i
	for f.i < len(f.s) {
		if f.s[f.i] == ':' && (f.i+1 == len(f.s) || f.s[f.i+1] == ' ') {
			k := strings.TrimSpace(f.s[start:f.i])
			f.i++
			return k, nil
		}
		if strings.ContainsRune(",}]", rune(f.s[f.i])) {
			break
		}
		f.i++
	}
	return "", fmt.Errorf("expected \"key: value\" in %q", f.s)
}

// sep consumes "," or the closing bracket.
func (f *flow) sep(closer byte) error {
	f.skip()
	if f.i >= len(f.s) {
		return fmt.Errorf("missing %q", closer)
	}
	switch f.s[f.i] {
	case ',':
		f.i++
		return nil
	case closer:
		f.i++
		return nil
	}
	return fmt.Errorf("expected ',' or %q at %q", closer, f.s[f.i:])
}
