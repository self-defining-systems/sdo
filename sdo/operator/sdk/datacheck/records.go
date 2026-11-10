package datacheck

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// record is one sampled record: its id and its value, decoded with
// json.Number so ids and counters keep their exact text.
type record struct {
	ID    string
	Value any
	// Undecodable is set when a JSON value did not decode; the record's shape
	// is then wrong.
	Undecodable bool
}

// readout is one complete read of one store instance.
type readout struct {
	Records   []record
	WrongType map[string]string // id -> the type the key holds
	Queried   map[string]bool   // keys a lookup was asked for
}

// parseOutput reads the line protocol of one store read. A read without its
// end marker, or one the store refused, is an error.
func parseOutput(s Store, stdout string) (readout, error) {
	if s.Engine == EngineMemcached {
		return parseMemcached(s, stdout)
	}
	out := readout{WrongType: map[string]string{}, Queried: map[string]bool{}}
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	complete := false
	for _, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		if strings.HasPrefix(line, "ERR ") {
			return readout{}, fmt.Errorf("store %s refused the read: %s", s.Component, clip(strings.TrimPrefix(line, "ERR ")))
		}
		if complete && line != "" {
			return readout{}, fmt.Errorf("store %s read printed after its end", s.Component)
		}
		parts := strings.SplitN(line, "\t", 3)
		switch parts[0] {
		case "E":
			complete = len(parts) == 1
		case "R":
			if len(parts) != 3 {
				return readout{}, fmt.Errorf("store %s read has a malformed record line", s.Component)
			}
			value, err := decodeJSON(parts[2])
			if err != nil {
				return readout{}, fmt.Errorf("store %s record %s: %w", s.Component, parts[1], err)
			}
			out.Records = append(out.Records, s.decodeValue(parts[1], value))
		case "W":
			if len(parts) == 3 {
				out.WrongType[parts[1]] = parts[2]
			}
		case "Q":
			if len(parts) == 2 {
				out.Queried[parts[1]] = true
			}
		}
	}
	if !complete {
		first := ""
		if len(lines) > 0 {
			first = lines[0]
		}
		return readout{}, fmt.Errorf("store %s read did not complete: %s", s.Component, clip(first))
	}
	if len(out.Records) > MaxRecords {
		return readout{}, fmt.Errorf("store %s read returned %d records, more than %d", s.Component, len(out.Records), MaxRecords)
	}
	return out, nil
}

func clip(s string) string {
	if len(s) > 200 {
		return s[:200] + "..."
	}
	return s
}

func decodeJSON(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var v any
	if err := decoder.Decode(&v); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, fmt.Errorf("trailing data after the value")
	}
	return v, nil
}

// decodeValue turns a string-typed value whose read says decode: json into
// the decoded value, and one that says ndjson into {"lines": [...]} with one
// decoded document per non-empty line ({"lines": {"$size": n}} past
// MaxMembers); anything else is kept as read.
func (s Store) decodeValue(id string, value any) record {
	r := record{ID: id, Value: value}
	if s.Read.Decode != "json" && s.Read.Decode != "ndjson" {
		return r
	}
	object, ok := value.(map[string]any)
	if !ok {
		return r
	}
	text, ok := object["value"].(string)
	if !ok {
		return r // a capped value ({"$long": n}) stays unchecked
	}
	if s.Read.Decode == "ndjson" {
		lines, ok := decodeLines(text)
		if !ok {
			r.Value, r.Undecodable = nil, true
			return r
		}
		r.Value = map[string]any{"lines": lines}
		return r
	}
	decoded, err := decodeJSON(text)
	if err != nil {
		r.Value, r.Undecodable = nil, true
		return r
	}
	r.Value = decoded
	return r
}

// decodeLines decodes newline-delimited JSON; it is false when a non-empty
// line is not one JSON document.
func decodeLines(text string) (any, bool) {
	var lines []any
	n := 0
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		n++
		v, err := decodeJSON(line)
		if err != nil {
			return nil, false
		}
		if len(lines) < MaxMembers {
			lines = append(lines, v)
		}
	}
	if n > MaxMembers {
		return map[string]any{"$size": json.Number(fmt.Sprint(n))}, true
	}
	if lines == nil {
		lines = []any{}
	}
	return lines, true
}

var memcachedValue = regexp.MustCompile(`^VALUE (\S+) \d+ (\d+)$`)

// parseMemcached reads the Q/K lines the script prints and the protocol answer
// to get: VALUE <key> <flags> <bytes>, the data, END.
func parseMemcached(s Store, stdout string) (readout, error) {
	out := readout{WrongType: map[string]string{}, Queried: map[string]bool{}}
	pre, suf, _ := strings.Cut(s.Read.Pattern, "{id}")
	rest := stdout
	ended, completed := false, false
	for rest != "" {
		line, tail, _ := strings.Cut(rest, "\n")
		rest = tail
		line = strings.TrimSuffix(line, "\r")
		switch {
		case strings.HasPrefix(line, "ERR "):
			return readout{}, fmt.Errorf("store %s refused the read: %s", s.Component, clip(strings.TrimPrefix(line, "ERR ")))
		case strings.HasPrefix(line, "Q\t"):
			out.Queried[strings.TrimPrefix(line, "Q\t")] = true
		case strings.HasPrefix(line, "K\t"), line == "":
		case line == "END":
			ended = true
		case line == "E":
			completed = true
		case memcachedValue.MatchString(line):
			m := memcachedValue.FindStringSubmatch(line)
			size, err := strconv.Atoi(m[2])
			if err != nil || size+2 > len(rest) || rest[size:size+2] != "\r\n" {
				return readout{}, fmt.Errorf("store %s value of %s exceeds the read bound", s.Component, m[1])
			}
			id := strings.TrimSuffix(strings.TrimPrefix(m[1], pre), suf)
			value := map[string]any{"value": rest[:size]}
			out.Records = append(out.Records, s.decodeValue(id, value))
			rest = rest[size+2:]
		default:
			return readout{}, fmt.Errorf("store %s answered %q", s.Component, clip(line))
		}
	}
	empty := len(out.Queried) == 0 && len(out.Records) == 0
	if !completed || !ended && !empty {
		return readout{}, fmt.Errorf("store %s read did not complete", s.Component)
	}
	return out, nil
}

// values returns the values at path: "a.b" walks objects, "list[]" takes each
// element of an array. Missing steps yield nothing.
func values(v any, path string) []any {
	out := []any{v}
	for _, segment := range strings.Split(path, ".") {
		name, each := strings.CutSuffix(segment, "[]")
		var next []any
		for _, x := range out {
			object, ok := x.(map[string]any)
			if !ok || marker(object) != "" {
				continue
			}
			y, ok := object[name]
			if !ok {
				continue
			}
			if !each {
				next = append(next, y)
				continue
			}
			if items, ok := y.([]any); ok {
				next = append(next, items...)
			}
		}
		out = next
	}
	return out
}

// marker names a cap marker the read printed in place of a value.
func marker(object map[string]any) string {
	if len(object) != 1 {
		return ""
	}
	for k := range object {
		switch k {
		case "$long", "$size", "$deep", "$type":
			return k
		}
	}
	return ""
}

// typeOf is the shape type of a value; capped values keep their type.
func typeOf(v any) string {
	switch x := v.(type) {
	case nil:
		return TypeNull
	case bool:
		return TypeBoolean
	case string:
		return TypeString
	case json.Number:
		if strings.ContainsAny(x.String(), ".eE") {
			return TypeNumber
		}
		return TypeInteger
	case []any:
		return TypeArray
	case map[string]any:
		switch marker(x) {
		case "$long":
			return TypeString
		case "$size":
			return TypeArray
		case "$type":
			return "other"
		}
		return TypeObject
	}
	return "other"
}

var digits = regexp.MustCompile(`^-?[0-9]+$`)

// matches reports whether a value has the shape type want.
func matches(v any, want string) bool {
	got := typeOf(v)
	switch want {
	case TypeAny:
		return true
	case TypeNumber:
		return got == TypeNumber || got == TypeInteger
	case TypeDigits:
		if s, ok := v.(string); ok {
			return digits.MatchString(s)
		}
		return got == TypeInteger
	}
	return got == want
}

// scalar is the comparable text of a scalar value; capped and composite
// values have none.
func scalar(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	case bool:
		return strconv.FormatBool(x), true
	case nil:
		return "null", true
	}
	return "", false
}

// number reads a numeric value or numeric text.
func number(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	}
	return 0, false
}

// partial reports whether a record's collection was capped: members lists
// fewer than size, or the value is a size marker.
func partial(v any, path string) bool {
	top := strings.TrimSuffix(strings.SplitN(path, ".", 2)[0], "[]")
	object, ok := v.(map[string]any)
	if !ok {
		return false
	}
	if inner, ok := object[top].(map[string]any); ok && marker(inner) != "" {
		return true
	}
	if top == "members" {
		members, _ := object["members"].([]any)
		size, ok := number(object["size"])
		return ok && int(size) > len(members)
	}
	for _, x := range values(v, strings.TrimSuffix(path, "[]")) {
		if inner, ok := x.(map[string]any); ok && marker(inner) != "" {
			return true
		}
	}
	return false
}

// texts returns the sorted distinct scalar texts at path, and false when a
// value there is capped or not a scalar.
func texts(v any, path string) ([]string, bool) {
	if partial(v, path) {
		return nil, false
	}
	var out []string
	for _, x := range values(v, path) {
		s, ok := scalar(x)
		if !ok {
			return nil, false
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return slices.Compact(out), true
}

// canonical is a value's JSON text, for an exact comparison of composite
// values.
func canonical(v any) string {
	var b bytes.Buffer
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(v)
	return strings.TrimSpace(b.String())
}
