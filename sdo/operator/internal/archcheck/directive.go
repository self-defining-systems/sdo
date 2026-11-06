package archcheck

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// Directives let a hand-written file carry its own allowlist or ratchet entry
// so parallel PRs never edit one shared table:
//
//	//archcheck:allow R4 until=S3-13b reason="generic registry switches on the raw strategy"
//	//archcheck:ratchet R5 count=3 until=S3-11c
//
// A directive is a line comment that starts exactly with "//archcheck:" and may
// sit anywhere in the file (conventionally just above the package clause).
// Generated files cannot carry directives because generators overwrite
// comments; their entries stay in the rule table.

const directivePrefix = "//archcheck:"

// RDirective reports a malformed, misplaced or stale directive.
const RDirective Rule = "DIR"

type directive struct {
	verb   string // "allow" or "ratchet"
	rule   Rule
	until  string
	reason string
	count  int
	line   int
}

// parseDirectives returns the directives in file and a violation for each
// malformed one.
func (c Config) parseDirectives(fset *token.FileSet, file *ast.File, relFile string) ([]directive, []Violation) {
	var ds []directive
	var vs []Violation
	for _, group := range file.Comments {
		for _, cm := range group.List {
			if !strings.HasPrefix(cm.Text, directivePrefix) {
				continue
			}
			line := fset.Position(cm.Pos()).Line
			d, err := c.parseDirective(strings.TrimPrefix(cm.Text, directivePrefix))
			if err != nil {
				vs = append(vs, Violation{Rule: RDirective, File: relFile, Line: line, Message: err.Error()})
				continue
			}
			d.line = line
			ds = append(ds, d)
		}
	}
	if len(ds) > 0 && ast.IsGenerated(file) {
		return nil, append(vs, Violation{Rule: RDirective, File: relFile, Line: ds[0].line, Message: "generated files cannot carry archcheck directives (generators overwrite comments); put the entry in rules.go"})
	}
	return ds, vs
}

func (c Config) parseDirective(text string) (directive, error) {
	fields, err := splitDirective(text)
	if err != nil {
		return directive{}, fmt.Errorf("malformed archcheck directive: %v", err)
	}
	if len(fields) < 2 {
		return directive{}, fmt.Errorf("malformed archcheck directive %q: want <allow|ratchet> <rule> key=value...", text)
	}
	d := directive{verb: fields[0], rule: Rule(fields[1])}
	kv := map[string]string{}
	for _, f := range fields[2:] {
		k, v, ok := strings.Cut(f, "=")
		if !ok {
			return directive{}, fmt.Errorf("malformed archcheck directive %q: %q is not key=value", text, f)
		}
		if _, dup := kv[k]; dup {
			return directive{}, fmt.Errorf("archcheck directive repeats %s=", k)
		}
		kv[k] = v
	}
	allowed := map[string]bool{"until": true}
	switch d.verb {
	case "allow":
		if d.rule != R1 && d.rule != R2 && d.rule != R3 && d.rule != R4 {
			return directive{}, fmt.Errorf("allow directive rule %q must be R1..R4 (R5 uses ratchet)", d.rule)
		}
		allowed["reason"] = true
	case "ratchet":
		if d.rule != R5 {
			return directive{}, fmt.Errorf("ratchet directive rule %q must be R5", d.rule)
		}
		allowed["count"] = true
	default:
		return directive{}, fmt.Errorf("unknown archcheck directive %q (want allow or ratchet)", d.verb)
	}
	for k := range kv {
		if !allowed[k] {
			return directive{}, fmt.Errorf("%s directive does not take %s=", d.verb, k)
		}
	}
	d.until = kv["until"]
	if d.until == "" {
		return directive{}, fmt.Errorf("%s directive needs until=<PR id> naming the PR that removes it", d.verb)
	}
	if !c.knownPR(d.until) {
		return directive{}, fmt.Errorf("until=%s is not in TransitionalPRs in rules.go", d.until)
	}
	if d.verb == "allow" {
		d.reason = kv["reason"]
		if d.reason == "" {
			return directive{}, fmt.Errorf("allow directive needs a quoted reason=")
		}
	} else {
		n, err := strconv.Atoi(kv["count"])
		if err != nil || n <= 0 {
			return directive{}, fmt.Errorf("ratchet directive needs a positive count=, got %q", kv["count"])
		}
		d.count = n
	}
	return d, nil
}

func (c Config) knownPR(id string) bool {
	for _, p := range c.TransitionalPRs {
		if p == id {
			return true
		}
	}
	return false
}

// splitDirective splits on spaces, keeping a quoted value (key="a b") whole
// and unquoting it.
func splitDirective(s string) ([]string, error) {
	var out []string
	for i := 0; i < len(s); {
		if s[i] == ' ' || s[i] == '\t' {
			i++
			continue
		}
		start := i
		for i < len(s) && s[i] != ' ' && s[i] != '\t' {
			if s[i] == '"' {
				j := i + 1
				for j < len(s) && s[j] != '"' {
					if s[j] == '\\' {
						j++
					}
					j++
				}
				if j >= len(s) {
					return nil, fmt.Errorf("unterminated quote in %q", s)
				}
				i = j
			}
			i++
		}
		tok := s[start:i]
		if k, v, ok := strings.Cut(tok, "="); ok && strings.HasPrefix(v, `"`) {
			u, err := strconv.Unquote(v)
			if err != nil {
				return nil, fmt.Errorf("bad quoted value in %q", tok)
			}
			tok = k + "=" + u
		}
		out = append(out, tok)
	}
	return out, nil
}
