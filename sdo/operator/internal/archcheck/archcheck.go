// Package archcheck enforces the core/plugin-family boundary of the operator
// from one declarative rule table (see Rules). It is purely syntactic: it
// parses Go files and never type-checks, so it also runs on fixtures that
// import libraries absent from go.mod.
//
// Rules:
//
//	R1 core packages import no tool library and no implementation subpackage.
//	R2 an implementation subpackage imports only its family interface package,
//	   its support package, stdlib, contracts, sdk and tool libraries.
//	R3 only wiring packages (and tests) import implementation subpackages.
//	R4 no use of a plugin config type outside its owner: type assertions and
//	   switches everywhere; composite literals, UnmarshalTo/anypb targets, var
//	   and parameter types and any other reference outside tests and wiring.
//	R5 ratchet: ManifestPaths/manifest_paths counts per file may only fall.
//	R6 wiring packages are the top layer: no non-test package outside them
//	   imports one (the production runtime stays below operatorcli and is
//	   handed what it needs by injection). A package main is a composition
//	   root and may import wiring.
//
// Known violations live in the violating file as //archcheck: directives (see
// directive.go); only generated files keep entries in the rule table.
package archcheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Rule identifies one boundary rule.
type Rule string

const (
	R1 Rule = "R1"
	R2 Rule = "R2"
	R3 Rule = "R3"
	R4 Rule = "R4"
	R5 Rule = "R5"
	R6 Rule = "R6"
)

// Family is a plugin family: an interface package in Dir with implementation
// subpackages beneath it. SupportDirs are test-support children (rendertest)
// that are neither core nor implementations.
type Family struct {
	Dir         string
	SupportDirs []string
}

// ConfigType is a plugin config type that only its owning package may name
// (assert, switch to, construct, decode into or declare). ConfigTypes is the
// explicit registry of such types: nothing infers membership from a name, so
// registering a new plugin config type is a deliberate edit to the rule table.
type ConfigType struct {
	Name     string
	Import   string
	OwnerPkg string
}

// Allow is a known violation. File is slash-separated, relative to the root.
type Allow struct {
	Rule   Rule
	File   string
	Reason string
	Slice  string
}

// RatchetEntry is the allowed count of ratchet tokens in a file.
type RatchetEntry struct {
	File   string
	Count  int
	Reason string
	Slice  string
}

// Config is the whole rule table.
type Config struct {
	Module        string
	Families      []Family
	WiringDirs    []string
	ExemptDirs    []string
	ToolPrefixes  []string
	ImplMayImport []string
	ConfigTypes   []ConfigType
	RatchetTokens []string
	// Allow and Ratchet hold entries for files that cannot carry directives
	// (generated code) and for fixtures; hand-written files use directives.
	Allow   []Allow
	Ratchet []RatchetEntry
	// TransitionalPRs is the closed list of PR ids a directive's until= may
	// name. Only the PRs that add or retire transitional forms edit it.
	TransitionalPRs []string
}

// Validate rejects incomplete tables; an allowlist entry without a reason or
// slice is a table bug.
func (c Config) Validate() error {
	if c.Module == "" {
		return fmt.Errorf("archcheck: empty module")
	}
	for _, a := range c.Allow {
		if a.File == "" || a.Reason == "" || a.Slice == "" {
			return fmt.Errorf("archcheck: allow entry %+v needs file, reason and slice", a)
		}
	}
	for _, r := range c.Ratchet {
		if r.File == "" || r.Reason == "" || r.Slice == "" || r.Count <= 0 {
			return fmt.Errorf("archcheck: ratchet entry %+v needs file, positive count, reason and slice", r)
		}
	}
	return nil
}

type kind int

const (
	kindOther kind = iota
	kindCore
	kindImpl
	kindSupport
	kindWiring
	kindExempt
)

// classify places a package (path relative to the module root) in a role.
// For implementations it also returns the owning family.
func (c Config) classify(rel string) (kind, *Family) {
	for _, d := range c.ExemptDirs {
		if within(rel, d) {
			return kindExempt, nil
		}
	}
	for _, d := range c.WiringDirs {
		if within(rel, d) {
			return kindWiring, nil
		}
	}
	for i := range c.Families {
		f := &c.Families[i]
		if !within(rel, f.Dir) || rel == f.Dir {
			continue
		}
		child := strings.SplitN(strings.TrimPrefix(rel, f.Dir+"/"), "/", 2)[0]
		for _, s := range f.SupportDirs {
			if child == s {
				return kindSupport, f
			}
		}
		return kindImpl, f
	}
	return kindCore, nil
}

func within(rel, dir string) bool { return rel == dir || strings.HasPrefix(rel, dir+"/") }

// Violation is one failed rule.
type Violation struct {
	Rule    Rule
	File    string
	Line    int
	Message string
}

func (v Violation) String() string {
	return fmt.Sprintf("%s %s:%d: %s", v.Rule, v.File, v.Line, v.Message)
}

// Check walks root (the operator module directory, nested modules included)
// and returns every violation not covered by the allowlist, plus stale
// allowlist and ratchet entries. Output is sorted.
func Check(root string, cfg Config) ([]Violation, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	var out []Violation
	used := map[Allow]bool{}
	counts := map[string]int{}
	allowed := map[string]Allow{}
	for _, a := range cfg.Allow {
		allowed[string(a.Rule)+" "+a.File] = a
	}
	fromDirective := map[string]bool{}
	ratchets := map[string]directive{}
	add := func(v Violation) {
		if a, ok := allowed[string(v.Rule)+" "+v.File]; ok {
			used[a] = true
			return
		}
		out = append(out, v)
	}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "testdata" || n == "vendor" || strings.HasPrefix(n, ".") && p != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution|parser.ParseComments)
		if err != nil {
			return fmt.Errorf("archcheck: %w", err)
		}
		relFile, _ := filepath.Rel(root, p)
		relFile = filepath.ToSlash(relFile)
		if role, _ := cfg.classify(path.Dir(relFile)); role != kindExempt {
			ds, vs := cfg.parseDirectives(fset, file, relFile)
			out = append(out, vs...)
			for _, d := range ds {
				out = append(out, cfg.registerDirective(d, relFile, allowed, fromDirective, ratchets)...)
			}
		}
		cfg.checkFile(fset, file, relFile, add, counts)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for k, a := range allowed {
		if !used[a] {
			what := "allowlist entry in rules.go"
			if fromDirective[k] {
				what = "archcheck:allow directive"
			}
			out = append(out, Violation{Rule: a.Rule, File: a.File, Message: "stale " + what + " matches nothing; delete it (" + a.Reason + ")"})
		}
	}
	out = append(out, cfg.checkRatchet(counts, ratchets)...)
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

func (c Config) checkFile(fset *token.FileSet, file *ast.File, relFile string, add func(Violation), counts map[string]int) {
	relDir := path.Dir(relFile)
	if relDir == "." {
		relDir = ""
	}
	isTest := strings.HasSuffix(relFile, "_test.go")
	pkgPath := c.Module
	if relDir != "" {
		pkgPath += "/" + relDir
	}
	role, family := c.classify(relDir)
	if role == kindExempt {
		return
	}
	aliases := map[string]string{}
	for _, spec := range file.Imports {
		imported, _ := strconv.Unquote(spec.Path.Value)
		name := path.Base(imported)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		aliases[name] = imported
		line := fset.Position(spec.Pos()).Line
		report := func(r Rule, msg string) { add(Violation{Rule: r, File: relFile, Line: line, Message: msg}) }
		tool := c.isTool(imported)
		target, _ := c.classifyImport(imported)
		if target == kindWiring && role != kindWiring && !isTest && file.Name.Name != "main" {
			report(R6, "imports wiring package "+imported+"; wiring is the top layer, inject the dependency instead")
		}
		switch role {
		case kindCore:
			if tool {
				report(R1, "core imports tool library "+imported)
			}
			if target == kindImpl && !isTest {
				report(R1, "core imports implementation package "+imported)
			}
		case kindImpl:
			if target == kindImpl && !within(strings.TrimPrefix(imported, c.Module+"/"), family.Dir+"/"+implName(relDir, family)) {
				report(R2, "implementation imports sibling or foreign implementation "+imported)
			} else if strings.HasPrefix(imported, c.Module+"/") && target != kindImpl && !c.implMayImport(imported, family) {
				report(R2, "implementation imports core package "+imported)
			}
		case kindSupport:
			if target == kindImpl && !isTest {
				report(R3, "support package imports implementation package "+imported)
			}
		}
	}
	c.checkAssertions(fset, file, relFile, pkgPath, aliases, role, add)
	counts[relFile] = c.countRatchet(file)
	if counts[relFile] == 0 {
		delete(counts, relFile)
	}
}

func implName(relDir string, f *Family) string {
	return strings.SplitN(strings.TrimPrefix(relDir, f.Dir+"/"), "/", 2)[0]
}

func (c Config) isTool(imported string) bool {
	for _, p := range c.ToolPrefixes {
		if strings.HasPrefix(imported, p) {
			return true
		}
	}
	return false
}

func (c Config) classifyImport(imported string) (kind, *Family) {
	if !strings.HasPrefix(imported, c.Module+"/") {
		return kindOther, nil
	}
	return c.classify(strings.TrimPrefix(imported, c.Module+"/"))
}

func (c Config) implMayImport(imported string, f *Family) bool {
	for _, p := range c.ImplMayImport {
		if imported == p || strings.HasPrefix(imported, p+"/") {
			return true
		}
	}
	rel := strings.TrimPrefix(imported, c.Module+"/")
	if rel == f.Dir {
		return true
	}
	for _, s := range f.SupportDirs {
		if rel == f.Dir+"/"+s {
			return true
		}
	}
	return false
}

// checkAssertions enforces R4. The check is syntactic: it sees a registered
// type only where the file names it as pkg.Type, which covers every way code
// can construct or decode one (composite literals, UnmarshalTo and anypb
// targets, var and parameter types, generic arguments) without type
// information. Reading fields of a value obtained without naming the type is
// not visible here; the type assertion or decode that produced it is.
//
// Type assertions and switches are checked everywhere. The other uses are
// exempt in tests (fixtures build configs) and in wiring packages (which
// register plugins by naming their config type).
func (c Config) checkAssertions(fset *token.FileSet, file *ast.File, relFile, pkgPath string, aliases map[string]string, role kind, add func(Violation)) {
	match := func(expr ast.Expr) *ConfigType {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return nil
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return nil
		}
		for i := range c.ConfigTypes {
			if c.ConfigTypes[i].Import == aliases[id.Name] && c.ConfigTypes[i].Name == sel.Sel.Name {
				return &c.ConfigTypes[i]
			}
		}
		return nil
	}
	lenient := strings.HasSuffix(relFile, "_test.go") || role == kindWiring
	// context records, parent first, how a type expression is used.
	context := map[ast.Expr]string{}
	mark := func(expr ast.Expr, what string) {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		context[expr] = what
	}
	report := func(expr ast.Expr, what string, strict bool) {
		ct := match(expr)
		if ct == nil || pkgPath == ct.OwnerPkg || strings.HasPrefix(pkgPath, ct.OwnerPkg+"/") {
			return
		}
		if !strict && lenient {
			return
		}
		add(Violation{Rule: R4, File: relFile, Line: fset.Position(expr.Pos()).Line, Message: what + " plugin config type " + ct.Name + " outside owner " + ct.OwnerPkg})
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.TypeAssertExpr:
			if n.Type != nil {
				mark(n.Type, "type assertion to")
				report(n.Type, "type assertion to", true)
			}
		case *ast.TypeSwitchStmt:
			for _, clause := range n.Body.List {
				for _, e := range clause.(*ast.CaseClause).List {
					mark(e, "type switch case on")
					report(e, "type switch case on", true)
				}
			}
		case *ast.CompositeLit:
			if n.Type != nil {
				mark(n.Type, "composite literal of")
			}
		case *ast.SelectorExpr:
			if _, handled := context[n]; handled {
				if context[n] == "composite literal of" {
					report(n, context[n], false)
				}
				return true
			}
			report(n, "use of", false)
		}
		return true
	})
}

func (c Config) countRatchet(file *ast.File) int {
	n := 0
	has := func(s string) bool {
		for _, t := range c.RatchetTokens {
			if strings.Contains(s, t) {
				return true
			}
		}
		return false
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch x := node.(type) {
		case *ast.Ident:
			if has(x.Name) {
				n++
			}
		case *ast.BasicLit:
			if x.Kind == token.STRING && has(x.Value) {
				n++
			}
		}
		return true
	})
	return n
}

// registerDirective records one parsed directive and reports a conflict when
// the rule table already holds an entry for the same file.
func (c Config) registerDirective(d directive, relFile string, allowed map[string]Allow, fromDirective map[string]bool, ratchets map[string]directive) []Violation {
	conflict := func(msg string) []Violation {
		return []Violation{{Rule: RDirective, File: relFile, Line: d.line, Message: msg}}
	}
	if d.verb == "ratchet" {
		for _, r := range c.Ratchet {
			if r.File == relFile {
				return conflict("file has an archcheck:ratchet directive and an entry in rules.go; keep only the directive")
			}
		}
		if _, dup := ratchets[relFile]; dup {
			return conflict("file has more than one archcheck:ratchet directive")
		}
		ratchets[relFile] = d
		return nil
	}
	key := string(d.rule) + " " + relFile
	if _, dup := allowed[key]; dup {
		return conflict("duplicate allow " + string(d.rule) + " for this file (directive or rules.go entry)")
	}
	allowed[key] = Allow{Rule: d.rule, File: relFile, Reason: d.reason, Slice: d.until}
	fromDirective[key] = true
	return nil
}

func (c Config) checkRatchet(counts map[string]int, directives map[string]directive) []Violation {
	var out []Violation
	type limit struct {
		count int
		dir   bool
		line  int
	}
	allowed := map[string]limit{}
	for _, r := range c.Ratchet {
		allowed[r.File] = limit{count: r.Count}
	}
	for f, d := range directives {
		allowed[f] = limit{count: d.count, dir: true, line: d.line}
	}
	lower := func(l limit) string {
		if l.dir {
			return "lower count= in this file's archcheck:ratchet directive"
		}
		return "lower the Count in the rule table"
	}
	for file, got := range counts {
		want, ok := allowed[file]
		switch {
		case !ok:
			out = append(out, Violation{Rule: R5, File: file, Message: fmt.Sprintf("%d use(s) of %s in a file without an archcheck:ratchet directive; use the plugin config instead", got, strings.Join(c.RatchetTokens, "/"))})
		case got > want.count:
			out = append(out, Violation{Rule: R5, File: file, Line: want.line, Message: fmt.Sprintf("count rose to %d from allowed %d; the ratchet may only fall", got, want.count)})
		case got < want.count:
			out = append(out, Violation{Rule: R5, File: file, Line: want.line, Message: fmt.Sprintf("count fell to %d from %d; %s", got, want.count, lower(want))})
		}
	}
	for file, want := range allowed {
		if _, ok := counts[file]; !ok {
			what := "delete the ratchet entry"
			if want.dir {
				what = "delete this file's archcheck:ratchet directive"
			}
			out = append(out, Violation{Rule: R5, File: file, Line: want.line, Message: fmt.Sprintf("stale ratchet: count fell to 0 from %d; %s", want.count, what)})
		}
	}
	return out
}
