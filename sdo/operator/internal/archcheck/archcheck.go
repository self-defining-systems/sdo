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
//	R4 no type assertion or type switch to a plugin config type outside its owner.
//	R5 ratchet: ManifestPaths/manifest_paths counts per file may only fall.
package archcheck

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
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
)

// Family is a plugin family: an interface package in Dir with implementation
// subpackages beneath it. SupportDirs are test-support children (rendertest)
// that are neither core nor implementations.
type Family struct {
	Dir         string
	SupportDirs []string
}

// ConfigType is a plugin config type that only its owning package may
// assert or switch to.
type ConfigType struct {
	Name     string
	Import   string
	OwnerPkg string
}

// ConfigTypeSource lets the checker prove ConfigTypes is complete: every
// exported type in Dir whose name ends in Suffix must be in the table.
type ConfigTypeSource struct {
	Dir    string
	Suffix string
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
	Module           string
	Families         []Family
	WiringDirs       []string
	ExemptDirs       []string
	ToolPrefixes     []string
	ImplMayImport    []string
	ConfigTypes      []ConfigType
	ConfigTypeSource ConfigTypeSource
	RatchetTokens    []string
	Allow            []Allow
	Ratchet          []RatchetEntry
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
		file, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return fmt.Errorf("archcheck: %w", err)
		}
		relFile, _ := filepath.Rel(root, p)
		relFile = filepath.ToSlash(relFile)
		cfg.checkFile(fset, file, relFile, add, counts)
		return nil
	})
	if err != nil {
		return nil, err
	}
	for a := range allowed {
		if !used[allowed[a]] {
			out = append(out, Violation{Rule: allowed[a].Rule, File: allowed[a].File, Message: "stale allowlist entry matches nothing; delete it (" + allowed[a].Reason + ")"})
		}
	}
	out = append(out, cfg.checkRatchet(counts)...)
	out = append(out, cfg.checkConfigTypeCompleteness(root)...)
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
	c.checkAssertions(fset, file, relFile, pkgPath, aliases, add)
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

func (c Config) checkAssertions(fset *token.FileSet, file *ast.File, relFile, pkgPath string, aliases map[string]string, add func(Violation)) {
	match := func(expr ast.Expr) *ConfigType {
		if star, ok := expr.(*ast.StarExpr); ok {
			expr = star.X
		}
		var importPath, name string
		switch t := expr.(type) {
		case *ast.SelectorExpr:
			id, ok := t.X.(*ast.Ident)
			if !ok {
				return nil
			}
			importPath, name = aliases[id.Name], t.Sel.Name
		case *ast.Ident:
			importPath, name = pkgPath, t.Name
		default:
			return nil
		}
		for i := range c.ConfigTypes {
			if c.ConfigTypes[i].Import == importPath && c.ConfigTypes[i].Name == name {
				return &c.ConfigTypes[i]
			}
		}
		return nil
	}
	check := func(expr ast.Expr, what string) {
		ct := match(expr)
		if ct == nil || pkgPath == ct.OwnerPkg || strings.HasPrefix(pkgPath, ct.OwnerPkg+"/") {
			return
		}
		add(Violation{Rule: R4, File: relFile, Line: fset.Position(expr.Pos()).Line, Message: what + " to plugin config type " + ct.Name + " outside owner " + ct.OwnerPkg})
	}
	ast.Inspect(file, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.TypeAssertExpr:
			if n.Type != nil {
				check(n.Type, "type assertion")
			}
		case *ast.TypeSwitchStmt:
			for _, clause := range n.Body.List {
				for _, e := range clause.(*ast.CaseClause).List {
					check(e, "type switch case")
				}
			}
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

func (c Config) checkRatchet(counts map[string]int) []Violation {
	var out []Violation
	allowed := map[string]int{}
	for _, r := range c.Ratchet {
		allowed[r.File] = r.Count
	}
	for file, got := range counts {
		want, ok := allowed[file]
		switch {
		case !ok:
			out = append(out, Violation{Rule: R5, File: file, Message: fmt.Sprintf("%d use(s) of %s in a file not on the ratchet; use the plugin config instead", got, strings.Join(c.RatchetTokens, "/"))})
		case got > want:
			out = append(out, Violation{Rule: R5, File: file, Message: fmt.Sprintf("count rose to %d from allowed %d; the ratchet may only fall", got, want)})
		case got < want:
			out = append(out, Violation{Rule: R5, File: file, Message: fmt.Sprintf("count fell to %d from %d; lower the Count in the rule table (delete the entry at 0)", got, want)})
		}
	}
	for file := range allowed {
		if _, ok := counts[file]; !ok {
			out = append(out, Violation{Rule: R5, File: file, Message: fmt.Sprintf("count fell to 0 from %d; delete the ratchet entry", allowed[file])})
		}
	}
	return out
}

// checkConfigTypeCompleteness fails when the contracts package declares a
// config type matching the table's suffix that the table does not list, so a
// new plugin config type cannot silently escape R4.
func (c Config) checkConfigTypeCompleteness(root string) []Violation {
	src := c.ConfigTypeSource
	if src.Dir == "" {
		return nil
	}
	dir := filepath.Join(root, filepath.FromSlash(src.Dir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		return []Violation{{Rule: R4, File: src.Dir, Message: "config type source directory unreadable: " + err.Error()}}
	}
	listed := map[string]bool{}
	for _, ct := range c.ConfigTypes {
		listed[ct.Name] = true
	}
	var out []Violation
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				name := spec.(*ast.TypeSpec).Name.Name
				if ast.IsExported(name) && strings.HasSuffix(name, src.Suffix) && !listed[name] {
					out = append(out, Violation{Rule: R4, File: src.Dir + "/" + e.Name(), Message: "config type " + name + " is not in the rule table's ConfigTypes"})
				}
			}
		}
	}
	return out
}
