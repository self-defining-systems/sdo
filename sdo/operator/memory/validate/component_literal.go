package validate

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// componentLiteralDiagnostics rejects a change that writes a component name
// (an accepted-source object or traffic scenario ID) into code a role or
// binding could supply it to: a playbook program, with or without a role (one
// without finds its subject by relation, so a name is as much an over-fit), or
// an incident detector that binds roles. Such code serves one component; the
// next incident on another component then copies it or enumerates a second
// name. Names the accepted version of the same playbook or detector already
// had are left alone, so editing old memory does not force a rewrite. Spec
// bodies, prose fields and error text are not component uses.
func componentLiteralDiagnostics(ctx context.Context, r Request, paths []string) []Diagnostic {
	type unit struct {
		dir      string
		detector bool
	}
	units := map[string]unit{}
	for _, p := range paths {
		kind, id, ok := layout.Classify(p)
		if !ok || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			continue
		}
		id, _, _ = strings.Cut(id, "/")
		switch kind {
		case layout.KindPlaybookProgram:
			units[path.Join(layout.PlaybooksDir, id)] = unit{dir: path.Join(layout.PlaybooksDir, id)}
		case layout.KindIncidentDetectorSource:
			dir := layout.DetectorDir(layout.ClassIncident, id)
			units[dir] = unit{dir: dir, detector: true}
		}
	}
	if len(units) == 0 {
		return nil
	}
	components := loadMemoryNames(ctx, r).components
	dirs := make([]string, 0, len(units))
	for dir := range units {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	var diagnostics []Diagnostic
	for _, dir := range dirs {
		u := units[dir]
		local := filepath.Join(r.Root, filepath.FromSlash(dir))
		sources := goSources(local)
		if len(sources) == 0 {
			continue
		}
		if u.detector {
			binds := false
			for _, file := range sources {
				binds = binds || bindsByName(filepath.Join(local, file))
			}
			if !binds {
				continue
			}
		}
		had := map[string]bool{}
		for _, file := range baseGoSources(ctx, r, dir) {
			for _, name := range componentUses(file.name, file.data, components) {
				had[name] = true
			}
		}
		var added []string
		for _, file := range sources {
			data, err := os.ReadFile(filepath.Join(local, file))
			if err != nil {
				continue
			}
			for _, name := range componentUses(file, data, components) {
				if !had[name] {
					had[name] = true
					added = append(added, strconv.Quote(name))
				}
			}
		}
		if len(added) == 0 {
			continue
		}
		sort.Strings(added)
		if u.detector {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleDetectorComponentLiteral, Path: dir,
				Message: fmt.Sprintf("incident detector names the component %s it should take from the failure; it covers only that component", strings.Join(added, ", ")),
				Hint:    "take the component from the failure: accept scenario failures (strings.HasPrefix(f.RuleID, traffic.RuleIDPrefix)) that carry the cause's own failure text (traffic.MatchFailures) or log pattern, and bind the role per suspect with localize.BindSuspects(f, result, role) (result from localize.FromContext on the application's probe workload), keeping only workloads the playbook can act on; the playbook's Diagnose checks its own evidence on the bound component"})
		} else {
			supplier := "that the application map, the owners or the source diff should supply"
			if roles := programRoles(local); len(roles) > 0 {
				supplier = "that its role " + strings.Join(roles, ", ") + " should supply"
			}
			diagnostics = append(diagnostics, Diagnostic{Rule: RulePlaybookComponentLiteral, Path: dir,
				Message: fmt.Sprintf("playbook program names the component %s %s; it repairs only that component", strings.Join(added, ", "), supplier),
				Hint:    "derive per-component values from the bound object, or for a role-less program from the subject found by relation, through ReadView: a container from the bound workload's Pod template, its Pods by owner, the failing journeys through it with localize.FromContext(v, <the application's probe workload>) and Implicates(ref) instead of a scenario ID; the same program then repairs every component its detector binds"})
		}
	}
	return diagnostics
}

// goSources lists the non-test Go files of dir.
func goSources(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var files []string
	for _, entry := range entries {
		if name := entry.Name(); !entry.IsDir() && strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
			files = append(files, name)
		}
	}
	return files
}

type sourceFile struct {
	name string
	data []byte
}

// baseGoSources reads the non-test Go files of dir at the base commit.
func baseGoSources(ctx context.Context, r Request, dir string) []sourceFile {
	listing, err := gitText(ctx, r.Root, "ls-tree", "--name-only", r.Base+":"+dir)
	if err != nil {
		return nil
	}
	var files []sourceFile
	for _, name := range strings.Split(listing, "\n") {
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := git(ctx, r.Root, "show", r.Base+":"+path.Join(dir, name))
		if err == nil {
			files = append(files, sourceFile{name: name, data: data})
		}
	}
	return files
}

// componentUses returns the component names that string literals of one Go
// source use, outside Spec bodies, prose fields and error text.
func componentUses(name string, data []byte, components []string) []string {
	parsed, err := parser.ParseFile(token.NewFileSet(), name, data, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	var used []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.FuncDecl:
			return !(n.Recv != nil && n.Name.Name == "Spec")
		case *ast.KeyValueExpr:
			if key, ok := n.Key.(*ast.Ident); ok && proseFields[key.Name] {
				return false
			}
		case *ast.AssignStmt:
			if selector, ok := n.Lhs[0].(*ast.SelectorExpr); ok && len(n.Lhs) == 1 && proseFields[selector.Sel.Name] {
				return false
			}
		case *ast.CallExpr:
			if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
				if pkg, ok := selector.X.(*ast.Ident); ok && messageCalls[pkg.Name+"."+selector.Sel.Name] {
					return false
				}
			}
		case *ast.BasicLit:
			if n.Kind != token.STRING {
				return false
			}
			text, err := strconv.Unquote(n.Value)
			if err != nil {
				return false
			}
			for _, component := range components {
				if component != "" && containsName(text, component) {
					used = append(used, component)
				}
			}
		}
		return true
	})
	return used
}
