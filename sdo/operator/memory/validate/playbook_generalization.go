package validate

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// generatedPodName matches a controller-generated Pod name: an owner name, the
// pod-template hash and the random suffix. Kubernetes draws both generated
// segments from rand.SafeEncodeString's alphabet (no vowels, no 0, 1 or 3), so
// ordinary words and StatefulSet ordinals ("redis-0") do not match.
var generatedPodName = regexp.MustCompile(`(?:^|[^a-z0-9-])([a-z0-9](?:[-a-z0-9]*[a-z0-9])?-[bcdfghjklmnpqrstvwxz2456789]{6,10}-[bcdfghjklmnpqrstvwxz2456789]{5})(?:$|[^a-z0-9-])`)

// liveObjectNameDiagnostics rejects changed executable memory that names one
// cluster's live Pod. A Pod name dies with its Pod: a program, script or
// detector that carries one runs against a stale or different object on the
// next incident. Code derives the Pod from its bound workload instead.
func liveObjectNameDiagnostics(root string, paths []string) []Diagnostic {
	var diagnostics []Diagnostic
	for _, p := range paths {
		kind, _, ok := layout.Classify(p)
		if !ok {
			continue
		}
		var texts []string
		switch {
		case kind == layout.KindPlaybookScript:
			data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(p)))
			if err != nil {
				continue
			}
			texts = []string{string(data)}
		case (kind == layout.KindPlaybookProgram || kind == layout.KindLibrary || kind == layout.KindIncidentDetectorSource) && strings.HasSuffix(p, ".go") && !strings.HasSuffix(p, "_test.go"):
			texts = goStringLiterals(filepath.Join(root, filepath.FromSlash(p)))
		default:
			continue
		}
		for _, text := range texts {
			if match := generatedPodName.FindStringSubmatch(text); match != nil {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleLiveObjectName, Path: p,
					Message: fmt.Sprintf("names the live Pod %q; a generated Pod name belongs to one cluster's current Pod", match[1]),
					Hint:    "bind the owning workload by role and find its Pods through ReadView (owner relation), then exec with a typed Command{Pod: ...}/EnsureExec; never copy a Pod name from a receipt into code or RunScript"})
				break
			}
		}
	}
	return diagnostics
}

// goStringLiterals returns the unquoted string literals of one Go file; a file
// that does not parse is reported by the syntax rules, not here.
func goStringLiterals(file string) []string {
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	var texts []string
	ast.Inspect(parsed, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			if text, err := strconv.Unquote(literal.Value); err == nil {
				texts = append(texts, text)
			}
		}
		return true
	})
	return texts
}

// proseFields are SDK struct fields that explain a step to people. Their text
// never changes what a plan does, so repair signatures ignore it.
var proseFields = map[string]bool{"Reason": true, "Rationale": true, "Fact": true, "Observed": true, "Expected": true, "Summary": true, "Description": true}

// duplicateRepairDiagnostics compares each playbook added by this change with
// the active playbooks. A copy of a whole program (the same parameter kinds,
// Diagnose and Mitigate once names, prose, comments and identifier spelling
// are set aside) is rejected: it is the same playbook for another component.
// A new playbook that only repeats another's repair is noted, not rejected:
// its Diagnose may hold the evidence the other lacks, and merging a weaker
// Diagnose into a broader playbook widens what that playbook restarts.
func duplicateRepairDiagnostics(ctx context.Context, r Request, paths []string) ([]Diagnostic, error) {
	added := map[string]bool{}
	for _, p := range paths {
		kind, id, ok := layout.Classify(p)
		if !ok || kind != layout.KindPlaybookProgram && kind != layout.KindPlaybook && kind != layout.KindPlaybookScript {
			continue
		}
		id, _, _ = strings.Cut(id, "/")
		if added[id] {
			continue
		}
		if _, err := os.Stat(filepath.Join(r.Root, layout.PlaybooksDir, id)); err != nil {
			continue
		}
		if _, err := git(ctx, r.Root, "cat-file", "-e", r.Base+":"+path.Join(layout.PlaybooksDir, id)); err != nil {
			added[id] = true
		}
	}
	if len(added) == 0 {
		return nil, nil
	}
	entries, err := os.ReadDir(filepath.Join(r.Root, layout.PlaybooksDir))
	if err != nil {
		return nil, nil
	}
	var ids []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		retired, err := layout.ReadPlaybookRetired(filepath.Join(r.Root, layout.PlaybooksDir, entry.Name()))
		if err == nil && !retired {
			ids = append(ids, entry.Name())
		}
	}
	names := append([]string(nil), ids...)
	if base, err := gitText(ctx, r.Root, "rev-parse", "--verify", r.Base+"^{commit}"); err == nil {
		if facts, err := memory.SourceInventory(ctx, r.Root, base); err == nil {
			for _, resource := range facts.Resources {
				names = append(names, resource.GetName())
			}
		}
	}
	type signatures struct{ repair, program string }
	known := map[string]signatures{}
	for _, id := range ids {
		dir := filepath.Join(r.Root, layout.PlaybooksDir, id)
		repair, ok := ProgramSignature(dir, names, "Mitigate")
		if !ok {
			continue
		}
		whole, _ := ProgramSignature(dir, names, "Diagnose", "Mitigate")
		known[id] = signatures{repair: repair, program: whole}
	}
	// Existing playbooks first, then added ones in order, so a pair added
	// together reports the later one.
	sort.SliceStable(ids, func(i, j int) bool { return !added[ids[i]] && added[ids[j]] })
	var diagnostics []Diagnostic
	for index, id := range ids {
		mine, ok := known[id]
		if !added[id] || !ok {
			continue
		}
		for _, other := range ids[:index] {
			theirs, ok := known[other]
			if !ok || theirs.repair != mine.repair {
				continue
			}
			where := path.Join(layout.PlaybooksDir, id)
			if theirs.program == mine.program {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleDuplicateRepair, Path: where,
					Message: fmt.Sprintf("new playbook %q copies %q: same parameter kinds, Diagnose and Mitigate apart from names and prose", id, other),
					Hint:    fmt.Sprintf("keep one playbook per repair method: refine %q (bind the component by role, widen its incident detector) and remove the new playbook's files", other)})
			} else {
				r.noteGaps(Diagnostic{Rule: RuleSharedRepair, Path: where,
					Message: fmt.Sprintf("new playbook %q repeats the repair of %q with a different Diagnose", id, other),
					Hint:    fmt.Sprintf("if this is the same failure mechanism on another component, refine %q instead; keep a separate playbook only when its Diagnose needs evidence %q lacks", other, other)})
			}
			break
		}
	}
	return diagnostics, nil
}

// ProgramSignature is the comparable shape of a playbook program: its
// parameter kinds in order and the named methods (Mitigate for its repair,
// Diagnose and Mitigate for the whole program) with the package helpers they
// reach. Comments, the package name, prose fields, object and role names, and
// identifier spelling are normalized. ok is false without the methods or a
// parseable Spec; such programs are left to the other rules.
func ProgramSignature(dir string, names []string, methods ...string) (string, bool) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return "", false
	}
	fset := token.NewFileSet()
	roots := map[string]*ast.FuncDecl{}
	var roles, kinds []string
	// Package-level helpers the methods may reach, by name.
	helpers := map[string]ast.Decl{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			return "", false
		}
		for _, decl := range parsed.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				switch {
				case d.Recv != nil && d.Name.Name == "Spec":
					roles, kinds = specParams(d)
				case d.Recv != nil:
					roots[d.Name.Name] = d
				default:
					helpers[d.Name.Name] = d
				}
			case *ast.GenDecl:
				if d.Tok != token.CONST && d.Tok != token.VAR {
					continue
				}
				for _, spec := range d.Specs {
					if value, ok := spec.(*ast.ValueSpec); ok {
						for _, name := range value.Names {
							helpers[name.Name] = d
						}
					}
				}
			}
		}
	}
	if len(roles) == 0 {
		return "", false
	}
	var queue []ast.Decl
	visited := map[ast.Decl]bool{}
	for _, method := range methods {
		root, ok := roots[method]
		if !ok {
			return "", false
		}
		queue = append(queue, root)
		visited[root] = true
	}
	replacements := map[string]string{}
	for index, role := range roles {
		replacements[role] = fmt.Sprintf("<role%d>", index)
	}
	sorted := append([]string(nil), names...)
	sort.Slice(sorted, func(i, j int) bool { return len(sorted[i]) > len(sorted[j]) })
	// Visit the methods, then each helper in order of first reference, so
	// identifiers are numbered the same way in equal programs.
	identifiers := map[string]string{}
	var parts []string
	for len(queue) > 0 {
		decl := queue[0]
		queue = queue[1:]
		ast.Inspect(decl, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok {
				if helper, ok := helpers[ident.Name]; ok && !visited[helper] {
					visited[helper] = true
					queue = append(queue, helper)
				}
			}
			return true
		})
		parts = append(parts, normalizedDecl(fset, decl, identifiers, helpers, replacements, sorted))
	}
	return strings.Join(kinds, ",") + "\n" + strings.Join(parts, "\n"), true
}

// specParams reads the ParamSpec literals of a Spec body in order, including
// elements of a []program.ParamSpec whose element type is elided.
func specParams(spec *ast.FuncDecl) (roles, kinds []string) {
	isParamSpec := func(expr ast.Expr) bool {
		selector, ok := expr.(*ast.SelectorExpr)
		return ok && selector.Sel.Name == "ParamSpec"
	}
	read := func(literal *ast.CompositeLit) {
		fields := map[string]string{}
		for _, element := range literal.Elts {
			pair, ok := element.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, _ := pair.Key.(*ast.Ident)
			if key == nil {
				continue
			}
			switch value := pair.Value.(type) {
			case *ast.BasicLit:
				fields[key.Name], _ = strconv.Unquote(value.Value)
			case *ast.Ident:
				fields[key.Name] = value.Name
			}
		}
		roles = append(roles, fields["Name"])
		kinds = append(kinds, fields["Kind"]+"/"+fields["ValueKind"]+"/"+fields["Required"])
	}
	ast.Inspect(spec, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok {
			return true
		}
		if isParamSpec(literal.Type) {
			read(literal)
			return false
		}
		if array, ok := literal.Type.(*ast.ArrayType); ok && isParamSpec(array.Elt) {
			for _, element := range literal.Elts {
				if inner, ok := element.(*ast.CompositeLit); ok {
					read(inner)
				}
			}
			return false
		}
		return true
	})
	return roles, kinds
}

// normalizedDecl prints one declaration after erasing what does not change the
// repair: prose field values, object and program names inside strings, role
// names, and the spelling of package-local identifiers.
func normalizedDecl(fset *token.FileSet, decl ast.Decl, identifiers map[string]string, helpers map[string]ast.Decl, roles map[string]string, names []string) string {
	ast.Inspect(decl, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.KeyValueExpr:
			if key, ok := n.Key.(*ast.Ident); ok && proseFields[key.Name] {
				n.Value = ast.NewIdent("_prose")
				return false
			}
		case *ast.SelectorExpr:
			// Qualified SDK names (program.RestartRollout, ref.Name) are the repair.
			ast.Inspect(n.X, func(inner ast.Node) bool { return normalizeLeaf(inner, identifiers, helpers, roles, names) })
			return false
		}
		return normalizeLeaf(node, identifiers, helpers, roles, names)
	})
	var out bytes.Buffer
	if err := printer.Fprint(&out, fset, decl); err != nil {
		return ""
	}
	return out.String()
}

func normalizeLeaf(node ast.Node, identifiers map[string]string, helpers map[string]ast.Decl, roles map[string]string, names []string) bool {
	switch n := node.(type) {
	case *ast.Ident:
		if _, helper := helpers[n.Name]; (n.Obj != nil || helper) && n.Name != "_" {
			canonical, ok := identifiers[n.Name]
			if !ok {
				canonical = fmt.Sprintf("v%d", len(identifiers))
				identifiers[n.Name] = canonical
			}
			n.Name = canonical
		}
	case *ast.BasicLit:
		if n.Kind != token.STRING {
			return true
		}
		text, err := strconv.Unquote(n.Value)
		if err != nil {
			return true
		}
		if role, ok := roles[text]; ok {
			n.Value = strconv.Quote(role)
			return true
		}
		for _, name := range names {
			if name != "" {
				text = strings.ReplaceAll(text, name, "<name>")
			}
		}
		n.Value = strconv.Quote(text)
	}
	return true
}

// SourceBookkeepingAnnotations are the annotations SDO removes from both sides
// of every source and baseline comparison. It mirrors the controller's
// comparison policy; a runtime test keeps the two lists equal.
var SourceBookkeepingAnnotations = []string{
	"sdo.dev/source-tree",
	"sdo.dev/accepted-paths",
	"deployment.kubernetes.io/revision",
	"deprecated.daemonset.template.generation",
	"kubectl.kubernetes.io/last-applied-configuration",
	"kubectl.kubernetes.io/restartedAt",
}

// diffMethods return a source or baseline comparison, or a field of one.
var diffMethods = map[string]bool{"DiffAgainstSource": true, "DiffAgainstBaseline": true, "Field": true}

// normalizedReadDiagnostics rejects changed program code that looks up a
// bookkeeping annotation in a source or baseline comparison. SDO strips those
// annotations before comparing, so the lookup never sees a value: a Diagnose
// built on it keeps matching after its own repair (a restart it cannot see)
// and repeats the repair. The live object carries the annotation.
func normalizedReadDiagnostics(root string, paths []string) []Diagnostic {
	var diagnostics []Diagnostic
	for _, p := range paths {
		kind, _, ok := layout.Classify(p)
		if !ok || kind != layout.KindPlaybookProgram && kind != layout.KindLibrary || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, filepath.FromSlash(p)), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			if key, ok := strippedLookup(function.Body); ok {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleProgramNormalizedRead, Path: p,
					Message: fmt.Sprintf("%s looks up %q in a source or baseline comparison, which strips that annotation; the read never changes, so the program cannot observe its own repair", function.Name.Name, key),
					Hint:    "read bookkeeping annotations (restartedAt, revision) from the live object (v.Deployments(), KubernetesRead), and use the comparison only for fields it keeps"})
				break
			}
		}
	}
	return diagnostics
}

// strippedLookup finds a call or index expression that combines a value
// derived from a comparison with a stripped annotation key.
func strippedLookup(body *ast.BlockStmt) (string, bool) {
	derived := map[string]bool{}
	fromDiff := func(expr ast.Expr) bool {
		found := false
		ast.Inspect(expr, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				if selector, ok := n.Fun.(*ast.SelectorExpr); ok && diffMethods[selector.Sel.Name] {
					found = true
				}
			case *ast.Ident:
				found = found || derived[n.Name]
			}
			return !found
		})
		return found
	}
	// Two passes reach values assigned from earlier derived values.
	for range 2 {
		ast.Inspect(body, func(node ast.Node) bool {
			if assign, ok := node.(*ast.AssignStmt); ok {
				for _, value := range assign.Rhs {
					if fromDiff(value) {
						for _, target := range assign.Lhs {
							if ident, ok := target.(*ast.Ident); ok && ident.Name != "_" {
								derived[ident.Name] = true
							}
						}
					}
				}
			}
			return true
		})
	}
	key, found := "", false
	ast.Inspect(body, func(node ast.Node) bool {
		var operands []ast.Expr
		switch n := node.(type) {
		case *ast.CallExpr:
			operands = append(append(operands, n.Fun), n.Args...)
		case *ast.IndexExpr:
			operands = []ast.Expr{n.X, n.Index}
		default:
			return !found
		}
		literal, fromComparison := "", false
		for _, operand := range operands {
			if lit, ok := operand.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				if text, err := strconv.Unquote(lit.Value); err == nil {
					for _, annotation := range SourceBookkeepingAnnotations {
						if strings.Contains(text, annotation) {
							literal = annotation
						}
					}
				}
				continue
			}
			fromComparison = fromComparison || fromDiff(operand)
		}
		if literal != "" && fromComparison {
			key, found = literal, true
		}
		return !found
	})
	return key, found
}
