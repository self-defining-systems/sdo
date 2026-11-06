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

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// proseFields are SDK struct fields that explain a step to people or record
// provenance. Their text never changes what a plan or finding does, so
// signatures ignore it.
var proseFields = map[string]bool{"Reason": true, "Rationale": true, "Fact": true, "Observed": true, "Expected": true, "Summary": true, "Description": true, "OriginatingIncident": true, "OriginatingCommit": true}

// messageCalls build error text: their string arguments are prose.
var messageCalls = map[string]bool{"fmt.Errorf": true, "errors.New": true}

// memoryNames are the names copy detection sets aside. ids are logical
// playbook and incident detector IDs; components are what a role or binding
// supplies on another incident: accepted-source object names and the
// scenario IDs of the traffic workloads.
type memoryNames struct {
	ids        []string
	components []string
}

func (n memoryNames) all() []string { return append(append([]string(nil), n.ids...), n.components...) }

func loadMemoryNames(ctx context.Context, r Request) memoryNames {
	var names memoryNames
	for _, dir := range []string{layout.PlaybooksDir, layout.DetectorDir(layout.ClassIncident, "")} {
		entries, _ := os.ReadDir(filepath.Join(r.Root, filepath.FromSlash(dir)))
		for _, entry := range entries {
			if entry.IsDir() {
				names.ids = append(names.ids, entry.Name())
			}
		}
	}
	// Component names are the accepted architecture's inventory lines (Go
	// wrote them from the rendered source), so every source kind contributes.
	if data, err := os.ReadFile(filepath.Join(r.Root, filepath.FromSlash(layout.ArchitectureFile))); err == nil {
		for key := range appmap.ParseInventory(string(data)).Workloads {
			_, name, _ := strings.Cut(key, "/")
			names.components = append(names.components, name)
		}
		sort.Strings(names.components)
	}
	names.components = append(names.components, scenarioIDs(r.Root)...)
	return names
}

// scenarioIDs lists the scenario IDs the traffic workloads declare.
func scenarioIDs(root string) []string {
	files, _ := filepath.Glob(filepath.Join(root, filepath.FromSlash(layout.TrafficWorkloadsDir), "*.yaml"))
	var ids []string
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		var workload struct {
			Scenarios []struct {
				ID string `yaml:"id"`
			} `yaml:"scenarios"`
		}
		if yaml.Unmarshal(data, &workload) != nil {
			continue
		}
		for _, scenario := range workload.Scenarios {
			if scenario.ID != "" {
				ids = append(ids, scenario.ID)
			}
		}
	}
	return ids
}

// addedIDs returns the IDs whose directory (dir(id)) this change creates:
// present in the checkout, absent at the base.
func addedIDs(ctx context.Context, r Request, paths []string, kinds map[layout.Kind]bool, dir func(string) string) map[string]bool {
	added := map[string]bool{}
	for _, p := range paths {
		kind, id, ok := layout.Classify(p)
		if !ok || !kinds[kind] {
			continue
		}
		id, _, _ = strings.Cut(id, "/")
		if added[id] {
			continue
		}
		if _, err := os.Stat(filepath.Join(r.Root, filepath.FromSlash(dir(id)))); err != nil {
			continue
		}
		if _, err := git(ctx, r.Root, "cat-file", "-e", r.Base+":"+dir(id)); err != nil {
			added[id] = true
		}
	}
	return added
}

// duplicateRepairDiagnostics compares each playbook added by this change with
// the active playbooks, setting aside names, prose, comments, layout and
// identifier spelling:
//   - the same parameter kinds, Diagnose and Mitigate is a copy
//     (playbook.duplicate-repair);
//   - the same Diagnose apart from the components it names (scenario,
//     workload, Service) is the same failure on another component
//     (playbook.component-copy): parameterize the existing playbook by role;
//   - only the same Mitigate is noted (playbook.shared-repair): the Diagnose
//     may hold evidence the other lacks, and merging a weaker Diagnose into a
//     broader playbook widens what that playbook repairs.
func duplicateRepairDiagnostics(ctx context.Context, r Request, paths []string) ([]Diagnostic, error) {
	added := addedIDs(ctx, r, paths, map[layout.Kind]bool{layout.KindPlaybookProgram: true, layout.KindPlaybook: true, layout.KindPlaybookScript: true},
		func(id string) string { return path.Join(layout.PlaybooksDir, id) })
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
	names := loadMemoryNames(ctx, r)
	type signatures struct{ repair, program, diagnosis, namedDiagnosis string }
	known := map[string]signatures{}
	for _, id := range ids {
		dir := filepath.Join(r.Root, layout.PlaybooksDir, id)
		repair, ok := ProgramSignature(dir, names.all(), "Mitigate")
		if !ok {
			continue
		}
		whole, _ := ProgramSignature(dir, names.all(), "Diagnose", "Mitigate")
		diagnosis, _ := ProgramSignature(dir, names.all(), "Diagnose")
		named, _ := ProgramSignature(dir, names.ids, "Diagnose")
		known[id] = signatures{repair: repair, program: whole, diagnosis: diagnosis, namedDiagnosis: named}
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
		where := path.Join(layout.PlaybooksDir, id)
		const (
			none = iota
			shared
			component
			duplicate
		)
		best, match := none, ""
		for _, other := range ids[:index] {
			theirs, ok := known[other]
			if !ok {
				continue
			}
			found := none
			switch {
			case theirs.program == mine.program:
				found = duplicate
			case theirs.diagnosis == mine.diagnosis && theirs.namedDiagnosis != mine.namedDiagnosis:
				found = component
			case theirs.repair == mine.repair:
				found = shared
			}
			if found > best {
				best, match = found, other
			}
		}
		roles := strings.Join(programRoles(filepath.Join(r.Root, layout.PlaybooksDir, match)), ", ")
		switch best {
		case duplicate:
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleDuplicateRepair, Path: where,
				Message: fmt.Sprintf("new playbook %q copies %q: same parameter kinds, Diagnose and Mitigate apart from names and prose", id, match),
				Hint:    fmt.Sprintf("keep one playbook per repair method: refine %q (bind the component by its role %s, widen its incident detector) and remove the new playbook's files", match, roles)})
		case component:
			named := componentLiterals(filepath.Join(r.Root, layout.PlaybooksDir, id), names.components)
			diagnostics = append(diagnostics, Diagnostic{Rule: RulePlaybookComponentCopy, Path: where,
				Message: fmt.Sprintf("new playbook %q diagnoses the same failure as %q on another component: the two Diagnose differ only in the components they name (%s)", id, match, strings.Join(named, ", ")),
				Hint:    fmt.Sprintf("parameterize %q by its role %s instead of copying it: derive per-component values (scenario, container, Service) from the bound object through ReadView, let its incident detector bind the role from the failure (localize.BindSuspects), and remove the new playbook's files; keep a separate playbook only with evidence that tells the two failures apart", match, roles)})
		case shared:
			r.noteGaps(Diagnostic{Rule: RuleSharedRepair, Path: where,
				Message: fmt.Sprintf("new playbook %q repeats the repair of %q with a different Diagnose", id, match),
				Hint:    fmt.Sprintf("if this is the same failure mechanism on another component, refine %q instead; keep a separate playbook only when its Diagnose needs evidence %q lacks", match, match)})
		}
	}
	return diagnostics, nil
}

// detectorCopyDiagnostics rejects an incident detector added by this change
// that is another incident detector apart from IDs, prose, layout,
// identifier spelling and the components it names (the scenario it filters,
// the workload it binds). One detector covers every component that fails
// the same way: it binds the component from the failure.
func detectorCopyDiagnostics(ctx context.Context, r Request, paths []string) []Diagnostic {
	dir := func(id string) string { return layout.DetectorDir(layout.ClassIncident, id) }
	added := addedIDs(ctx, r, paths, map[layout.Kind]bool{layout.KindIncidentDetectorSource: true, layout.KindIncidentDetectorManifest: true}, dir)
	if len(added) == 0 {
		return nil
	}
	entries, err := os.ReadDir(filepath.Join(r.Root, filepath.FromSlash(dir(""))))
	if err != nil {
		return nil
	}
	names := loadMemoryNames(ctx, r)
	var ids []string
	signatures := map[string]string{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if signature, ok := goSignature(filepath.Join(r.Root, filepath.FromSlash(dir(entry.Name()))), names.all(), false, "Spec", "Detect"); ok {
			ids = append(ids, entry.Name())
			signatures[entry.Name()] = signature
		}
	}
	sort.SliceStable(ids, func(i, j int) bool { return !added[ids[i]] && added[ids[j]] })
	var diagnostics []Diagnostic
	for index, id := range ids {
		if !added[id] {
			continue
		}
		for _, other := range ids[:index] {
			if signatures[other] != signatures[id] {
				continue
			}
			named := componentLiterals(filepath.Join(r.Root, filepath.FromSlash(dir(id))), names.components)
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleDetectorComponentCopy, Path: dir(id),
				Message: fmt.Sprintf("new incident detector %q copies %q apart from IDs, prose and the components it names (%s)", id, other, strings.Join(named, ", ")),
				Hint:    fmt.Sprintf("widen %q instead of copying it: keep it keyed on the cause's own failure text or log pattern, bind the component from the failure (localize.BindSuspects, or the failing finding's resource), list every playbook it binds, and remove the new detector's files", other)})
			break
		}
	}
	return diagnostics
}

// componentLiterals lists the component names that string literals in the
// non-test Go files of dir contain, sorted.
func componentLiterals(dir string, components []string) []string {
	files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	found := map[string]bool{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		for _, text := range goStringLiterals(file) {
			for _, name := range components {
				if name != "" && containsName(text, name) {
					found[name] = true
				}
			}
		}
	}
	named := make([]string, 0, len(found))
	for name := range found {
		named = append(named, name)
	}
	sort.Strings(named)
	return named
}

// containsName reports whether text contains name as a whole DNS-style name:
// not inside a longer name ("orders-cache" is not in "orders-cache-acl").
func containsName(text, name string) bool {
	inName := func(b byte) bool { return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '-' }
	for offset := 0; ; {
		index := strings.Index(text[offset:], name)
		if index < 0 {
			return false
		}
		start, end := offset+index, offset+index+len(name)
		if (start == 0 || !inName(text[start-1])) && (end == len(text) || !inName(text[end])) {
			return true
		}
		offset = start + 1
	}
}

// programRoles returns the parameter roles of the program in dir as
// "name (Kind)".
func programRoles(dir string) []string {
	files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		for _, decl := range parsed.Decls {
			if function, ok := decl.(*ast.FuncDecl); ok && function.Recv != nil && function.Name.Name == "Spec" {
				roles, kinds := specParams(function)
				described := make([]string, len(roles))
				for index, role := range roles {
					kind, _, _ := strings.Cut(kinds[index], "/")
					described[index] = fmt.Sprintf("%q (%s)", role, kind)
				}
				return described
			}
		}
	}
	return nil
}

// ProgramSignature is the comparable shape of a playbook program: its
// parameter kinds in order and the named methods (Mitigate for its repair,
// Diagnose and Mitigate for the whole program) with the package helpers they
// reach. Comments, layout, the package name, prose fields and error text, the
// given names and role names, and identifier spelling are normalized. ok is
// false without the methods or a parseable Spec; such programs are left to
// the other rules.
func ProgramSignature(dir string, names []string, methods ...string) (string, bool) {
	return goSignature(dir, names, true, methods...)
}

// goSignature is the normalized shape of the named methods of the Go package
// in dir and the package helpers they reach. With roles, the package must
// declare a program Spec with parameters; its role names and parameter kinds
// are part of the shape.
func goSignature(dir string, names []string, roles bool, methods ...string) (string, bool) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return "", false
	}
	fset := token.NewFileSet()
	roots := map[string]*ast.FuncDecl{}
	var roleNames, kinds []string
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
				case d.Recv != nil && d.Name.Name == "Spec" && roles:
					roleNames, kinds = specParams(d)
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
	if roles && len(roleNames) == 0 {
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
	for index, role := range roleNames {
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
		parts = append(parts, normalizedDecl(decl, identifiers, helpers, replacements, sorted))
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

// normalizedDecl renders one declaration's syntax tree after erasing what does
// not change the repair or finding: layout and comments, prose field values
// and error text, the given names inside strings, role names, the spelling of
// package-local identifiers, and `var x T = v` against `x := v`.
func normalizedDecl(decl ast.Decl, identifiers map[string]string, helpers map[string]ast.Decl, roles map[string]string, names []string) string {
	shortVariableDeclarations(decl)
	prose := func() ast.Expr { return ast.NewIdent("_prose") }
	ast.Inspect(decl, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.KeyValueExpr:
			if key, ok := n.Key.(*ast.Ident); ok && proseFields[key.Name] {
				n.Value = prose()
				return false
			}
		case *ast.AssignStmt:
			if selector, ok := n.Lhs[0].(*ast.SelectorExpr); ok && len(n.Lhs) == 1 && len(n.Rhs) == 1 && proseFields[selector.Sel.Name] {
				n.Rhs[0] = prose()
			}
		case *ast.CallExpr:
			if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
				if pkg, ok := selector.X.(*ast.Ident); ok && messageCalls[pkg.Name+"."+selector.Sel.Name] {
					for index, arg := range n.Args {
						if literal, ok := arg.(*ast.BasicLit); ok && literal.Kind == token.STRING {
							n.Args[index] = prose()
						}
					}
				}
			}
		case *ast.SelectorExpr:
			// Qualified SDK names (program.RestartRollout, ref.Name) are the repair.
			ast.Inspect(n.X, func(inner ast.Node) bool { return normalizeLeaf(inner, identifiers, helpers, roles, names) })
			return false
		}
		return normalizeLeaf(node, identifiers, helpers, roles, names)
	})
	return syntaxShape(decl)
}

// shortVariableDeclarations rewrites `var x T = v` statements to `x := v`.
func shortVariableDeclarations(decl ast.Decl) {
	rewrite := func(list []ast.Stmt) {
		for index, statement := range list {
			declaration, ok := statement.(*ast.DeclStmt)
			if !ok {
				continue
			}
			general, ok := declaration.Decl.(*ast.GenDecl)
			if !ok || general.Tok != token.VAR || len(general.Specs) != 1 {
				continue
			}
			value, ok := general.Specs[0].(*ast.ValueSpec)
			if !ok || len(value.Values) == 0 || len(value.Values) != len(value.Names) {
				continue
			}
			lhs := make([]ast.Expr, len(value.Names))
			for i, name := range value.Names {
				lhs[i] = name
			}
			list[index] = &ast.AssignStmt{Lhs: lhs, Tok: token.DEFINE, Rhs: value.Values}
		}
	}
	ast.Inspect(decl, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.BlockStmt:
			rewrite(n.List)
		case *ast.CaseClause:
			rewrite(n.Body)
		case *ast.CommClause:
			rewrite(n.Body)
		}
		return true
	})
}

// syntaxShape renders a syntax tree without positions, so line breaks,
// alignment and trailing commas do not change it.
func syntaxShape(node ast.Node) string {
	var out strings.Builder
	ast.Inspect(node, func(node ast.Node) bool {
		switch n := node.(type) {
		case nil:
			out.WriteByte(')')
			return false
		case *ast.CommentGroup, *ast.Comment:
			return false
		case *ast.Ident:
			fmt.Fprintf(&out, "(%s", n.Name)
		case *ast.BasicLit:
			fmt.Fprintf(&out, "(%s", n.Value)
		case *ast.BinaryExpr:
			fmt.Fprintf(&out, "(%s", n.Op)
		case *ast.UnaryExpr:
			fmt.Fprintf(&out, "(%s", n.Op)
		case *ast.AssignStmt:
			fmt.Fprintf(&out, "(=%s", n.Tok)
		case *ast.IncDecStmt:
			fmt.Fprintf(&out, "(%s", n.Tok)
		case *ast.BranchStmt:
			fmt.Fprintf(&out, "(%s", n.Tok)
		case *ast.GenDecl:
			fmt.Fprintf(&out, "(%s", n.Tok)
		case *ast.RangeStmt:
			fmt.Fprintf(&out, "(range%s", n.Tok)
		case *ast.ChanType:
			fmt.Fprintf(&out, "(chan%d", n.Dir)
		default:
			fmt.Fprintf(&out, "(%T", n)
		}
		return true
	})
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
