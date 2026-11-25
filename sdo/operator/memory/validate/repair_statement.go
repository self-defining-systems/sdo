package validate

import (
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/parser"
	"go/token"
	"go/types"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/storeops"
)

// repairStatementDiagnostics checks, in the source of the changed playbook programs, what a learned program states in
// constants about a RepairData or a store scope: some store engine must accept the statement, the scope and, for a read,
// the fields together, the way the engine will run them, and ExpectMin/ExpectMax must change something. The replay
// cannot run a program whose reads the incident's capture lacks, so a plan that would fail on apply is otherwise found
// only on the repeat. Any Go constant expression is judged (a named constant, a concatenation of constants), not only a
// literal; text the program computes is not, and is judged where the replay yields the plan.
func repairStatementDiagnostics(root string, paths []string) []Diagnostic {
	packages := map[string][]string{}
	for _, file := range paths {
		if kind, _, ok := layout.Classify(file); ok && (kind == layout.KindPlaybookProgram || kind == layout.KindLibrary) && goSource(file) {
			packages[path.Dir(file)] = append(packages[path.Dir(file)], file)
		}
	}
	var diagnostics []Diagnostic
	for dir, changed := range packages {
		fset := token.NewFileSet()
		siblings, _ := filepath.Glob(filepath.Join(root, filepath.FromSlash(dir), "*.go"))
		var files []*ast.File
		names := map[*ast.File]string{}
		for _, sibling := range siblings {
			if !goSource(sibling) {
				continue
			}
			if parsed, err := parser.ParseFile(fset, sibling, nil, parser.SkipObjectResolution); err == nil {
				files = append(files, parsed)
				names[parsed] = path.Join(dir, filepath.Base(sibling))
			}
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}}
		// The SDK is not imported: an unresolved import leaves its types unknown and the package's own constants known.
		config := types.Config{Importer: noImports{}, Error: func(error) {}}
		_, _ = config.Check(dir, fset, files, info)
		for _, file := range files {
			name := names[file]
			for _, edited := range changed {
				if edited == name {
					diagnostics = append(diagnostics, storeLiteralDiagnostics(fset, info, file, name)...)
				}
			}
		}
	}
	sort.SliceStable(diagnostics, func(i, j int) bool {
		if diagnostics[i].Path != diagnostics[j].Path {
			return diagnostics[i].Path < diagnostics[j].Path
		}
		return diagnostics[i].Line < diagnostics[j].Line
	})
	return diagnostics
}

func goSource(file string) bool {
	return strings.HasSuffix(file, ".go") && !strings.HasSuffix(file, "_test.go")
}

type noImports struct{}

func (noImports) Import(string) (*types.Package, error) { return nil, errors.New("not imported") }

func storeLiteralDiagnostics(fset *token.FileSet, info *types.Info, file *ast.File, name string) []Diagnostic {
	var diagnostics []Diagnostic
	// A scope inside a RepairData or a StoreQuery is judged with it.
	judged := map[ast.Expr]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || judged[literal] {
			return true
		}
		var problems []string
		what := typeName(literal.Type)
		fields := literalFields(literal)
		switch what {
		case "RepairData":
			problems = repairLiteralProblems(info, fields)
			judged[scopeLiteral(fields["Scope"])] = true
		case "StoreQuery":
			problems = queryLiteralProblems(info, fields)
			judged[scopeLiteral(fields["Scope"])] = true
		case "DataScope", "Scope":
			if problem := (storeText{scope: literalFields(literal), info: info}).problem(); problem != "" {
				problems = append(problems, problem)
			}
		default:
			return true
		}
		for _, problem := range problems {
			diagnostics = append(diagnostics, Diagnostic{Rule: RuleProgramPlanInvalid, Path: name, Line: fset.Position(literal.Pos()).Line, Message: what + " " + problem,
				Hint: "it would be refused at run time: use \"delete\", an assignment list (postgres, mysql), an update document (mongodb) or a single-key write (redis), and a selector of the engine; a quoted value may hold any character"})
		}
		return true
	})
	return diagnostics
}

// typeName is the last name of a composite literal's type; "Scope" only as storeops.Scope.
func typeName(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.SelectorExpr:
		if base, ok := t.X.(*ast.Ident); t.Sel.Name == "Scope" && (!ok || base.Name != "storeops") {
			return ""
		}
		return t.Sel.Name
	case *ast.Ident:
		if t.Name == "Scope" {
			return ""
		}
		return t.Name
	}
	return ""
}

func literalFields(literal *ast.CompositeLit) map[string]ast.Expr {
	fields := map[string]ast.Expr{}
	for _, element := range literal.Elts {
		if pair, ok := element.(*ast.KeyValueExpr); ok {
			if key, ok := pair.Key.(*ast.Ident); ok {
				fields[key.Name] = pair.Value
			}
		}
	}
	return fields
}

// scopeLiteral is the composite literal expr is, or points to.
func scopeLiteral(expr ast.Expr) *ast.CompositeLit {
	if unary, ok := expr.(*ast.UnaryExpr); ok {
		expr = unary.X
	}
	literal, _ := expr.(*ast.CompositeLit)
	return literal
}

func repairLiteralProblems(info *types.Info, fields map[string]ast.Expr) []string {
	var problems []string
	text := storeText{info: info, target: fields["Target"]}
	if scope := scopeLiteral(fields["Scope"]); scope != nil {
		text.scope = literalFields(scope)
	}
	if statement, ok := constantString(info, fields["Statement"]); ok && statement != "" {
		text.statement = &statement
	}
	if problem := text.problem(); problem != "" {
		problems = append(problems, problem)
	}
	min, haveMin := constantInt(info, fields["ExpectMin"])
	max, haveMax := constantInt(info, fields["ExpectMax"])
	if haveMax && max == 0 {
		problems = append(problems, "ExpectMax is 0, so the repair changes nothing")
	} else if haveMin && haveMax && min > max {
		problems = append(problems, fmt.Sprintf("ExpectMin %d is above ExpectMax %d", min, max))
	}
	return problems
}

func queryLiteralProblems(info *types.Info, fields map[string]ast.Expr) []string {
	text := storeText{info: info, target: fields["Target"], read: []string{"id"}}
	if scope := scopeLiteral(fields["Scope"]); scope != nil {
		text.scope = literalFields(scope)
	}
	if list := scopeLiteral(fields["Fields"]); list != nil {
		var names []string
		for _, element := range list.Elts {
			name, ok := constantString(info, element)
			if !ok {
				names = nil
				break
			}
			names = append(names, name)
		}
		if names != nil {
			text.read = names
		}
	}
	if problem := text.problem(); problem != "" {
		return []string{problem}
	}
	return nil
}

// storeText is what a literal states in constants about one store operation. A scope field or target engine that is
// not constant may be anything.
type storeText struct {
	info      *types.Info
	target    ast.Expr
	scope     map[string]ast.Expr
	statement *string
	read      []string
}

// problem is "" when some store engine accepts the operation, else what each engine says.
func (t storeText) problem() string {
	engines := storeops.Engines()
	if target := scopeLiteral(t.target); target != nil {
		if engine, ok := constantString(t.info, literalFields(target)["Engine"]); ok {
			if _, known := storeops.For(engine); !known {
				return ""
			}
			engines = []string{engine}
		}
	}
	var reasons []string
	for _, name := range engines {
		err := t.refusal(name)
		if err == nil {
			return ""
		}
		reasons = append(reasons, err.Error())
	}
	return "is accepted by no store engine: " + strings.Join(reasons, "; ")
}

func (t storeText) refusal(name string) error {
	engine, _ := storeops.For(name)
	if t.statement != nil {
		if err := engine.ValidateStatement(*t.statement); err != nil {
			return fmt.Errorf("statement %q: %w", *t.statement, err)
		}
	}
	if t.scope == nil && t.read == nil {
		return nil
	}
	var refused error
	for _, scope := range t.scopes() {
		err := engine.ValidateScope(scope)
		if err == nil && t.read != nil {
			err = storeops.ValidateRead(engine, scope, t.read)
		}
		if err == nil {
			return nil
		}
		// The last candidate names a database and a collection, so its refusal is about the text itself.
		refused = err
	}
	return refused
}

// scopes are the scopes the literal can state: its constants, and for a field that is not one, no value or a plain one.
func (t storeText) scopes() []storeops.Scope {
	values := func(field string, unknown ...string) []string {
		expr, set := t.scope[field]
		if t.scope != nil && !set {
			return []string{""}
		}
		if value, ok := constantString(t.info, expr); ok {
			return []string{value}
		}
		return unknown
	}
	var scopes []storeops.Scope
	for _, database := range values("Database", "", "app") {
		for _, collection := range values("Collection", "", "items") {
			for _, selector := range values("Selector", "") {
				scope := storeops.Scope{Database: database, Collection: collection, Selector: selector}
				if limit, ok := constantInt(t.info, t.scope["Limit"]); ok {
					scope.Limit = int(limit)
				}
				scopes = append(scopes, scope)
			}
		}
	}
	return scopes
}

func constantString(info *types.Info, expr ast.Expr) (string, bool) {
	if value := constantValue(info, expr); value != nil && value.Kind() == constant.String {
		return constant.StringVal(value), true
	}
	return "", false
}

func constantInt(info *types.Info, expr ast.Expr) (int64, bool) {
	if value := constantValue(info, expr); value != nil && value.Kind() == constant.Int {
		return constant.Int64Val(value)
	}
	return 0, false
}

// constantValue is the value of a constant expression, else nil.
func constantValue(info *types.Info, expr ast.Expr) constant.Value {
	if expr == nil {
		return nil
	}
	return info.Types[expr].Value
}
