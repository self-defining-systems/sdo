package validate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/storeops"
)

// repairStatementDiagnostics requires raw repair statements in changed learned
// code to be fixed and accepted by a store engine, even when captured reads
// cannot produce a plan. Plan.Validate checks concrete actions and their
// selected engine whenever Mitigate can run.
func repairStatementDiagnostics(root string, paths []string) []Diagnostic {
	var files []string
	for _, path := range paths {
		if kind, _, ok := layout.Classify(path); ok && (kind == layout.KindPlaybookProgram || kind == layout.KindLibrary) && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
	}
	sort.Strings(files)
	var diagnostics []Diagnostic
	for _, path := range files {
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(path)), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.CompositeLit)
			if !ok || !isRepairData(literal.Type) {
				return true
			}
			line := fset.Position(literal.Pos()).Line
			for _, problem := range repairLiteralProblems(literal) {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleProgramPlanInvalid, Path: path, Line: line, Message: "RepairData " + problem,
					Hint: "use a quoted engine-native statement such as \"SET status = 0\" (postgres, mysql) or \"delete\"; derive the target and scope separately"})
			}
			return true
		})
	}
	return diagnostics
}

func isRepairData(expr ast.Expr) bool {
	switch t := expr.(type) {
	case *ast.SelectorExpr:
		return t.Sel.Name == "RepairData"
	case *ast.Ident:
		return t.Name == "RepairData"
	}
	return false
}

func repairLiteralProblems(literal *ast.CompositeLit) []string {
	var problems []string
	var min, max *int
	for _, element := range literal.Elts {
		pair, ok := element.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		key, ok := pair.Key.(*ast.Ident)
		if !ok {
			continue
		}
		basic, _ := ast.Unparen(pair.Value).(*ast.BasicLit)
		switch key.Name {
		case "Statement":
			if basic == nil || basic.Kind != token.STRING {
				problems = append(problems, "Statement must be a fixed string literal so it can be checked without captured reads")
				continue
			}
			statement, err := strconv.Unquote(basic.Value)
			if err != nil {
				continue
			}
			if problem := statementProblem(statement); problem != "" {
				problems = append(problems, problem)
			}
		case "ExpectMin", "ExpectMax":
			if basic == nil || basic.Kind != token.INT {
				continue
			}
			value, err := strconv.Atoi(basic.Value)
			if err != nil {
				continue
			}
			if key.Name == "ExpectMin" {
				min = &value
			} else {
				max = &value
			}
		}
	}
	if max != nil && *max == 0 {
		problems = append(problems, "ExpectMax is 0, so the repair changes nothing")
	} else if min != nil && max != nil && *min > *max {
		problems = append(problems, fmt.Sprintf("ExpectMin %d is above ExpectMax %d", *min, *max))
	}
	return problems
}

// statementProblem is "" when some store engine accepts the statement, else what each engine says.
func statementProblem(statement string) string {
	var reasons []string
	for _, name := range storeops.Engines() {
		engine, _ := storeops.For(name)
		err := engine.ValidateStatement(statement)
		if err == nil {
			return ""
		}
		reasons = append(reasons, err.Error())
	}
	return fmt.Sprintf("statement %q is accepted by no store engine: %s", statement, strings.Join(reasons, "; "))
}
