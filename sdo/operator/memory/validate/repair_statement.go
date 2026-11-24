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

// repairStatementDiagnostics checks, in the source of the changed playbook programs, what a learned Mitigate states
// about a RepairData in literals: its Statement must be accepted by some store engine the way the engine will apply it,
// and its ExpectMin/ExpectMax must change something. The replay cannot run a Mitigate before Go has written the case,
// so a plan that would fail on apply is otherwise found only on the repeat. A Statement the program computes is not
// checked here; the plan check in the replay validates the plan it yields.
func repairStatementDiagnostics(root string, paths []string) []Diagnostic {
	var files []string
	for _, path := range paths {
		if kind, _, ok := layout.Classify(path); ok && kind == layout.KindPlaybookProgram && strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") {
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
					Hint: "the plan fails on apply, not here: use \"delete\", an assignment list (postgres, mysql), an update document (mongodb) or a single-key write (redis)"})
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
		basic, ok := pair.Value.(*ast.BasicLit)
		if !ok {
			continue
		}
		switch key.Name {
		case "Statement":
			statement, err := strconv.Unquote(basic.Value)
			if err != nil || basic.Kind != token.STRING {
				continue
			}
			if problem := statementProblem(statement); problem != "" {
				problems = append(problems, problem)
			}
		case "ExpectMin", "ExpectMax":
			if basic.Kind != token.INT {
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
