package runtime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// Follow all definitions and assignments of a computed producer, not only its
// initial value. Identity copies remain identity copies: their source writes
// are checked independently and wire validation still rejects invalid input.
func checkFailureReason(file *ast.File, expr ast.Expr, message proto.Message) error {
	seen := map[*ast.Object]bool{}
	var check func(ast.Expr) error
	check = func(expr ast.Expr) error {
		switch value := expr.(type) {
		case *ast.BasicLit:
			if value.Kind != token.STRING {
				break
			}
			text, err := strconv.Unquote(value.Value)
			if err != nil {
				return err
			}
			if text == "" {
				return nil
			}
			for _, allowed := range wire.StringIn(message, "failure_reason") {
				if text == allowed {
					return nil
				}
			}
			return fmt.Errorf("failure_reason %q absent from %s", text, message.ProtoReflect().Descriptor().FullName())
		case *ast.Ident:
			if value.Obj == nil {
				return fmt.Errorf("unresolved failure_reason %s", value.Name)
			}
			if seen[value.Obj] {
				return fmt.Errorf("cyclic failure_reason %s", value.Name)
			}
			seen[value.Obj] = true
			defer delete(seen, value.Obj)
			var inputs []ast.Expr
			var writeErr error
			ast.Inspect(file, func(node ast.Node) bool {
				switch statement := node.(type) {
				case *ast.AssignStmt:
					for i, left := range statement.Lhs {
						if id, ok := left.(*ast.Ident); ok && id.Obj == value.Obj && i < len(statement.Rhs) {
							if statement.Tok != token.ASSIGN && statement.Tok != token.DEFINE {
								writeErr = fmt.Errorf("computed update to failure_reason variable %s", value.Name)
							}
							inputs = append(inputs, statement.Rhs[i])
						}
					}
				case *ast.UnaryExpr:
					if statement.Op == token.AND {
						if id, ok := statement.X.(*ast.Ident); ok && id.Obj == value.Obj {
							writeErr = fmt.Errorf("failure_reason variable %s escapes by address", value.Name)
						}
					}
				case *ast.ValueSpec:
					for i, name := range statement.Names {
						if name.Obj == value.Obj && i < len(statement.Values) {
							inputs = append(inputs, statement.Values[i])
						}
					}
				}
				return true
			})
			if writeErr != nil {
				return writeErr
			}
			if len(inputs) == 0 {
				return fmt.Errorf("unclassified failure_reason variable %s", value.Name)
			}
			for _, input := range inputs {
				if err := check(input); err != nil {
					return err
				}
			}
			return nil
		case *ast.SelectorExpr:
			if value.Sel.Name == "FailureReason" {
				return nil
			} // identity forwarding, no vocabulary invented
		case *ast.CallExpr:
			if selector, ok := value.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "GetFailureReason" && len(value.Args) == 0 {
				return nil
			}
			// Database reasons have an exhaustive descriptor sweep below; all
			// FailureReason methods have their return expressions checked by the walk.
			if name, ok := value.Fun.(*ast.Ident); ok && name.Name == "databaseFailureCode" {
				return nil
			}
			if selector, ok := value.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "FailureReason" && len(value.Args) == 0 {
				return nil
			}
		}
		return fmt.Errorf("unclassified computed failure_reason %T; add descriptor-checked coverage", expr)
	}
	return check(expr)
}

func TestFailureReasonProducersClosedEnums(t *testing.T) {
	t.Run("database", TestDatabaseFailureReasonsBelongToBothReceiptContracts)
	t.Run("worker", TestWorkerFailureReasonProducerClosedEnums)
	t.Run("negative-controls", TestFailureReasonRatchetRejectsComputedVocabulary)
	messages := map[string]proto.Message{"ActionReport": &contracts.ActionReport{}, "PlanActionReport": &contracts.ActionReport{}, "RepairActionReceipt": &contracts.RepairActionReceipt{}, "continuationReceipt": &contracts.RepairActionReceipt{}, "AgentResult": &contracts.AgentResult{}, "ReflectionDecision": &contracts.ReflectionDecision{}}
	root := ".."
	checkedWrites := 0
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "gen" || entry.Name() == "vendor" || entry.Name() == "testdata" || (entry.Name() != ".." && entry.Name() != "." && strings.HasPrefix(entry.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		check := func(expr ast.Expr, message proto.Message) {
			checkedWrites++
			if err := checkFailureReason(file, expr, message); err != nil {
				t.Errorf("%s: %v", path, err)
			}
		}
		ast.Inspect(file, func(node ast.Node) bool {
			switch typed := node.(type) {
			case *ast.FuncDecl:
				if (typed.Name.Name == "FailureReason" || typed.Name.Name == "GetFailureReason") && typed.Body != nil {
					ast.Inspect(typed.Body, func(child ast.Node) bool {
						if result, ok := child.(*ast.ReturnStmt); ok {
							if len(result.Results) != 1 {
								t.Errorf("%s: unclassified failure_reason return", path)
							}
							for _, expression := range result.Results {
								check(expression, &contracts.ReflectionDecision{})
							}
						}
						return true
					})
				}
			case *ast.AssignStmt:
				for i, left := range typed.Lhs {
					selector, ok := left.(*ast.SelectorExpr)
					if !ok || selector.Sel.Name != "FailureReason" {
						continue
					}
					if typed.Tok != token.ASSIGN {
						t.Errorf("%s: computed failure_reason update must be a checked assignment", path)
						continue
					}
					if i >= len(typed.Rhs) {
						t.Errorf("%s: unclassified multiple-result failure_reason write", path)
						continue
					}
					var message proto.Message = &contracts.RepairActionReceipt{}
					ast.Inspect(selector.X, func(child ast.Node) bool {
						if field, ok := child.(*ast.SelectorExpr); ok && field.Sel.Name == "ActionReports" {
							message = &contracts.ActionReport{}
						}
						return true
					})
					check(typed.Rhs[i], message)
				}
			case *ast.CompositeLit:
				name := ""
				switch typ := typed.Type.(type) {
				case *ast.Ident:
					name = typ.Name
				case *ast.SelectorExpr:
					name = typ.Sel.Name
				}
				for _, element := range typed.Elts {
					if _, keyed := element.(*ast.KeyValueExpr); !keyed && messages[name] != nil {
						t.Errorf("%s: unkeyed %s bypasses failure_reason classification", path, name)
					}
					if field, ok := element.(*ast.KeyValueExpr); ok {
						if key, ok := field.Key.(*ast.Ident); ok && key.Name == "FailureReason" {
							message := messages[name]
							if message == nil {
								t.Errorf("%s: unclassified failure_reason producer type %s", path, name)
							} else {
								check(field.Value, message)
							}
						}
					}
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checkedWrites == 0 {
		t.Fatal("no production failure_reason writes checked")
	}
}

func TestFailureReasonRatchetRejectsComputedVocabulary(t *testing.T) {
	for _, source := range []string{
		`package p; func f(){ reason := "invented"; report.FailureReason = reason }`,
		`package p; func f(){ reason := "execution_failed"; reason = "invented"; report.FailureReason = reason }`,
		`package p; func f(){ reason := unknown(); report.FailureReason = reason }`,
		`package p; func f(){ reason := "execution_failed"; reason += "execution_failed"; report.FailureReason = reason }`,
		`package p; func f(){ reason := "execution_failed"; mutate(&reason); report.FailureReason = reason }`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "producer.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if assignment, ok := node.(*ast.AssignStmt); ok {
				if selector, ok := assignment.Lhs[0].(*ast.SelectorExpr); ok && selector.Sel.Name == "FailureReason" {
					if err := checkFailureReason(file, assignment.Rhs[0], &contracts.ActionReport{}); err == nil {
						t.Errorf("accepted invalid variable producer: %s", source)
					}
				}
			}
			return true
		})
	}
}

// Exhaust the only non-identity method producer accepted by the ratchet.
func TestWorkerFailureReasonProducerClosedEnums(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "../worker_registry/kubernetes_failure.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "FailureReason" {
			continue
		}
		found = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			if result, ok := node.(*ast.ReturnStmt); ok {
				for _, expression := range result.Results {
					if err := checkFailureReason(file, expression, &contracts.ReflectionDecision{}); err != nil {
						t.Error(err)
					}
				}
			}
			return true
		})
	}
	if !found {
		t.Fatal("worker reason producer disappeared; update ratchet")
	}
}
