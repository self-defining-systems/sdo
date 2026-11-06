package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// telemetrySiteRoots are the trees whose production code builds brokers and
// validation requests: the operator modules and the benchmark replay broker.
var telemetrySiteRoots = []string{"..", filepath.Join("..", "..", "..", "benchmarks", "sregym", "fastloop", "replay_go")}

// Every broker and every validation request runs under the environment's
// telemetry: each production broker.New call passes an ObservationPolicy
// literal that names Telemetry, and each non-empty validate.Request literal
// names both Telemetry and TelemetryRegistry. A construction site that forgets
// them is how the validation subprocess and the controller brokers came to
// reject every binding (and the alerts detector on a provided source).
func TestEveryBrokerAndValidationRequestNamesItsTelemetry(t *testing.T) {
	var brokers, requests int
	for _, root := range telemetrySiteRoots {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if name := entry.Name(); name == "testdata" || name == "gen" || strings.HasPrefix(name, ".") && name != "." && name != ".." {
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
			inValidate := file.Name.Name == "validate"
			inBroker := file.Name.Name == "broker"
			ast.Inspect(file, func(node ast.Node) bool {
				switch n := node.(type) {
				case *ast.CallExpr:
					if isSelector(n.Fun, "broker", "New") || (inBroker && isIdent(n.Fun, "New")) {
						if len(n.Args) != 4 {
							return true
						}
						brokers++
						literal, ok := n.Args[3].(*ast.CompositeLit)
						if !ok || !hasKey(literal, "Telemetry") {
							t.Errorf("%s: broker.New must pass an ObservationPolicy literal naming Telemetry", path)
						}
					}
				case *ast.CompositeLit:
					if (isSelector(n.Type, "validate", "Request") || (inValidate && isIdent(n.Type, "Request"))) && len(n.Elts) > 0 {
						requests++
						if !hasKey(n, "Telemetry") || !hasKey(n, "TelemetryRegistry") {
							t.Errorf("%s: a validate.Request must name Telemetry and TelemetryRegistry", path)
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
	}
	if brokers < 5 || requests < 4 {
		t.Fatalf("found %d broker.New and %d validate.Request sites; the walk is not seeing production code", brokers, requests)
	}
}

func isSelector(expr ast.Expr, pkg, name string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && isIdent(selector.X, pkg) && selector.Sel.Name == name
}

func isIdent(expr ast.Expr, name string) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == name
}

func hasKey(literal *ast.CompositeLit, key string) bool {
	for _, element := range literal.Elts {
		if pair, ok := element.(*ast.KeyValueExpr); ok && isIdent(pair.Key, key) {
			return true
		}
	}
	return false
}
