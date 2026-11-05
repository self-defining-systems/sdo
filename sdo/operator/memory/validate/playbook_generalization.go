package validate

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

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

// comparisonMethods return a source or baseline comparison.
var comparisonMethods = map[string]bool{"DiffAgainstSource": true, "DiffAgainstBaseline": true}

// messagePackages build text, not reads: a key in an error message is not a lookup.
var messagePackages = map[string]bool{"fmt": true, "errors": true, "log": true, "log/slog": true}

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
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(p)), nil, parser.SkipObjectResolution)
		if err != nil {
			continue
		}
		info := resolveIdentifiers(fset, parsed)
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			if key, ok := strippedLookup(function.Body, info); ok {
				diagnostics = append(diagnostics, Diagnostic{Rule: RuleProgramNormalizedRead, Path: p,
					Message: fmt.Sprintf("%s looks up %q in a source or baseline comparison, which strips that annotation; the read never changes, so the program cannot observe its own repair", function.Name.Name, key),
					Hint:    "read bookkeeping annotations (restartedAt, revision) from the live object (v.Deployments(), KubernetesRead), and use the comparison only for fields it keeps"})
				break
			}
		}
	}
	return diagnostics
}

// resolveIdentifiers binds each identifier to the variable it denotes, so two
// variables that share a name in different scopes stay distinct. Imports are
// not loaded: the file is checked alone and type errors are ignored; only the
// scope resolution is used.
func resolveIdentifiers(fset *token.FileSet, file *ast.File) *types.Info {
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	config := types.Config{Error: func(error) {}, Importer: emptyImporter{}}
	_, _ = config.Check(file.Name.Name, fset, []*ast.File{file}, info)
	return info
}

// emptyImporter gives every import an empty package: the check needs only
// local scopes and the import path of each package name.
type emptyImporter struct{}

func (emptyImporter) Import(importPath string) (*types.Package, error) {
	pkg := types.NewPackage(importPath, path.Base(importPath))
	pkg.MarkComplete()
	return pkg, nil
}

// strippedLookup finds an index or call expression that combines a value
// derived from a comparison with a stripped annotation key. A variable is
// derived when every assignment to it holds a comparison, a field of one, or
// another derived variable; the error result of a comparison is not derived,
// and a variable also assigned anything else is not judged.
func strippedLookup(body *ast.BlockStmt, info *types.Info) (string, bool) {
	object := func(ident *ast.Ident) types.Object {
		if defined := info.Defs[ident]; defined != nil {
			return defined
		}
		return info.Uses[ident]
	}
	derived, other := map[types.Object]bool{}, map[types.Object]bool{}
	var fromComparison func(ast.Expr) bool
	fromComparison = func(expr ast.Expr) bool {
		found := false
		ast.Inspect(expr, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				if selector, ok := n.Fun.(*ast.SelectorExpr); ok && (comparisonMethods[selector.Sel.Name] || selector.Sel.Name == "Field" && fromComparison(selector.X)) {
					found = true
				}
			case *ast.Ident:
				if o := object(n); o != nil && derived[o] && !other[o] {
					found = true
				}
			}
			return !found
		})
		return found
	}
	assign := func(lhs []ast.Expr, rhs []ast.Expr) {
		for index, target := range lhs {
			ident, ok := target.(*ast.Ident)
			if !ok || ident.Name == "_" {
				continue
			}
			o := object(ident)
			if o == nil {
				continue
			}
			switch {
			case len(rhs) == len(lhs) && fromComparison(rhs[index]),
				len(rhs) == 1 && index == 0 && fromComparison(rhs[0]):
				derived[o] = true
			case len(rhs) == 1 && index > 0:
				// The second result of a comparison or Field call is its error.
			default:
				other[o] = true
			}
		}
	}
	// Repeat until no variable changes: values flow through earlier assignments.
	for {
		before := len(derived) + len(other)
		ast.Inspect(body, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.AssignStmt:
				assign(n.Lhs, n.Rhs)
			case *ast.ValueSpec:
				if len(n.Values) > 0 {
					lhs := make([]ast.Expr, len(n.Names))
					for i, name := range n.Names {
						lhs[i] = name
					}
					assign(lhs, n.Values)
				}
			}
			return true
		})
		if len(derived)+len(other) == before {
			break
		}
	}
	key, found := "", false
	ast.Inspect(body, func(node ast.Node) bool {
		var operands []ast.Expr
		switch n := node.(type) {
		case *ast.CallExpr:
			if selector, ok := n.Fun.(*ast.SelectorExpr); ok {
				if ident, ok := selector.X.(*ast.Ident); ok {
					if name, ok := object(ident).(*types.PkgName); ok && messagePackages[name.Imported().Path()] {
						return true
					}
				}
			}
			operands = append(append(operands, n.Fun), n.Args...)
		case *ast.IndexExpr:
			operands = []ast.Expr{n.X, n.Index}
		default:
			return !found
		}
		literal, comparison := "", false
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
			comparison = comparison || fromComparison(operand)
		}
		if literal != "" && comparison {
			key, found = literal, true
		}
		return !found
	})
	return key, found
}
