package operatorcli

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// verifyCandidateOwners are the only production files that call
// VerifyCandidate. Each must apply the designer's telemetry backends first: the
// Coordinator gate through its TelemetryApply phase, the advance and repair
// paths through lifecycle.TelemetryAppliedVerifier. A new call site fails here
// until it is listed, with its apply requirement stated in this test.
var verifyCandidateOwners = map[string]string{
	"lifecycle/gate.go":               "Coordinator.TelemetryApply",
	"lifecycle/advance.go":            "Advancer.Verifier is deploywire.AppliedVerifier",
	"lifecycle/repair.go":             "RepairDeployer.Verifier is deploywire.AppliedVerifier",
	"lifecycle/head.go":               "HeadDeployer.Deployer is a RepairDeployer whose Verifier is deploywire.AppliedVerifier",
	"deployment/health.go":            "the verifier itself",
	"lifecycle/telemetry_verifier.go": "the apply wrapper",
}

// Review 6 F5: the apply phase was wired in the operate Job only, so the
// controller's advance and refresh paths verified detectors against backends
// that were never deployed. This walks production code: every VerifyCandidate
// call is in a known owner, every lifecycle.Coordinator with a Verifier names
// TelemetryApply, and every Advancer or RepairDeployer takes its Verifier from
// deploywire.AppliedVerifier.
func TestEveryVerifyCandidatePathAppliesTelemetry(t *testing.T) {
	root := ".."
	var coordinators, others int
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
		rel := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator)))
		appliedCalls := 0
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok && isAppliedVerifierCall(call) {
				appliedCalls++
			}
			return true
		})
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "VerifyCandidate" {
					if _, ok := verifyCandidateOwners[rel]; !ok {
						t.Errorf("%s calls VerifyCandidate and is not a listed owner; state how it applies telemetry backends first", rel)
					}
				}
			case *ast.CompositeLit:
				switch {
				case literalOf(n, "lifecycle", "Coordinator", file.Name.Name) && keyValue(n, "Verifier") != nil:
					coordinators++
					if keyValue(n, "TelemetryApply") == nil {
						t.Errorf("%s: a lifecycle.Coordinator with a Verifier must set TelemetryApply", rel)
					}
				case literalOf(n, "lifecycle", "Advancer", file.Name.Name) || literalOf(n, "lifecycle", "RepairDeployer", file.Name.Name):
					if v := keyValue(n, "Verifier"); v != nil {
						others++
						if !fromAppliedVerifier(v) || appliedCalls == 0 {
							t.Errorf("%s: an Advancer/RepairDeployer Verifier must come from deploywire.AppliedVerifier (or lifecycle.TelemetryAppliedVerifier)", rel)
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
	if coordinators < 2 || others < 1 {
		t.Fatalf("saw %d coordinators and %d advancer/repair verifiers; the walk is not seeing production code", coordinators, others)
	}
}

func literalOf(n *ast.CompositeLit, pkg, name, inPkg string) bool {
	switch typ := n.Type.(type) {
	case *ast.SelectorExpr:
		x, ok := typ.X.(*ast.Ident)
		return ok && x.Name == pkg && typ.Sel.Name == name
	case *ast.Ident:
		return inPkg == pkg && typ.Name == name
	}
	return false
}

func keyValue(n *ast.CompositeLit, key string) ast.Expr {
	for _, e := range n.Elts {
		if pair, ok := e.(*ast.KeyValueExpr); ok {
			if id, ok := pair.Key.(*ast.Ident); ok && id.Name == key {
				return pair.Value
			}
		}
	}
	return nil
}

// fromAppliedVerifier accepts the local variable a construction site bound
// from AppliedVerifier (the file must call it: appliedCalls), or a direct
// TelemetryAppliedVerifier literal.
func fromAppliedVerifier(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name == "verifier"
	case *ast.CompositeLit:
		return literalOf(e, "lifecycle", "TelemetryAppliedVerifier", "")
	}
	return false
}

func isAppliedVerifierCall(call *ast.CallExpr) bool {
	switch f := call.Fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name == "AppliedVerifier"
	case *ast.Ident:
		return f.Name == "AppliedVerifier"
	}
	return false
}
