package runtime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// externalControllerFields are the Controller fields that hold collaborators
// the controller does not own: any of them may re-enter the controller (the
// production LoopFactory takes controller.mu), so calling one while c.mu is
// held can deadlock the step loop. Seen live as P57.
var externalControllerFields = map[string]bool{"dispatcher": true, "broker": true, "provider": true, "OnError": true, "OnResult": true, "OnEvaluation": true, "OnClosureFailed": true}

// externalMethods are dispatcher entry points that are reached through a local
// variable (a type-asserted interface), so the field check cannot see them.
var externalMethods = map[string]bool{"ContinueAfterVerification": true, "StopResponder": true, "PrepareDispatch": true, "IncidentWork": true, "Dispatch": true}

// heldCallAllowlist lists the calls that run under c.mu today. Each is
// non-blocking bookkeeping or an observer that does not call back into the
// controller; do not add to it without proving the callee never takes c.mu.
var heldCallAllowlist = map[string]bool{}

// TestControllerDoesNotCallCollaboratorsWhileHoldingItsLock walks every
// Controller method in straight-line order and rejects a call through an
// external collaborator between c.mu.Lock() and the matching c.mu.Unlock().
// Nested blocks inherit the state at their start and do not leak changes out.
func TestControllerDoesNotCallCollaboratorsWhileHoldingItsLock(t *testing.T) {
	fset := token.NewFileSet()
	paths, err := globAll("controller*.go", "early_closure.go", "stale_incident.go", "responder_continuation.go")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, path, source, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Recv == nil || len(fn.Recv.List) == 0 || len(fn.Recv.List[0].Names) == 0 {
				continue
			}
			recv := fn.Recv.List[0].Names[0].Name
			if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); !ok || !isIdent(star.X, "Controller") {
				continue
			}
			held := strings.HasSuffix(fn.Name.Name, "Locked")
			walkHeld(fn.Body, recv, held, func(call *ast.CallExpr) {
				if name, bad := externalCall(call, recv); bad {
					key := fn.Name.Name + ":" + name
					if !heldCallAllowlist[key] {
						found = append(found, fmt.Sprintf("%s: %s calls %s while holding %s.mu", fset.Position(call.Pos()), fn.Name.Name, name, recv))
					}
				}
			})
		}
	}
	sort.Strings(found)
	if len(found) > 0 {
		t.Fatalf("external collaborator called under the controller lock (P57 class):\n%s", strings.Join(found, "\n"))
	}
}

func isIdent(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

func globAll(patterns ...string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			if !seen[m] {
				seen[m] = true
				out = append(out, m)
			}
		}
	}
	return out, nil
}

// muCall reports "Lock" or "Unlock" for a statement `recv.mu.Lock()` / `recv.mu.Unlock()`.
func muCall(stmt ast.Stmt, recv string) string {
	expr, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return ""
	}
	call, ok := expr.X.(*ast.CallExpr)
	if !ok {
		return ""
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	mu, ok := sel.X.(*ast.SelectorExpr)
	if !ok || mu.Sel.Name != "mu" || !isIdent(mu.X, recv) {
		return ""
	}
	if sel.Sel.Name == "Lock" || sel.Sel.Name == "Unlock" {
		return sel.Sel.Name
	}
	return ""
}

func externalCall(call *ast.CallExpr, recv string) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	if externalMethods[sel.Sel.Name] {
		return sel.Sel.Name, true
	}
	for expr := ast.Expr(sel); ; {
		s, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}
		if isIdent(s.X, recv) && externalControllerFields[s.Sel.Name] {
			return s.Sel.Name + "." + sel.Sel.Name, true
		}
		expr = s.X
	}
}

// walkHeld visits calls in source order with the lock state of straight-line code.
func walkHeld(block *ast.BlockStmt, recv string, held bool, visit func(*ast.CallExpr)) {
	for _, stmt := range block.List {
		switch muCall(stmt, recv) {
		case "Lock":
			held = true
			continue
		case "Unlock":
			held = false
			continue
		}
		walkStmt(stmt, recv, held, visit)
	}
}

func walkStmt(stmt ast.Stmt, recv string, held bool, visit func(*ast.CallExpr)) {
	ast.Inspect(stmt, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.BlockStmt:
			if node != ast.Node(stmt) {
				walkHeld(n, recv, held, visit)
				return false
			}
		case *ast.FuncLit:
			// A closure runs later, under whatever lock state its caller has.
			return false
		case *ast.CallExpr:
			if held {
				visit(n)
			}
		}
		return true
	})
}
