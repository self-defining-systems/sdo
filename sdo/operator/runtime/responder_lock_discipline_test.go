package runtime

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// responderUnlockHelpers are the only functions that may release a responder
// loop mutex outside a defer: each relocks in a defer and types a panic.
var responderUnlockHelpers = map[string]bool{"withoutResponderLocks": true, "acquireClosureGuard": true}

// TestResponderLoopLocksAreDeferredOrHelped keeps every responder loop lock
// window panic-safe. A manual Unlock/relock pair leaves the mutex unlocked when
// the work between them panics, and Handle's deferred Unlock then aborts the
// process; a Lock without a defer leaves it held and the next locker hangs.
func TestResponderLoopLocksAreDeferredOrHelped(t *testing.T) {
	fset := token.NewFileSet()
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
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
		files = append(files, file)
	}
	// Only local types matter; unresolved imports are reported and ignored.
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{}}
	config := types.Config{Importer: failingImporter{}, Error: func(error) {}}
	_, _ = config.Check("runtime", fset, files, info)
	loopType := func(expr ast.Expr) bool {
		typ := info.TypeOf(expr)
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = pointer.Elem()
		}
		named, ok := typ.(*types.Named)
		return ok && named.Obj().Name() == "ResponderLoop"
	}
	// loopMutexCall returns "mu.Lock" style names for a call on a loop mutex.
	loopMutexCall := func(node ast.Node) (string, string, bool) {
		statement, ok := node.(*ast.ExprStmt)
		if !ok {
			return "", "", false
		}
		call, ok := statement.X.(*ast.CallExpr)
		if !ok {
			return "", "", false
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || (method.Sel.Name != "Lock" && method.Sel.Name != "Unlock") {
			return "", "", false
		}
		field, ok := method.X.(*ast.SelectorExpr)
		if !ok || (field.Sel.Name != "mu" && field.Sel.Name != "operationMu") || !loopType(field.X) {
			return "", "", false
		}
		return types.ExprString(field), method.Sel.Name, true
	}
	var violations []string
	for _, file := range files {
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok || function.Body == nil || responderUnlockHelpers[function.Name.Name] {
				continue
			}
			ast.Inspect(function.Body, func(node ast.Node) bool {
				if _, deferred := node.(*ast.DeferStmt); deferred {
					return false
				}
				block, ok := node.(*ast.BlockStmt)
				if !ok {
					return true
				}
				for index, statement := range block.List {
					mutex, method, ok := loopMutexCall(statement)
					if !ok {
						continue
					}
					position := fset.Position(statement.Pos())
					if method == "Unlock" {
						violations = append(violations, position.String()+": "+mutex+".Unlock() outside a defer; use withoutResponderLocks")
						continue
					}
					next := ""
					if index+1 < len(block.List) {
						if deferred, ok := block.List[index+1].(*ast.DeferStmt); ok {
							next = types.ExprString(deferred.Call.Fun)
						}
					}
					if next != mutex+".Unlock" {
						violations = append(violations, position.String()+": "+mutex+".Lock() not followed by defer "+mutex+".Unlock()")
					}
				}
				return true
			})
		}
	}
	if len(violations) > 0 {
		t.Fatalf("responder loop lock windows are not panic-safe:\n%s", strings.Join(violations, "\n"))
	}
}

type failingImporter struct{}

func (failingImporter) Import(string) (*types.Package, error) {
	return nil, errors.New("imports are not type-checked")
}

// A panicking closure check fails that verify call and leaves the loop usable;
// before, Handle's deferred Unlock hit an already released mutex and aborted.
func TestHandleVerifyPanicFailsOnlyThatCall(t *testing.T) {
	loop := NewResponderLoop(IncidentRequest{IncidentID: "incident"}, nil, nil, nil, PlanExecutor{}, 3)
	loop.VerifyClosure = func(context.Context) ToolResponse { panic("observer bug") }
	response := loop.Handle(context.Background(), ToolRequest{Command: "verify"})
	if !strings.Contains(response.Error, "observer bug") {
		t.Fatalf("verify panic was not reported: %+v", response)
	}
	if !loop.mu.TryLock() || !loop.operationMu.TryLock() {
		t.Fatal("verify panic left the loop locked")
	}
}
