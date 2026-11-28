package runtime

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"
)

// loopWaitAllowlist names the Controller methods that may wait on a
// responder loop's locks, each because it never runs inside an observation
// pass: do not add to it without proving that.
var loopWaitAllowlist = map[string]string{
	// Runs in the closure-evidence goroutine, off the step loop.
	"gatherClosureEvidence:SettleRecoveryApplications": "off the step loop",
	// Closes the working programs of a loop this call just built and failed to restore; nothing else holds it.
	"restoreAutomaticLoop:CloseWorkingPrograms": "loop not yet shared",
}

// TestControllerNeverWaitsOnAResponderLoopLock rejects a Controller method
// that blocks on a responder loop's mutexes, directly (loop.mu.Lock,
// loop.operationMu.Lock or RLock, loop.locked) or through a ResponderLoop
// method that does. A loop holds them for a whole tool call: the automatic
// stage's or the agent's apply runs a script or polls a rollout for minutes
// under them. An observation pass that waited there stopped every detector
// for the apply's length. Controller code uses TryLock and treats a busy loop
// as such (observeMutations, acquireClosureGuard, mayHavePlanWrites).
func TestControllerNeverWaitsOnAResponderLoopLock(t *testing.T) {
	fset := token.NewFileSet()
	paths, err := globAll("*.go")
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
	// The ResponderLoop methods that block on its own locks.
	blocking := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverType(fn) != "ResponderLoop" || len(fn.Recv.List[0].Names) == 0 {
				continue
			}
			recv := fn.Recv.List[0].Names[0].Name
			if waitsInBody(fn.Body, func(base ast.Expr) bool { return isIdent(base, recv) }) {
				blocking[fn.Name.Name] = true
			}
		}
	}
	var found []string
	seen := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || receiverType(fn) != "Controller" {
				continue
			}
			guarded := triesOperation(fn.Body, isLoopExpr)
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				name := ""
				switch {
				case waitsOnLoopLock(call, isLoopExpr, guarded):
					name = sel.Sel.Name
				case blocking[sel.Sel.Name] && isLoopExpr(sel.X):
					name = sel.Sel.Name
				}
				if name != "" {
					seen[fn.Name.Name+":"+name] = true
					if _, ok := loopWaitAllowlist[fn.Name.Name+":"+name]; !ok {
						found = append(found, fmt.Sprintf("%s: %s waits on a responder loop lock via %s", fset.Position(call.Pos()), fn.Name.Name, name))
					}
				}
				return true
			})
		}
	}
	for key := range loopWaitAllowlist {
		if !seen[key] {
			found = append(found, "stale loopWaitAllowlist entry "+key)
		}
	}
	sort.Strings(found)
	if len(found) > 0 {
		t.Fatalf("controller code must not wait behind a responder loop's tool call (use TryLock):\n%s", strings.Join(found, "\n"))
	}
}

func receiverType(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
		if id, ok := star.X.(*ast.Ident); ok {
			return id.Name
		}
	}
	return ""
}

// isLoopExpr reports an expression that names a responder loop by this
// package's naming: a variable loop, or a field loop, Loop or responderLoop.
// The check is syntactic; the synctest tests over a held apply are the proof.
func isLoopExpr(expr ast.Expr) bool {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name == "loop"
	case *ast.SelectorExpr:
		return e.Sel.Name == "loop" || e.Sel.Name == "Loop" || e.Sel.Name == "responderLoop"
	}
	return false
}

// TestPlanJournalHasOneWriter keeps the journaled count controller
// observation reads (hasJournaledPlans) equal to the journal: every
// assignment to applicationsLog goes through setApplicationsLog.
func TestPlanJournalHasOneWriter(t *testing.T) {
	fset := token.NewFileSet()
	paths, err := globAll("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var found []string
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "setApplicationsLog" {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				assign, ok := node.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for _, lhs := range assign.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && sel.Sel.Name == "applicationsLog" {
						found = append(found, fmt.Sprintf("%s: %s assigns applicationsLog", fset.Position(assign.Pos()), fn.Name.Name))
					}
				}
				return true
			})
		}
	}
	if len(found) > 0 {
		t.Fatalf("use setApplicationsLog:\n%s", strings.Join(found, "\n"))
	}
}

// waitsInBody reports a body that waits on a loop lock of a base the
// predicate accepts. Closures count: a loop method's closure runs within it.
func waitsInBody(body *ast.BlockStmt, loopBase func(ast.Expr) bool) bool {
	guarded, waits := triesOperation(body, loopBase), false
	ast.Inspect(body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && waitsOnLoopLock(call, loopBase, guarded) {
			waits = true
		}
		return !waits
	})
	return waits
}

// triesOperation reports base.operationMu.TryLock() or TryRLock() in body.
// Only a tool call holds the loop's mu for long, and it holds operationMu
// exclusively throughout, so mu taken behind a successful try never waits
// on one.
func triesOperation(body *ast.BlockStmt, loopBase func(ast.Expr) bool) bool {
	tries := false
	ast.Inspect(body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && (sel.Sel.Name == "TryLock" || sel.Sel.Name == "TryRLock") {
				if mu, ok := sel.X.(*ast.SelectorExpr); ok && mu.Sel.Name == "operationMu" && loopBase(mu.X) {
					tries = true
				}
			}
		}
		return !tries
	})
	return tries
}

// waitsOnLoopLock reports base.locked(...) or base.mu.Lock() (unless guarded
// by a tried operationMu), or base.operationMu.Lock()/RLock(), for a base the
// predicate accepts.
func waitsOnLoopLock(call *ast.CallExpr, loopBase func(ast.Expr) bool, guarded bool) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if sel.Sel.Name == "locked" {
		return loopBase(sel.X) && !guarded
	}
	mu, ok := sel.X.(*ast.SelectorExpr)
	if !ok || !loopBase(mu.X) {
		return false
	}
	switch mu.Sel.Name {
	case "mu":
		return sel.Sel.Name == "Lock" && !guarded
	case "operationMu":
		return sel.Sel.Name == "Lock" || sel.Sel.Name == "RLock"
	}
	return false
}
