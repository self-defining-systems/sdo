package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// directInformerOwners lists the test files that may still construct an
// informer owner without the leak-guarding helpers, each with the reason. The
// list may only shrink: a file that no longer needs its entry must lose it.
var directInformerOwners = map[string]string{
	"informer_goroutines_test.go": "asserts goroutine counts itself at exact points inside failed Start/Resync",
}

// ownerConstructors build the runtime's informer owners.
var ownerConstructors = map[string]bool{
	"NewStateTracker":    true,
	"NewKubernetesCache": true,
}

// rawInformerConstructors build client-go informers. A test may build them to
// seed a store; only one that also starts them (Start or Run with a stop channel) owns goroutines.
var rawInformerConstructors = map[string]bool{
	"NewSharedInformerFactory":                true,
	"NewSharedInformerFactoryWithOptions":     true,
	"NewFilteredSharedInformerFactory":        true,
	"NewSharedIndexInformer":                  true,
	"NewSharedIndexInformerWithOptions":       true,
	"NewInformer":                             true,
	"NewIndexerInformer":                      true,
	"NewDynamicSharedInformerFactory":         true,
	"NewFilteredDynamicSharedInformerFactory": true,
}

func directInformerCalls(t *testing.T) map[string][]string {
	t.Helper()
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string][]string{}
	for _, file := range files {
		if strings.HasPrefix(file, "leak_guard") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var owners, raw []string
		starts := false
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := ""
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				name = fn.Name
			case *ast.SelectorExpr:
				name = fn.Sel.Name
				starts = starts || ((name == "Start" || name == "Run") && len(call.Args) == 1 && isStopChannel(call.Args[0]))
			}
			if ownerConstructors[name] {
				owners = append(owners, name)
			}
			if rawInformerConstructors[name] {
				raw = append(raw, name)
			}
			return true
		})
		found[file] = owners
		if starts {
			found[file] = append(found[file], raw...)
		}
		if len(found[file]) == 0 {
			delete(found, file)
		}
	}
	return found
}

// Every test that owns informers goes through newTrackerForTest or
// newCacheForTest, so a leaked reflector fails the test that leaked it.
func TestInformerOwnersUseLeakGuardedConstructors(t *testing.T) {
	found := directInformerCalls(t)
	var files []string
	for file := range found {
		files = append(files, file)
	}
	sort.Strings(files)
	for _, file := range files {
		if _, allowed := directInformerOwners[file]; !allowed {
			t.Errorf("%s calls %v directly; use newTrackerForTest or newCacheForTest so a leaked informer fails this test by name", file, found[file])
		}
	}
	for file, reason := range directInformerOwners {
		if len(found[file]) == 0 {
			t.Errorf("%s no longer constructs informer owners directly; remove its allowlist entry (%s)", file, reason)
		}
	}
}

// isStopChannel matches the stop-channel argument of client-go's factory
// Start and informer Run: ctx.Done() or a variable named like a stop channel.
func isStopChannel(arg ast.Expr) bool {
	switch expr := arg.(type) {
	case *ast.CallExpr:
		selector, ok := expr.Fun.(*ast.SelectorExpr)
		return ok && selector.Sel.Name == "Done"
	case *ast.Ident:
		return strings.Contains(strings.ToLower(expr.Name), "stop")
	}
	return false
}
