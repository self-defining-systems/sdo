package runtime

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

// Process-global state (klog verbosity and output) is read by every informer
// reflector in the process, so a test that changes it races with any
// goroutine another test left behind. Such a test runs alone in a child
// process of the test binary: nothing else can be running there.

const isolatedChildEnv = "SDO_TEST_ISOLATED_CHILD"

// inIsolatedProcess returns true in the child, where the test body must run.
// In the parent it runs this one test in a child process, fails t with the
// child's output if it fails, and returns false.
func inIsolatedProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv(isolatedChildEnv) == t.Name() {
		return true
	}
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.timeout=5m")
	command.Env = append(os.Environ(), isolatedChildEnv+"="+t.Name())
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("isolated child process failed: %v\n%s", err, output)
	}
	return false
}

// globalMutators are the klog calls that change process-wide logging state.
var globalMutators = map[string]bool{
	"CaptureState": true, "InitFlags": true, "SetOutput": true, "SetOutputBySeverity": true,
	"SetLogger": true, "SetLoggerWithOptions": true, "LogToStderr": true, "SetVerbosity": true,
}

// isolatedGlobalMutators lists the test files allowed to mutate klog state,
// each of whose tests must use inIsolatedProcess. It may only shrink.
var isolatedGlobalMutators = map[string]string{
	"credential_sources_test.go": "asserts credentials never reach verbose client logs",
}

func TestOnlyIsolatedTestsMutateProcessGlobalLogging(t *testing.T) {
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	used := map[string]bool{}
	for _, file := range files {
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Body == nil {
				continue
			}
			mutates, isolated := false, false
			ast.Inspect(function.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fn := call.Fun.(type) {
				case *ast.SelectorExpr:
					if pkg, ok := fn.X.(*ast.Ident); ok && pkg.Name == "klog" && globalMutators[fn.Sel.Name] {
						mutates = true
					}
				case *ast.Ident:
					isolated = isolated || fn.Name == "inIsolatedProcess"
				}
				return true
			})
			if !mutates {
				continue
			}
			used[file] = true
			if _, allowed := isolatedGlobalMutators[file]; !allowed || !isolated {
				t.Errorf("%s: %s changes process-global klog state; run it in a child process with inIsolatedProcess (allowlist: %v)", file, function.Name.Name, isolatedGlobalMutators)
			}
		}
	}
	var stale []string
	for file := range isolatedGlobalMutators {
		if !used[file] {
			stale = append(stale, file)
		}
	}
	sort.Strings(stale)
	for _, file := range stale {
		t.Errorf("%s no longer mutates klog state; remove its allowlist entry", file)
	}
}
