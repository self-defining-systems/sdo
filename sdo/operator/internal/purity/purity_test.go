package purity

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStructuralPurityRejectsConcurrencyAndGlobals(t *testing.T) {
	for _, code := range []string{
		`var cache = map[string]int{}`,
		`var now func() int`,
		`type Input struct { Done <-chan bool }`,
		`func step() { go func() {}() }`,
		`func init() {}`,
	} {
		t.Run(code, func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "core.go", "package core\n"+code, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := inspectSyntax(fset, file); len(got) == 0 {
				t.Fatalf("accepted impure core: %s", code)
			}
		})
	}
}

func TestStructuralPurityAllowsLocalMutationAndEventTime(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "core.go", `package core
import "time"
const Ready = 1
type Event struct { At time.Time }
func step(event Event) time.Time { var next = event.At; return next.Add(time.Second) }
`, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := inspectSyntax(fset, file); len(got) > 0 {
		t.Fatal(strings.Join(got, "\n"))
	}
}

func TestDeclaredCorePackages(t *testing.T) {
	root := filepath.Join("..", "..", "..", "..")
	manifest, err := os.ReadFile(filepath.Join(root, "scripts", "go_pure_packages.txt"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range strings.Split(string(manifest), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if len(fields) != 2 {
			t.Fatalf("invalid pure package declaration: %q", line)
		}
		count++
		t.Run(fields[0]+"/"+fields[1], func(t *testing.T) {
			if got := Check(filepath.Join(root, fields[0]), fields[1]); len(got) > 0 {
				t.Fatal(strings.Join(got, "\n"))
			}
		})
	}
	if count == 0 {
		t.Fatal("no pure packages declared")
	}
}

func TestPurityRejectsTimeAndIOAliases(t *testing.T) {
	for _, code := range []string{
		`import clock "time"; func step() { _ = clock.Now() }`,
		`import "net/http"; func step() { _ = http.MethodGet }`,
		`import "os/exec"; func step() { _ = exec.ErrNotFound }`,
		`import "math/rand/v2"; func step() { _ = rand.Int() }`,
		`import "sdo.dev/operator/runtime/internal/lifecycleio"`,
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "core.go", "package core\n"+code, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := inspectSyntax(token.NewFileSet(), file); len(got) == 0 {
			t.Fatalf("accepted impure source: %s", code)
		}
	}
}

func TestPurityChecksApplicationDependenciesTransitively(t *testing.T) {
	got := Check("../..", "./internal/purity/testdata/indirect")
	if !strings.Contains(strings.Join(got, "\n"), "wrapper.go") {
		t.Fatalf("hidden edge import passed: %v", got)
	}
}

func TestPurityRejectsMissingPackages(t *testing.T) {
	if got := Check("../..", "./internal/purity/testdata/missing"); len(got) == 0 {
		t.Fatal("missing declared core silently passed")
	}
}
