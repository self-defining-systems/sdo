package determinism

import (
	"go/parser"
	"go/token"
	"testing"
)

func TestForbiddenEdges(t *testing.T) {
	for _, src := range []string{
		`import clock "time"; var f = clock.Now`,
		`import "time"; var f = time.Sleep`,
		`import "time"; var f = time.After`,
		`import "time"; var f = time.NewTimer`,
		`import "time"; var f = time.NewTicker`,
		`import "time"; var f = time.Tick`,
		`import "time"; var f = time.Since`,
		`import "time"; var f = time.Until`,
		`import "time"; var f = time.AfterFunc`,
		`import "math/rand"; var f = rand.Int`,
		`import "math/rand/v2"; var f = rand.Int`,
		`import "crypto/rand"; var f = rand.Read`,
		`import "net"; var f = net.Dial`,
		`import "net"; var f = net.Listen`,
		`import "crypto/tls"; var f = tls.Dial`,
		`import "crypto/tls"; var f = tls.Listen`,
		`import "net"; var f = net.Dialer{}`,
		`import "net/http"; var f = http.Get`,
		`import "net/http"; var f = http.Client{}`,
		`import "net/http"; var f = http.DefaultClient`,
		`import "os/exec"; var f = exec.Command`,
		`import "os"; var f = os.Getenv`,
		`import "os"; var f = os.LookupEnv`,
		`import "os"; var f = os.Environ`,
		`import "net/http/httptest"; var f = httptest.NewServer`,
		`import . "time"; var f = Now`,
	} {
		t.Run(src, func(t *testing.T) {
			for _, name := range []string{"core.go", "core_test.go"} {
				if got := violations(t, name, src); len(got) == 0 {
					t.Fatalf("accepted %s: %s", name, src)
				}
			}
		})
	}
}

func violations(t *testing.T, name, src string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), name, "package fixture\n"+src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Inspect(name, file)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestDeterministicAlternatives(t *testing.T) {
	for _, src := range []string{
		`import "net"; var f = net.Pipe`,
		`import "net/http/httptest"; var f = httptest.NewRecorder`,
		`import "time"; var f = time.Date`,
		`import "math/rand"; var f = rand.New(rand.NewSource(42))`,
		`import "testing/synctest"; var f = synctest.Test`,
		`import ("testing/synctest"; "time"); func test() { synctest.Test(nil, func() { time.Sleep(time.Second); _ = time.Now() }) }`,
	} {
		if got := violations(t, "core_test.go", src); len(got) != 0 {
			t.Fatal(got)
		}
	}
}

func TestIntegrationMustBeRequiredByTag(t *testing.T) {
	for _, tag := range []string{"integration", "integration && linux"} {
		file, err := parser.ParseFile(token.NewFileSet(), "edge_test.go", "//go:build "+tag+"\n\npackage edge\nimport \"time\"\nvar f = time.Now", parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Inspect("edge_test.go", file)
		if err != nil || len(got) != 0 {
			t.Fatalf("%v %v", got, err)
		}
	}
	file, _ := parser.ParseFile(token.NewFileSet(), "edge_test.go", "//go:build integration || linux\n\npackage edge\nimport \"time\"\nvar f = time.Now", parser.ParseComments)
	if got, _ := Inspect("edge_test.go", file); len(got) == 0 {
		t.Fatal("optional integration tag bypassed rule")
	}
}

func TestRatchetRejectsGrowthAndStaleEntries(t *testing.T) {
	for _, tc := range []struct{ actual, allowed, committed []string }{
		{[]string{"new"}, nil, nil},
		{[]string{"new"}, []string{"new"}, []string{"old"}},
		{nil, []string{"old"}, []string{"old"}},
	} {
		if err := Compare(tc.actual, tc.allowed, tc.committed); err == nil {
			t.Fatal("ratchet accepted regression", tc)
		}
	}
	if err := Compare([]string{"a"}, []string{"a"}, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
}

func TestProductionEdgesDoNotExemptTestsOrChildren(t *testing.T) {
	src := `import "time"; var f = time.Now`
	edge := "sdo/operator/runtime/internal/lifecycleio/"
	if got := violations(t, edge+"driver.go", src); len(got) != 0 {
		t.Fatal(got)
	}
	for _, name := range []string{edge + "driver_test.go", edge + "child/driver.go"} {
		if got := violations(t, name, src); len(got) == 0 {
			t.Fatalf("unexpected exemption: %s", name)
		}
	}
}

func TestTrailingCommentCannotExemptTest(t *testing.T) {
	if got := violations(t, "core_test.go", "import \"time\"; var f = time.Now\n//go:build integration\n"); len(got) == 0 {
		t.Fatal("trailing tag bypassed test rule")
	}
}
