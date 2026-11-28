//go:build integration

package builder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/builder/internal/buildio"
)

// The check's compile stage links nothing, so it must reject every compile error `go build ./...` rejects, with the
// same compiler report the diagnostics are parsed from: a library and a main package that do not compile, a missing
// import, an import cycle, and a clean module. (An error only the linker finds is rejected by the test stages, which
// link the generated package and with it every authored detector and program.)
func TestCompileAllReportsWhatGoBuildReports(t *testing.T) {
	for name, files := range map[string]map[string]string{
		"clean": {
			"lib/lib.go":    "package lib\n\nfunc F() int { return 1 }\n",
			"cmd/a/main.go": "package main\n\nimport \"example.invalid/m/lib\"\n\nfunc main() { _ = lib.F() }\n",
		},
		"library type error": {
			"lib/lib.go":    "package lib\n\nfunc F() int { return \"one\" }\n",
			"cmd/a/main.go": "package main\n\nimport \"example.invalid/m/lib\"\n\nfunc main() { _ = lib.F() }\n",
		},
		"main package error": {
			"lib/lib.go":    "package lib\n\nfunc F() int { return 1 }\n",
			"cmd/a/main.go": "package main\n\nfunc main() { x := 1 }\n",
		},
		"missing import": {
			"lib/lib.go": "package lib\n\nimport _ \"example.invalid/m/absent\"\n",
		},
		"import cycle": {
			"a/a.go": "package a\n\nimport _ \"example.invalid/m/b\"\n",
			"b/b.go": "package b\n\nimport _ \"example.invalid/m/a\"\n",
		},
		"errors in two packages": {
			"a/a.go": "package a\n\nvar X int = \"a\"\n",
			"b/b.go": "package b\n\nvar Y string = 2\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			files["go.mod"] = "module example.invalid/m\n\ngo 1.24\n"
			for path, content := range files {
				if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(path)), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, path), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			_, built := buildio.RunGo(context.Background(), dir, nil, "build", "-buildvcs=false", "./...")
			_, compiled := buildio.RunGo(context.Background(), dir, nil, compileAll...)
			if (built == nil) != (compiled == nil) {
				t.Fatalf("go build: %v\ncompile stage: %v", built, compiled)
			}
			if built == nil {
				return
			}
			var want, got *buildio.StageError
			if !errors.As(built, &want) || !errors.As(compiled, &got) {
				t.Fatalf("not stage errors: %v / %v", built, compiled)
			}
			// Packages compile in parallel, so the order of their reports varies between runs of either command;
			// the diagnostics are parsed per line.
			if !slices.Equal(sortedLines(got.Output), sortedLines(want.Output)) {
				t.Fatalf("compile stage reports\n%s\ngo build reports\n%s", got.Output, want.Output)
			}
		})
	}
}

func sortedLines(output string) []string {
	lines := strings.Split(output, "\n")
	slices.Sort(lines)
	return lines
}
