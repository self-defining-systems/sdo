package builder

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// compiledTelemetrySources returns the string constant the memory host
// reports in Ready, read back from the generated source.
func compiledTelemetrySources(t *testing.T, root string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "generated", "telemetry_sources.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range general.Specs {
			if value, ok := spec.(*ast.ValueSpec); ok && value.Names[0].Name == "TelemetrySources" {
				text, err := strconv.Unquote(value.Values[0].(*ast.BasicLit).Value)
				if err != nil {
					t.Fatal(err)
				}
				return text
			}
		}
	}
	t.Fatal("generated/telemetry_sources.go has no TelemetrySources constant")
	return ""
}

func TestAcceptedTelemetrySourcesAreCompiledIntoTheHostByteForByte(t *testing.T) {
	diagnostics, root := t.TempDir(), t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(diagnostics, "telemetry"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Quotes, a backslash and a trailing newline survive the Go string literal.
	want := "version: 1\nsources:\n  - id: prom\n    kinds: [metrics]\n    adapter: prometheus\n    endpoint: \"http://p.sdo-ctl.svc:9090\" # a\\b\n"
	if err := os.WriteFile(filepath.Join(diagnostics, "telemetry", "sources.yaml"), []byte(want), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := generateTelemetrySources(diagnostics, root); err != nil {
		t.Fatal(err)
	}
	if got := compiledTelemetrySources(t, root); got != want {
		t.Fatalf("compiled sources = %q, want %q", got, want)
	}
}

func TestRevisionWithoutTelemetrySourcesCompilesAnEmptyConstant(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "generated"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := generateTelemetrySources(t.TempDir(), root); err != nil {
		t.Fatal(err)
	}
	if got := compiledTelemetrySources(t, root); got != "" {
		t.Fatalf("compiled sources = %q, want none", got)
	}
}
