package builder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGeneratorImportErrorNamesAllowedAlternatives(t *testing.T) {
	for _, name := range []string{"math/rand", "net/http"} {
		t.Run(name, func(t *testing.T) {
			base := t.TempDir()
			dir := filepath.Join(base, "generators")
			if err := os.Mkdir(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "bad.go"), []byte("package generators\nimport \""+name+"\"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			err := validateGeneratorSources(base, "app.dev/diagnostics")
			if err == nil || !strings.Contains(err.Error(), map[string]string{"math/rand": "math/rand/v2", "net/http": "through the engine"}[name]) {
				t.Fatalf("import diagnostic: %v", err)
			}
		})
	}
}

func writeGenerator(t *testing.T, files map[string]string) string {
	t.Helper()
	base := t.TempDir()
	dir := filepath.Join(base, "generators")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return base
}

// Ordinary deterministic, I/O-free standard library is allowed in probes.
func TestGeneratorsMayUseOrdinaryStandardLibrary(t *testing.T) {
	body := "package generators\nimport (\n\t\"image/png\"\n\t\"reflect\"\n\t\"sync\"\n\t\"text/template\"\n\t\"hash/fnv\"\n\t\"math/bits\"\n\t\"path\"\n\t\"net/textproto\"\n\t\"cmp\"\n)\n" +
		"var _ = png.Decode\nvar _ = reflect.TypeOf\nvar _ sync.Mutex\nvar _ = template.New\nvar _ = fnv.New32\nvar _ = bits.Len\nvar _ = path.Join\nvar _ = textproto.CanonicalMIMEHeaderKey\nvar _ = cmp.Compare[int]\n"
	if err := validateGeneratorSources(writeGenerator(t, map[string]string{"ok.go": body}), "app.dev/diagnostics"); err != nil {
		t.Fatal(err)
	}
}

// What breaks a probe is refused, and the error names the fix.
func TestGeneratorRuleErrorsNameTheFix(t *testing.T) {
	for name, tc := range map[string]struct{ body, want string }{
		"clock":       {"package generators\nimport \"time\"\nvar _ = time.Now()\n", "traffic.StateNow"},
		"global rand": {"package generators\nimport \"math/rand/v2\"\nvar _ = rand.IntN(3)\n", "rng"},
		"rand v1":     {"package generators\nimport \"math/rand\"\n", "*rand.Rand"},
		"socket":      {"package generators\nimport \"crypto/tls\"\n", "engine"},
		"files":       {"package generators\nimport \"os\"\n", "files"},
		"module":      {"package generators\nimport \"example.com/x\"\n", "standard library"},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateGeneratorSources(writeGenerator(t, map[string]string{"bad.go": tc.body}), "app.dev/diagnostics")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q in: %v", tc.want, err)
			}
		})
	}
	err := validateGeneratorSources(writeGenerator(t, map[string]string{"ok.go": "package generators\n", "raw.s": "TEXT ·x(SB),0,$0\n"}), "app.dev/diagnostics")
	if err == nil || !strings.Contains(err.Error(), "raw.s") {
		t.Fatalf("a non-Go build input must be refused: %v", err)
	}
}
