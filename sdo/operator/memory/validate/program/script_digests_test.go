package program

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunScriptProvenanceDoesNotDependOnReplayVerdicts(t *testing.T) {
	const script = `kubectl exec --namespace "$NAMESPACE" "$POD" --container "$CONTAINER" -- bash "$MOUNT/$ITEM"`
	for _, mode := range []string{"literal", "type alias", "alias changed digest", "no snapshots", "constant", "constant expression", "changed digest", "missing receipt", "dynamic text"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			fixture := "../../../../../tests/fixtures/sdo/hotel_run_script_upgrade/.sdo"
			if err := os.CopyFS(root, os.DirFS(fixture)); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "playbooks/mongo-auth-bootstrap/mitigate.go")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			switch mode {
			case "type alias", "alias changed digest":
				text = strings.ReplaceAll(text, "program.RunScript{", "ScriptStep{") + "\ntype ScriptStep = program.RunScript\n"
				if mode == "alias changed digest" {
					text = strings.Replace(text, script, script+"\n", 1)
				}
			case "no snapshots":
				for _, name := range []string{"open.json", "fixed.json", "healthy.json"} {
					if err := os.Remove(filepath.Join(root, "corpus/mongo-auth", name)); err != nil {
						t.Fatal(err)
					}
				}
			case "constant":
				text = strings.Replace(text, "`"+script+"`", "savedScript", 1) + "\nconst savedScript = `" + script + "`\n"
			case "constant expression":
				text = strings.Replace(text, "`"+script+"`", "savedScript", 1) + fmt.Sprintf("\nconst savedScript = %q + %q\n", script[:20], script[20:])
			case "changed digest":
				text = strings.Replace(text, script, script+"\n", 1)
			case "dynamic text":
				text = strings.Replace(text, "`"+script+"`", "string([]byte(`"+script+"`))", 1)
			case "missing receipt":
				if err := os.WriteFile(filepath.Join(root, "corpus/mongo-auth/recordings.json"), []byte("[]"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(path, []byte(text), 0644); err != nil {
				t.Fatal(err)
			}
			// The inspector never runs Diagnose or Mitigate: only the authored script
			// bytes, the captured linkage and its timed successful receipt matter.
			_, err = InspectMemory(root, "fixture-diagnostics")
			wantError := mode == "alias changed digest" || mode == "changed digest" || mode == "missing receipt" || mode == "dynamic text"
			if (err != nil) != wantError {
				t.Fatalf("mode=%s err=%v", mode, err)
			}
		})
	}
}
