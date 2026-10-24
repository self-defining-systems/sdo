package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/builder"
)

func TestParseValidateLaunchRequiresBoundSchema(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	launch, err := ParseValidateLaunch([]string{"validate", "--app", "/workspace", "--operator-root", root, "--expect-schema", sdkSchemaIdentity})
	if err != nil {
		t.Fatal(err)
	}
	if launch.ApplicationRoot != "/workspace" || launch.OperatorRoot != root {
		t.Fatalf("unexpected validator launch: %+v", launch)
	}
	baseline, err := ParseValidateLaunch([]string{"validate", "--app", "/workspace", "--operator-root", root, "--expect-schema", sdkSchemaIdentity, "--healthy-baseline", "/sdo/healthy-baseline"})
	if err != nil || baseline.HealthyBaseline != "/sdo/healthy-baseline" {
		t.Fatalf("healthy baseline gate was lost: %+v, %v", baseline, err)
	}
	_, err = ParseValidateLaunch([]string{"validate", "--app", "/workspace", "--operator-root", root, "--expect-schema", "stale"})
	if err == nil || !strings.Contains(err.Error(), "stale validator image") {
		t.Fatalf("stale schema was accepted: %v", err)
	}
}

func TestIsolatedValidatorChecksProgramCorpus(t *testing.T) {
	app := t.TempDir()
	fixture := filepath.Join("..", "..", "..", "tests", "fixtures", "sdo", "playbook_program_app")
	copy := exec.Command("cp", "-a", fixture+"/.", app)
	if output, err := copy.CombinedOutput(); err != nil {
		t.Fatalf("copy fixture: %v: %s", err, output)
	}
	path := filepath.Join(app, ".sdo", "playbooks", "service-targetport-mismatch", "mitigate.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// A plan that writes back the faulty port must fail actual repair replay.
	data = []byte(strings.Replace(string(data), "Value: replacementValue", "Value: program.FromObserved(ref, \"spec.ports[0].targetPort\")", 1))
	data = []byte(strings.Replace(string(data), "if !matched && replacement > 0 {", "if !matched && replacement > 0 { _ = replacementValue", 1))
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := RunValidate(context.Background(), builder.Config{ApplicationRoot: app, OperatorRoot: ".."}); err == nil || !strings.Contains(err.Error(), "program.mitigation-not-repaired") {
		t.Fatalf("ineffective mitigation accepted: %v", err)
	}

}

func TestValidateAllowsPreDiagnosticsBootstrapStage(t *testing.T) {
	app := t.TempDir()
	if err := os.MkdirAll(filepath.Join(app, ".sdo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(app, ".sdo", "goal.md"), []byte("# Goal\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := RunValidate(context.Background(), builder.Config{ApplicationRoot: app, OperatorRoot: ".."}); err != nil {
		t.Fatalf("initial goal stage cannot be validated: %v", err)
	}
}
