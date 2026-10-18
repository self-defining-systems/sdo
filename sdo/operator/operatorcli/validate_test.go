package operatorcli

import (
	"context"
	"os"
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
