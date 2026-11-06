package operatorcli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"strings"
	"testing"

	"sdo.dev/operator/planfile"
)

func memoryCheckEnv(t *testing.T) map[string]string {
	t.Helper()
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	return map[string]string{"SDO_ACTOR": "responder", "SDO_PHASE": "reflection", "SDO_BASE_COMMIT": "HEAD", "SDO_OPERATOR_ROOT": operatorRoot}
}

func runMemory(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := RunMemory(context.Background(), args, func(key string) string { return env[key] }, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestMemoryCheckNoFlagsFailsClosedWithoutAcceptanceInputs(t *testing.T) {
	for _, env := range []map[string]string{
		nil,
		{"SDO_ACTOR": "responder", "SDO_PHASE": "reflection"},
		{"SDO_BASE_COMMIT": "HEAD", "SDO_ACTOR": "responder"},
		{"SDO_BASE_COMMIT": "HEAD", "SDO_PHASE": "reflection"},
	} {
		code, _, stderr := runMemory(t, env, "check")
		if code != 2 || stderr == "" {
			t.Fatalf("missing inputs accepted: env=%v code=%d stderr=%s", env, code, stderr)
		}
	}
}

func TestMemoryListCountsProgramPackages(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{
		".sdo/schema-version":            "5\n",
		".sdo/playbooks/fix/README.md":   "---\nschema_version: 1\nowner: responder\nfault_class: fix\noriginating_incident: synthetic\n---\n# Fix\n",
		".sdo/playbooks/fix/diagnose.go": "package fix\n",
		".sdo/playbooks/fix/mitigate.go": "package fix\n",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	code, stdout, stderr := runMemory(t, nil, "ls", "--root", root, "--kind", "program", "--json")
	listing := &contracts.MemoryListing{}
	if err := wire.UnmarshalDurable([]byte(stdout), listing); err != nil {
		t.Fatal(err)
	}
	if code != 0 || len(listing.Artifacts) != 1 || listing.Artifacts[0].Kind != "program" {
		t.Fatalf("program listing: %d %s %s", code, stdout, stderr)
	}
}

func TestPlanCatalogsDecodeTheRealRegistries(t *testing.T) {
	plan := func(renderer string) []byte {
		return []byte(`{"source":{"rendererId":"` + renderer + `","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["m"]}},"images":[{"image":"r.invalid/a:1","sourceId":"buildkit","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.BuildKitImageConfig","contextPath":"."}}]}`)
	}
	if _, err := planfile.Parse(plan("raw"), planCatalogs()); err != nil {
		t.Fatal(err)
	}
	if _, err := planfile.Parse(plan("jsonnet"), planCatalogs()); err == nil || !strings.Contains(err.Error(), "unknown renderer") {
		t.Fatalf("err = %v", err)
	}
}
