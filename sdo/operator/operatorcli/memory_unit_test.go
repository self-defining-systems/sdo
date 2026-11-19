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

	"sdo.dev/operator/memory/validate"
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
		".sdo/schema-version":            "13\n",
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

func TestReflectionRepairVerifiedEnvironment(t *testing.T) {
	for value, want := range map[string]bool{"true": true, "false": false} {
		verify := reflectionRepairVerifier(func(string) string { return value })
		if verify == nil {
			t.Fatalf("SDO_REPAIR_VERIFIED=%s: no verifier", value)
		}
		if got := verify(nil); got != want {
			t.Errorf("SDO_REPAIR_VERIFIED=%s: verified = %v, want %v", value, got, want)
		}
	}
	if reflectionRepairVerifier(func(string) string { return "" }) != nil {
		t.Error("an unset fact must leave the check's default")
	}
}

func TestMemoryTestTakesExactlyOnePlaybookID(t *testing.T) {
	for _, args := range [][]string{{"test"}, {"test", "a", "b"}, {"test", "Not A Playbook"}} {
		var stdout, stderr bytes.Buffer
		if code := RunMemory(context.Background(), args, func(string) string { return "" }, &stdout, &stderr); code != exitInfra {
			t.Errorf("memory %v exit = %d, want %d (stderr %q)", args, code, exitInfra, stderr.String())
		}
	}
}

func TestMemoryRootDefaultsToTheCheckoutContainingTheCurrentDirectory(t *testing.T) {
	root := t.TempDir()
	for name, body := range map[string]string{
		".git/HEAD":                      "ref: refs/heads/main\n",
		".sdo/schema-version":            "13\n",
		".sdo/playbooks/fix/README.md":   "---\nschema_version: 1\nowner: responder\nfault_class: fix\noriginating_incident: synthetic\n---\n# Fix\n",
		".sdo/playbooks/fix/diagnose.go": "package fix\n",
	} {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(filepath.Join(root, ".sdo", "playbooks", "fix"))
	code, stdout, stderr := runMemory(t, nil, "ls", "--json")
	listing := &contracts.MemoryListing{}
	if err := wire.UnmarshalDurable([]byte(stdout), listing); err != nil || code != 0 || len(listing.Artifacts) == 0 {
		t.Fatalf("ls from a subdirectory: %d %v %s %s", code, err, stdout, stderr)
	}
}

func TestMemoryCheckOutsideAnyCheckoutNamesTheFix(t *testing.T) {
	t.Chdir(t.TempDir())
	code, _, stderr := runMemory(t, memoryCheckEnv(t), "check")
	if code != 2 || !strings.Contains(stderr, "pass --root") {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
}

// "memory test" says, first, what the playbook's Diagnose did on the incident it was written for: a definite
// failure blocks, anything the capture cannot decide is shown and does not.
func TestMemoryTestPrintsTheOwnIncidentVerdict(t *testing.T) {
	report := &contracts.MemoryCheckReport{
		CoverageGaps: []*contracts.MemoryDiagnostic{
			{Rule: validate.RuleOwnIncidentReplay, Path: ".sdo/playbooks/seen", Message: "match: Diagnose matches the incident it was written for (open@detect.json)"},
			{Rule: validate.RuleOwnIncidentReplay, Path: ".sdo/playbooks/blind", Message: "unverified: Diagnose returned Unknown: unrecorded logs call"},
			{Rule: validate.RuleReplayObservation, Path: ".sdo/playbooks/seen", Message: "unrelated"},
		},
		Diagnostics: []*contracts.MemoryDiagnostic{
			{Rule: validate.RuleOwnIncidentReplay, Path: ".sdo/playbooks/wrong", Message: "Diagnose returned NoMatch on the incident it was written for"},
		},
	}
	for id, want := range map[string]string{
		"seen":    "  diagnose on incident: match: Diagnose matches the incident it was written for (open@detect.json)",
		"blind":   "  diagnose on incident: unverified: Diagnose returned Unknown: unrecorded logs call",
		"wrong":   "  diagnose on incident: Diagnose returned NoMatch on the incident it was written for (blocks publication)",
		"another": "",
	} {
		got := ownIncidentLines(report, id)
		if want == "" && len(got) != 0 || want != "" && (len(got) != 1 || got[0] != want) {
			t.Fatalf("%s: lines = %q, want %q", id, got, want)
		}
	}
}
