package operatorcli

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
)

const goodFixture = "../memory/validate/testdata/good"

func copyFixture(t *testing.T, from string) string {
	t.Helper()
	to := t.TempDir()
	err := filepath.WalkDir(from, func(p string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(from, p)
		target := filepath.Join(to, rel)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
	gitRun(t, to, "init", "-q")
	gitRun(t, to, "config", "user.name", "T")
	gitRun(t, to, "config", "user.email", "t@example.com")
	gitRun(t, to, "add", "-A")
	gitRun(t, to, "commit", "-qm", "base")
	return to
}

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

func prepareCleanReflection(t *testing.T, root string) {
	t.Helper()
	outcomePath := filepath.Join(root, layout.OutcomePath("incident"))
	if err := os.MkdirAll(filepath.Dir(outcomePath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outcomePath, []byte(`{"schema_version":1,"incident_id":"incident","classification":"failed"}`), 0644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, root, "add", ".")
	gitRun(t, root, "commit", "--allow-empty", "-qm", "outcome\n\nSDO-Incident: incident\nSDO-Phase: outcome")
	if err := validate.WritePublicationContext(context.Background(), root, validate.PublicationContext{BaseCommit: "HEAD", IncidentID: "incident"}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, ".sdo/playbooks/base/README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/base/README.md"), append(data, []byte("\nVerified recovery.\n")...), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", validate.DecisionFile), []byte(`{"learning_decision":"updated","proposed_artifact_ids":["base"]}`), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryCheckCleanTreeExitsZero(t *testing.T) {
	root := copyFixture(t, goodFixture)
	prepareCleanReflection(t, root)
	env := memoryCheckEnv(t)
	t.Chdir(root)
	code, stdout, stderr := runMemory(t, env, "check")
	if code != 0 || stdout != "" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestMemoryCheckReportsDiagnosticsAsLines(t *testing.T) {
	root := copyFixture(t, goodFixture)
	if err := os.WriteFile(filepath.Join(root, ".sdo/notes.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, _ := runMemory(t, memoryCheckEnv(t), "check", "--root", root)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(stdout, ".sdo/notes.txt: layout.unknown-path: ") {
		t.Fatalf("stdout = %q", stdout)
	}
}

func TestMemoryCheckJSONIsMemoryCheckReport(t *testing.T) {
	root := copyFixture(t, goodFixture)
	if err := os.WriteFile(filepath.Join(root, ".sdo/schema-version"), []byte("9\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, _ := runMemory(t, memoryCheckEnv(t), "check", "--root", root, "--json")
	if code != 1 {
		t.Fatalf("code = %d", code)
	}
	report := &contracts.MemoryCheckReport{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(stdout), report); err != nil {
		t.Fatalf("not a MemoryCheckReport: %v\n%s", err, stdout)
	}
	for _, diagnostic := range report.Diagnostics {
		if diagnostic.Rule == validate.RuleSchemaVersion && diagnostic.Line == 1 {
			return
		}
	}
	t.Fatalf("schema diagnostic missing: %v", report.Diagnostics)
}

func TestMemoryCheckCleanJSONHasEmptyDiagnosticList(t *testing.T) {
	root := copyFixture(t, goodFixture)
	prepareCleanReflection(t, root)
	code, stdout, _ := runMemory(t, memoryCheckEnv(t), "check", "--root", root, "--json")
	if code != 0 || !regexp.MustCompile(`"diagnostics":\s+\[\]`).MatchString(stdout) {
		t.Fatalf("code=%d stdout=%s", code, stdout)
	}
}

func TestMemoryCheckInfrastructureErrorsExitTwo(t *testing.T) {
	root := copyFixture(t, goodFixture)
	for name, args := range map[string][]string{
		"missing root":           {"check", "--root", filepath.Join(root, "missing")},
		"ownership without base": {"check", "--root", root, "--layers", "ownership"},
		"unknown layer":          {"check", "--root", root, "--layers", "bogus"},
		"unknown flag":           {"check", "--nope"},
	} {
		if code, _, stderr := runMemory(t, nil, args...); code != 2 || stderr == "" {
			t.Errorf("%s: code=%d stderr=%q", name, code, stderr)
		}
	}
}

func gitRun(t *testing.T, root string, args ...string) {
	t.Helper()
	if out, err := exec.Command("git", append([]string{"-C", root}, args...)...).CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func TestMemoryCheckActorAndPhaseDefaultFromEnvironment(t *testing.T) {
	root := copyFixture(t, goodFixture)
	if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	gitRun(t, root, "init", "-q")
	gitRun(t, root, "config", "user.name", "T")
	gitRun(t, root, "config", "user.email", "t@example.com")
	gitRun(t, root, "add", "-A")
	gitRun(t, root, "commit", "-q", "-m", "base")
	if err := os.WriteFile(filepath.Join(root, ".sdo/goal.md"), []byte("---\nschema_version: 1\nowner: human\napplication: demo\n---\nChanged.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	env := memoryCheckEnv(t)
	code, stdout, stderr := runMemory(t, env, "check", "--root", root)
	if code != 1 || !strings.Contains(stdout, "ownership.denied") {
		t.Fatalf("env defaults: code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	env["SDO_ACTOR"], env["SDO_PHASE"] = "human", "initial"
	code, stdout, stderr = runMemory(t, env, "check", "--root", root)
	if code != 1 || !strings.Contains(stdout, validate.RuleBootstrapClosed) || strings.Contains(stdout, validate.RuleOwnershipDenied) {
		t.Fatalf("human ownership: code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
}

func TestMemoryListIdentifiesArtifactsByLogicalID(t *testing.T) {
	root := copyFixture(t, goodFixture)
	code, stdout, stderr := runMemory(t, nil, "ls", "--root", root, "--json")
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	listing := &contracts.MemoryListing{}
	if err := protojson.Unmarshal([]byte(stdout), listing); err != nil {
		t.Fatal(err)
	}
	type key struct{ kind, id string }
	got := map[key]string{}
	for _, artifact := range listing.Artifacts {
		got[key{artifact.Kind, artifact.Id}] = artifact.Path
	}
	for want, path := range map[key]string{
		{"playbook", "base"}:      ".sdo/playbooks/base/README.md",
		{"script", "base/fix.sh"}: ".sdo/playbooks/base/scripts/fix.sh",
		{"detector", "cache"}:     ".sdo/diagnostics/detectors/incidents/cache/manifest.yaml",
		{"detector", "health"}:    ".sdo/diagnostics/detectors/health/health/manifest.yaml",
		{"goal", ""}:              ".sdo/goal.md",
		{"architecture", ""}:      ".sdo/arch.md",
	} {
		if got[want] != path {
			t.Errorf("%v path = %q, want %q (all: %v)", want, got[want], path, got)
		}
	}
	_, stdout, _ = runMemory(t, nil, "ls", "--root", root, "--kind", "playbook")
	if strings.TrimSpace(stdout) != "playbook\tbase\t.sdo/playbooks/base/README.md" {
		t.Fatalf("filtered listing = %q", stdout)
	}
	if code, _, _ := runMemory(t, nil, "ls", "--root", root, "--kind", "nonsense"); code != 2 {
		t.Fatalf("unknown kind code = %d", code)
	}
}

func TestMemoryNewScaffoldsPassCheck(t *testing.T) {
	root := copyFixture(t, goodFixture)
	for _, args := range [][]string{
		{"new", "--root", root, "playbook", "disk-full"},
		{"new", "--root", root, "detector", "disk-pressure", "--playbook", "disk-full"},
		{"new", "--root", root, "detector", "ready-pods", "--class", "health"},
	} {
		if code, _, stderr := runMemory(t, nil, args...); code != 0 {
			t.Fatalf("%v: code=%d stderr=%q", args, code, stderr)
		}
	}
	if diagnostics, err := validate.Validate(context.Background(), validate.Request{Root: root, Layers: validate.Structural | validate.Semantic}); err != nil || len(diagnostics) > 0 {
		t.Fatalf("scaffolds fail static validation: %v %v", diagnostics, err)
	}
	tree, err := layout.ReadTree(root)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]layout.Class{}
	for _, detector := range tree.Detectors {
		found[detector.Manifest.ID] = detector.Class
	}
	if found["disk-pressure"] != layout.ClassIncident || found["ready-pods"] != layout.ClassHealth {
		t.Fatalf("detectors = %v", found)
	}
}

func TestMemoryNewRefusesExistingAndInvalidIDs(t *testing.T) {
	root := copyFixture(t, goodFixture)
	for name, args := range map[string][]string{
		"existing playbook": {"new", "--root", root, "playbook", "base"},
		"existing detector": {"new", "--root", root, "detector", "cache"},
		"bad id":            {"new", "--root", root, "playbook", "Bad_ID"},
		"bad kind":          {"new", "--root", root, "widget", "x"},
		"bad class":         {"new", "--root", root, "detector", "x", "--class", "other"},
	} {
		if code, _, stderr := runMemory(t, nil, args...); code != 2 || stderr == "" {
			t.Errorf("%s: code=%d stderr=%q", name, code, stderr)
		}
	}
}

func TestMemoryOutcomesPrintsStoredRecords(t *testing.T) {
	root := copyFixture(t, goodFixture)
	record := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "inc-1", Classification: "success"}
	if _, err := memory.NewJSONStore(root).Put(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := runMemory(t, nil, "outcomes", "--root", root, "--json")
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	outcomes := &contracts.MemoryOutcomes{}
	if err := protojson.Unmarshal([]byte(stdout), outcomes); err != nil {
		t.Fatal(err)
	}
	if len(outcomes.Outcomes) != 1 || outcomes.Outcomes[0].IncidentId != "inc-1" {
		t.Fatalf("outcomes = %v", outcomes.Outcomes)
	}
}

func TestMemoryNewDetectorScaffoldsBuildVetAndPassTheirTests(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".sdo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/schema-version"), []byte("3\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"new", "--root", root, "detector", "disk-pressure"},
		{"new", "--root", root, "detector", "ready-pods", "--class", "health"},
	} {
		if code, _, stderr := runMemory(t, nil, args...); code != 0 {
			t.Fatalf("%v: code=%d stderr=%q", args, code, stderr)
		}
	}
	operatorRoot, err := filepath.Abs(filepath.Join("..", "."))
	if err != nil {
		t.Fatal(err)
	}
	if err := RunValidate(context.Background(), builder.Config{ApplicationRoot: root, OperatorRoot: operatorRoot}); err != nil {
		t.Fatal(err)
	}
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
