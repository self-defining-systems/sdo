//go:build integration

package operatorcli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
	"sdo.dev/operator/operability"
)

// cleanPlan expects the one ConfigMap world has: no workloads.
const cleanPlan = `{"source":{"rendererId":"raw","config":{"@type":"type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig","paths":["k8s"]}}}`

func operabilityCLI(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := RunOperability(context.Background(), args, func(key string) string { return env[key] }, &out, &errOut)
	return code, out.String(), errOut.String()
}

func writePlan(t *testing.T, work, plan string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(work, ".sdo", "deployment", "plan.yaml"), []byte(plan), 0o644); err != nil {
		t.Fatal(err)
	}
}

func decodeOutput(t *testing.T, out string) operabilityOutput {
	t.Helper()
	var o operabilityOutput
	if err := json.Unmarshal([]byte(out), &o); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, out)
	}
	if err := o.Report.Validate(); err != nil {
		t.Fatalf("printed report is invalid: %v", err)
	}
	return o
}

func TestOperabilityCheckOfflineCleanCheckout(t *testing.T) {
	work := cliWorld(t)
	writePlan(t, work, cleanPlan)
	code, out, errOut := operabilityCLI(t, nil, "check", "--offline", "--root", work)
	if code != 0 {
		t.Fatalf("clean = %d\n%s%s", code, out, errOut)
	}
	for _, line := range []string{"pass     O1.render", "pass     O6.scope", "skipped  O6.allowlist", "decision: accepted"} {
		if !strings.Contains(out, line) {
			t.Errorf("missing %q in\n%s", line, out)
		}
	}
	code, out, _ = operabilityCLI(t, nil, "check", "--offline", "--root", work, "--json")
	o := decodeOutput(t, out)
	if code != 0 || o.Decision.Kind != operability.Accepted {
		t.Fatalf("json = %d %+v", code, o.Decision)
	}
	if s := o.Report.Subject; s.EffectiveTree == operability.Unavailable || s.RenderDigest == operability.Unavailable || s.BaseTree == operability.Unavailable || s.AcceptedCommit == operability.Unavailable {
		t.Fatalf("a clean checkout produced an incomplete subject: %+v", s)
	}
}

func TestOperabilityCheckOfflineReportsTypedFailures(t *testing.T) {
	work := cliWorld(t)
	// A committed tree with no deployment files is the deployer's O1.render
	// failure (case A), found offline.
	testutil.Git(t, work, "rm", "-rq", "k8s")
	testutil.Git(t, work, "commit", "-qm", "no deployment files")
	if err := os.Remove(filepath.Join(work, ".sdo", "deployment", "plan.yaml")); err != nil {
		t.Fatal(err)
	}
	code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work, "--json")
	o := decodeOutput(t, out)
	if code != 1 || o.Decision.Kind != operability.Correct || o.Decision.Owner != operability.OwnerDeployer || len(o.Decision.Checks) != 1 || o.Decision.Checks[0] != "O1.render" {
		t.Fatalf("no deployment files = %d %+v", code, o.Decision)
	}
	code, out, _ = operabilityCLI(t, nil, "check", "--offline", "--root", work, "--allow-registry", "ghcr.io")
	if code != 1 || !strings.Contains(out, "fail     O1.render [render]") || !strings.Contains(out, "no deployment files") || !strings.Contains(out, "owner=deployer") {
		t.Fatalf("text = %d\n%s", code, out)
	}

	// An invalid override is O1.plan.
	writePlan(t, work, "{")
	if code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work); code != 1 || !strings.Contains(out, "fail     O1.plan [render]") {
		t.Fatalf("invalid plan = %d\n%s", code, out)
	}
}

func TestOperabilityCheckOfflinePolicyFlags(t *testing.T) {
	work := cliWorld(t)
	writePlan(t, work, cleanPlan)
	foreign := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n  namespace: elsewhere\n"
	if err := os.WriteFile(filepath.Join(work, "k8s", "foreign.yaml"), []byte(foreign), 0o644); err != nil {
		t.Fatal(err)
	}
	// The foreign object is app source: Go checks what is committed.
	testutil.Git(t, work, "add", "k8s/foreign.yaml")
	testutil.Git(t, work, "commit", "-qm", "foreign object")
	if code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work); code != 0 || !strings.Contains(out, "note     O6.scope") {
		t.Fatalf("default scope = %d\n%s", code, out)
	}
	if code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work, "--scope-blocking"); code != 1 || !strings.Contains(out, "fail     O6.scope [scope]") {
		t.Fatalf("blocking scope = %d\n%s", code, out)
	}
	if code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work, "--scope-blocking", "--grant-foreign"); code != 0 {
		t.Fatalf("granted scope = %d\n%s", code, out)
	}
	if code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work, "--allow-registry", "ghcr.io, docker.io"); code != 0 || !strings.Contains(out, "pass     O6.allowlist") {
		t.Fatalf("allowlist = %d\n%s", code, out)
	}
}

func TestOperabilityCheckOfflineCompleteStageRunsTheMemoryValidator(t *testing.T) {
	work := cliWorld(t)
	writePlan(t, work, cleanPlan)
	code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work, "--stage", "complete", "--json")
	o := decodeOutput(t, out)
	if o.Report.Stage != operability.StageComplete {
		t.Fatalf("stage = %s", o.Report.Stage)
	}
	var ids []string
	for _, it := range o.Report.Items {
		for _, c := range it.Checks {
			ids = append(ids, c.ID)
		}
	}
	joined := strings.Join(ids, " ")
	for _, id := range []string{"O3.coverage", "O6.ownership"} {
		if !strings.Contains(joined, id) {
			t.Errorf("complete stage lacks %s: %s", id, joined)
		}
	}
	// This synthetic checkout has no operational memory beyond the plan, so the
	// shared validator objects, routes it to the designer, and the exit follows.
	if code != 1 || o.Decision.Kind != operability.Correct || o.Decision.Owner != operability.OwnerDesigner || !strings.Contains(joined, "O5.memory.memory") {
		t.Fatalf("complete stage over missing memory = %d %+v (%s)", code, o.Decision, joined)
	}
	// With the ownership inputs set, O6.ownership is evaluated rather than skipped.
	env := map[string]string{"SDO_BASE_COMMIT": testutil.Git(t, work, "rev-parse", "HEAD"), "SDO_ACTOR": "deployer", "SDO_PHASE": "initial"}
	_, out, _ = operabilityCLI(t, env, "check", "--offline", "--root", work, "--stage", "complete")
	if strings.Contains(out, "skipped  O6.ownership") {
		t.Fatalf("ownership skipped despite base, actor and phase:\n%s", out)
	}
}

// treeWorld is a checkout of an accepted repository holding files and no
// plan: the source is derived from the files, as deploy derives it.
func treeWorld(t *testing.T, files map[string]string) string {
	t.Helper()
	accepted := t.TempDir()
	testutil.InitGit(t, accepted)
	for name, content := range files {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(accepted, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(accepted, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	testutil.Git(t, accepted, "add", ".")
	testutil.Git(t, accepted, "commit", "-qm", "seed")
	work := filepath.Join(t.TempDir(), "checkout")
	testutil.Git(t, filepath.Dir(work), "clone", "-q", accepted, work)
	return work
}

// With the push target, the offline check applies deploy's image rule: an
// image under it with no Dockerfile fails O2.images before any build.
func TestOperabilityCheckOfflineMatchesPushedImagesToDockerfiles(t *testing.T) {
	const web = "apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: web}\nspec:\n  selector: {matchLabels: {app: web}}\n  template:\n    metadata: {labels: {app: web}}\n    spec: {containers: [{name: web, image: 'reg.example/team/web:v1'}]}\n"
	missing := treeWorld(t, map[string]string{"k8s/web.yaml": web, "src/main.go": "package main\n"})
	code, out, errOut := operabilityCLI(t, nil, "check", "--offline", "--root", missing, "--registry-host", "reg.example")
	if code != 1 || !strings.Contains(out, "fail     O2.images") || !strings.Contains(out, "Dockerfile.web") {
		t.Fatalf("no Dockerfile = %d\n%s%s", code, out, errOut)
	}
	built := treeWorld(t, map[string]string{"k8s/web.yaml": web, "web/Dockerfile": "FROM scratch\n", "Dockerfile.tools": "FROM scratch\n"})
	if code, out, errOut := operabilityCLI(t, nil, "check", "--offline", "--root", built, "--registry-host", "reg.example"); code != 0 || !strings.Contains(out, "pass     O2.images") {
		t.Fatalf("matched Dockerfile = %d\n%s%s", code, out, errOut)
	}
}
