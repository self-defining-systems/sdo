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
	for _, line := range []string{"pass     O1.patches", "pass     O1.render", "pass     O1.deterministic", "pass     O1.workloads", "pass     O6.scope", "skipped  O6.allowlist", "decision: accepted"} {
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
	work := cliWorld(t) // plan expects Deployment/default/app, which the tree does not render
	code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work, "--json")
	o := decodeOutput(t, out)
	if code != 1 || o.Decision.Kind != operability.Correct || o.Decision.Owner != operability.OwnerDeployer || len(o.Decision.Checks) != 1 || o.Decision.Checks[0] != "O1.workloads" {
		t.Fatalf("expected workloads = %d %+v", code, o.Decision)
	}
	code, out, _ = operabilityCLI(t, nil, "check", "--offline", "--root", work, "--allow-registry", "ghcr.io")
	if code != 1 || !strings.Contains(out, "fail     O1.workloads [render]") || !strings.Contains(out, "owner=deployer") {
		t.Fatalf("text = %d\n%s", code, out)
	}

	// A series that does not apply stops at O1.patches; the subject is operability.Unavailable, not invented.
	bad := "SDO-Patch: 1\nOwner: deployer\nReason: |\n  " + patchCLIReason + "\n\n" +
		"diff --git a/README b/README\nindex " + strings.Repeat("1", 40) + ".." + strings.Repeat("2", 40) + " 100644\n--- a/README\n+++ b/README\n@@ -1 +1 @@\n-nope\n+x\n"
	dir := filepath.Join(work, ".sdo", "deployment", "patches")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "0001-bad.patch"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, _ = operabilityCLI(t, nil, "check", "--offline", "--root", work, "--json")
	o = decodeOutput(t, out)
	if code != 1 || o.Decision.Kind != operability.Correct || o.Decision.Checks[0] != "O1.patches" {
		t.Fatalf("broken series = %d %+v", code, o.Decision)
	}
	if o.Report.Subject.EffectiveTree != operability.Unavailable {
		t.Fatalf("subject invented an effective tree: %+v", o.Report.Subject)
	}
	if got := o.Report.Items[0].Checks[0].Paths; len(got) == 0 || got[0] != ".sdo/deployment/patches/0001-bad.patch" {
		t.Fatalf("patch paths = %v", got)
	}

	// An invalid plan is O1.plan, and a missing plan is an infrastructure error.
	writePlan(t, work, "{")
	if code, out, _ := operabilityCLI(t, nil, "check", "--offline", "--root", work); code != 1 || !strings.Contains(out, "fail     O1.plan [render]") {
		t.Fatalf("invalid plan = %d\n%s", code, out)
	}
	if err := os.Remove(filepath.Join(work, ".sdo", "deployment", "plan.yaml")); err != nil {
		t.Fatal(err)
	}
	if code, _, errOut := operabilityCLI(t, nil, "check", "--offline", "--root", work); code != 2 || !strings.Contains(errOut, "plan.yaml") {
		t.Fatalf("missing plan = %d %s", code, errOut)
	}
}

func TestOperabilityCheckOfflinePolicyFlags(t *testing.T) {
	work := cliWorld(t)
	writePlan(t, work, cleanPlan)
	foreign := "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: other\n  namespace: elsewhere\n"
	if err := os.WriteFile(filepath.Join(work, "k8s", "foreign.yaml"), []byte(foreign), 0o644); err != nil {
		t.Fatal(err)
	}
	// The foreign object is app source, so capture it into the series first.
	if code, out, errOut := patchCLI(t, "create", "--root", work, "--reason", patchCLIReason, "k8s/foreign.yaml"); code != 0 {
		t.Fatalf("create = %d\n%s%s", code, out, errOut)
	}
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
