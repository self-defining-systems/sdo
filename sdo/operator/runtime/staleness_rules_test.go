package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storeops"
	"sdo.dev/operator/staleness"
)

const stalenessCommit = "synthetic-accepted"

// stalenessStage is the stage of keyedStageLoop with the guard's knowledge set.
func stalenessStage(t *testing.T, p program.Mitigator, d program.Diagnoser, proof staleness.Proof, release staleness.Release, target broker.Target) (*sameWriteWorld, *contracts.StageReport) {
	t.Helper()
	world, stage, state := keyedStageLoop(t, nil)
	loop := stage.NewLoop(state.Request)
	registry, err := program.NewRegistry([]program.Diagnoser{d}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	loop.Programs = registry
	id := d.Spec().ID
	for i := range state.Findings {
		state.Findings[i].Playbooks = []string{id}
	}
	state.Request.Findings, loop.findings = state.Findings, state.Findings
	loop.Request = state.Request
	loop.Staleness = &StalenessGuard{loaded: true, Target: func() broker.Target { return target },
		state: stalenessState{Release: release, Proofs: map[string]staleness.Proof{id: proof}}}
	if testing.Verbose() {
		t.Logf("proof %+v release %+v", proof, release)
	}
	report, err := stage.Run(context.Background(), state)
	if err != nil {
		t.Fatal(err)
	}
	return world, report
}

func writes(report *contracts.StageReport) (n int) {
	for _, m := range report.GetMitigations() {
		if m.GetApplyReceipt().GetApplied() {
			n++
		}
	}
	return n
}

// restartAll is a scan that restarts every Deployment: it was learned on one.
type restartAll struct{ id string }

func (p restartAll) Spec() program.ProgramSpec { return program.ProgramSpec{ID: p.id} }
func (p restartAll) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, nil
}
func (p restartAll) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	var plan program.Plan
	for _, d := range v.Deployments() {
		plan.Actions = append(plan.Actions, program.RestartRollout{Ref: sdk.ObjectRefFrom("Deployment", "apps/v1", &d)})
	}
	return plan, nil
}

// restoreAll is the same scan over accepted source.
type restoreAll struct{ restartAll }

func (p restoreAll) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	var plan program.Plan
	for _, d := range v.Deployments() {
		plan.Actions = append(plan.Actions, program.ReconcileFromSource{Ref: sdk.ObjectRefFrom("Deployment", "apps/v1", &d)})
	}
	return plan, nil
}

func proven(components ...string) staleness.Proof {
	return staleness.Proof{Releases: []string{stalenessCommit}, Footprint: components}
}

// Harm 1: a drift scan that plans every workload of a healthy app. Its own
// recovered capture matches, so it is a hint and the agent takes over.
func TestHarm1APlaybookThatMatchesItsRecoveredStateIsAHint(t *testing.T) {
	p := restartAll{id: "mount-drift"}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	world, _ := stalenessStage(t, p, p, proven("workload/api", "workload/web"), staleness.Release{Commit: stalenessCommit}, broker.Target{Commit: stalenessCommit})
	if world.restarts.Load() == 0 {
		t.Fatal("without knowledge the scan writes: the case is not exercised")
	}
	dir := t.TempDir()
	snapshot, err := world.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writeStalenessCase(t, dir, p.id, snapshot.(sdktest.Snapshot))
	if _, err := sdktest.LoadCorpusCase(filepath.Join(dir, "case")); err != nil {
		t.Fatal(err)
	}
	proofs, _ := StalenessProofs(context.Background(), dir, []*contracts.OutcomeRecord{{IncidentId: "i1", Classification: "success", DeployedCommit: stalenessCommit}}, registry, nil)
	proof := proofs[p.id]
	if proof.Hint == "" || !slices.Contains(proof.Releases, stalenessCommit) {
		t.Fatalf("proof = %+v, want a hint proven on the release", proof)
	}
	world, report := stalenessStage(t, p, p, proof, staleness.Release{Commit: stalenessCommit}, broker.Target{Commit: stalenessCommit})
	requireRefused(t, world, report, "recovered state")
}

func requireRefused(t *testing.T, world *sameWriteWorld, report *contracts.StageReport, reason string) {
	t.Helper()
	if world.restarts.Load() != 0 || writes(report) != 0 || len(report.GetMitigations()) == 0 {
		t.Fatalf("stage wrote: %v", report)
	}
	for _, m := range report.GetMitigations() {
		if m.GetResult() != "skipped" || !strings.Contains(m.GetError(), reason) {
			t.Fatalf("attempt %v, want a skip for %q", m, reason)
		}
	}
	if stats := ProgramTrustStats([]*contracts.OutcomeRecord{{IncidentId: "x", Classification: "success", PriorStages: []*contracts.StageReport{report}}}); stats[report.GetMitigations()[0].GetPlaybookId()].Failures != 0 {
		t.Fatal("a refusal counted as a playbook failure")
	}
}

// Harm 2: the cluster runs a held release and accepted source is the older one;
// the restore also plans workloads its verified plan never wrote.
func TestHarm2ARestoreFromSourceThatIsNotTheRunningReleaseIsRefused(t *testing.T) {
	p := restoreAll{restartAll{id: "source-mounts"}}
	healthy := staleness.Release{Commit: stalenessCommit, Checked: map[string]bool{p.id: true}}
	all := proven("workload/api", "workload/web")
	world, report := stalenessStage(t, p, p, all, healthy, broker.Target{Commit: "10-6-0", Unaccepted: true})
	requireRefused(t, world, report, "not the running release")
	world, report = stalenessStage(t, p, p, proven("workload/api"), healthy, broker.Target{Commit: stalenessCommit})
	requireRefused(t, world, report, "its verified plan wrote 1")
}

// R1b: a Match on the healthy app of the release demotes; R3 refuses another component.
func TestDemotedAndForeignComponentPlansAreRefused(t *testing.T) {
	p := rolelessRestart{id: "restart", target: "api"}
	target := broker.Target{Commit: stalenessCommit}
	world, report := stalenessStage(t, p, p, proven("workload/api"), staleness.Release{Commit: stalenessCommit, Demoted: map[string]string{p.id: "Diagnose matches the healthy app"}}, target)
	requireRefused(t, world, report, "healthy app")
	world, report = stalenessStage(t, p, p, proven("workload/web"), staleness.Release{Commit: stalenessCommit}, target)
	requireRefused(t, world, report, "workload/api")
	if _, report = stalenessStage(t, p, p, proven("workload/api"), staleness.Release{Commit: stalenessCommit}, target); writes(report) != 1 {
		t.Fatalf("a proven plan did not run: %v", report)
	}
}

// A release nobody proved: the cells decide, and what cannot be read waits for a proof.
func TestR2bLiterals(t *testing.T) {
	repair := func(statement string) program.Plan {
		return program.Plan{Actions: []program.Action{program.RepairData{Target: program.StoreTarget{Component: "db", Engine: "postgres"},
			Scope: storeops.Scope{Database: "d", Collection: "limits", Selector: "plan_id = 1 AND size < 10"}, Statement: statement}}}
	}
	old := staleness.Proof{Releases: []string{"old"}}
	checked := func(facts staleness.Facts) staleness.Release {
		return staleness.Release{Commit: "new", Checked: map[string]bool{"p": true}, Facts: facts}
	}
	key := func(plan program.Plan) string { cells, _ := staleness.Literals(plan); return cells[0].Keys[0] }
	lit := repair("SET size = 500, updated_at = now()")
	for name, c := range map[string]struct {
		plan    program.Plan
		proof   staleness.Proof
		release staleness.Release
		want    string
	}{
		"same":             {lit, old, checked(staleness.Facts{key(lit): true}), ""},
		"differs":          {lit, old, checked(staleness.Facts{key(lit): false}), "did not hold"},
		"differs, proven":  {lit, staleness.Proof{Releases: []string{"new"}}, checked(staleness.Facts{key(lit): false}), "did not hold"},
		"unknown":          {lit, old, checked(nil), "not known"},
		"unknown, proven":  {lit, staleness.Proof{Releases: []string{"new"}}, checked(nil), ""},
		"default":          {repair("SET size = DEFAULT"), old, checked(nil), "cannot read"},
		"not checked yet":  {lit, old, staleness.Release{Commit: "new"}, "not yet checked"},
		"script":           {program.Plan{Actions: []program.Action{program.RunScript{}}}, old, checked(nil), "cannot read"},
		"restart, checked": {program.Plan{Actions: []program.Action{program.RestartRollout{}}}, old, checked(nil), ""},
	} {
		if got := staleness.Refuse("p", c.plan, c.proof, c.release); (c.want == "") != (got == "") || !strings.Contains(got, c.want) {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
	cells, _ := staleness.Literals(program.Plan{Actions: []program.Action{program.EnsureExec{Ensure: program.Command{Args: []string{"sh", "-c", `psql -c "UPDATE limits l SET size = 20, updated_at = now() WHERE size = 0"`}}}}})
	if len(cells) != 2 || !strings.Contains(cells[1].Keys[0], "limits.size=20") {
		t.Errorf("SQL in a command: %+v", cells)
	}
	if !strings.Contains(key(lit), "where plan_id = 1") || strings.Contains(key(lit), "size < 10") {
		t.Errorf("key %s", key(lit))
	}
}

func writeStalenessCase(t *testing.T, dir, id string, snapshot sdktest.Snapshot) {
	t.Helper()
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	sum := fmt.Sprintf("%x", sha256.Sum256(raw))
	labels := fmt.Sprintf(`{"root_cause_program_ids":[%q],"bindings":{},"peer_bindings":{},"provenance":{"incident_id":"i1","open_sha256":%q,"fixed_sha256":%q}}`, id, sum, sum)
	for name, body := range map[string][]byte{"open.json": raw, "fixed.json": raw, "labels.json": []byte(labels), "recordings.json": []byte("[]")} {
		path := filepath.Join(dir, "case", name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
