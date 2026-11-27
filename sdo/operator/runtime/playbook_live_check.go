package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// liveCheckPlanDeadline bounds one new playbook's Mitigate in the live check.
const liveCheckPlanDeadline = time.Minute

// LivePlaybookCheckRule is the memory-check rule of a new playbook whose
// read-only check cannot run on the live cluster.
const LivePlaybookCheckRule = "playbook.check-cannot-run"

// LivePlaybookCheckNote is the memory-check note of a changed playbook whose
// live check could not run (a timeout, a Pod replaced during the check, a
// Diagnose error, no captured bindings for its roles). It never blocks: only
// the check's own exit code says the check is wrong.
const LivePlaybookCheckNote = "playbook.check-not-run-live"

// LiveCheckReport is what a live check found: Blocking diagnostics for checks
// whose own exit code answered neither "holds" nor "needs the change", and
// non-blocking Notes for checks that could not run.
type LiveCheckReport struct {
	Blocking []*contracts.MemoryDiagnostic
	Notes    []*contracts.MemoryDiagnostic
}

// LivePlaybookCheck runs, read only, the checks of the playbooks a closing
// turn added or edited in the checkout root (relative to base) against the
// recovered cluster. The static memory check builds a playbook and replays its
// Diagnose on captured cases but never runs its commands, so a check that
// fails on first reuse fails here instead, while the session that wrote it can
// still fix it.
//
// Read only is the playbook's contract, not an enforced boundary: Diagnose
// reads through the same live read view the response stage gives it, and each
// observation runs through the same exec/HTTP transport the stage's executor
// uses for its own observation step. Neither path can stop a command that
// writes; the stage has the same exposure.
type LivePlaybookCheck func(ctx context.Context, incidentID, root, base string) (LiveCheckReport, error)

// liveCheckLoop is the part of a responder loop the live check uses.
type liveCheckLoop interface {
	loadWorkingPrograms(ctx context.Context, id string) error
	snapshotView(ctx context.Context) (program.ReadView, error)
	programs() program.ProgramRegistry
	operations() (PlanOperationClient, bool)
}

func (l *ResponderLoop) programs() program.ProgramRegistry { return l.Programs }
func (l *ResponderLoop) operations() (PlanOperationClient, bool) {
	client, ok := l.Executor.Client.(PlanOperationClient)
	return client, ok
}

// liveCheckTarget is one changed playbook and what this incident captured for
// it, when Go wrote a corpus case for this incident: the bindings it matched
// on and a view of the fault-time snapshot.
type liveCheckTarget struct {
	ID       string
	Roles    []string
	Captured bool
	Bindings program.Bindings
	Open     program.ReadView
}

// NewLivePlaybookCheck checks with a fresh responder loop per call, bound to
// the incident and the checkout under check.
func NewLivePlaybookCheck(newLoop func(IncidentRequest) *ResponderLoop, namespace string) LivePlaybookCheck {
	return func(ctx context.Context, incidentID, root, base string) (LiveCheckReport, error) {
		ids, err := memory.ChangedPlaybooks(ctx, root, base)
		if err != nil || len(ids) == 0 {
			return LiveCheckReport{}, err
		}
		loop := newLoop(IncidentRequest{IncidentID: incidentID, Namespace: namespace, RepositoryWorktree: root})
		loop.WorkingProgramRoot = root
		blocking, notes := checkPlaybooksLive(ctx, loop, liveCheckTargets(root, incidentID, ids))
		return LiveCheckReport{Blocking: validate.ProtoDiagnostics(blocking), Notes: validate.ProtoDiagnostics(notes)}, nil
	}
}

// liveCheckTargets pairs each playbook with this incident's corpus case for
// it in root, the case Go wrote from the incident's capture with the bindings
// the playbook matched on.
func liveCheckTargets(root, incidentID string, ids []string) []liveCheckTarget {
	cases := map[string]liveCheckTarget{}
	entries, _ := os.ReadDir(filepath.Join(root, layout.CorpusDir))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, layout.CorpusDir, entry.Name())
		meta, err := sdktest.LoadCorpusMetadata(dir)
		if err != nil || meta.Provenance.IncidentID != incidentID {
			continue
		}
		for _, id := range meta.RootCauseProgramIDs {
			if _, seen := cases[id]; seen || !slices.Contains(ids, id) {
				continue
			}
			captured := liveCheckTarget{Captured: true, Bindings: meta.Bindings}
			if full, err := sdktest.LoadCorpusCase(dir); err == nil {
				captured.Open = &sdktest.ReplayReadView{Snapshot: full.Open, Calls: full.PhaseRecordings("open")}
			}
			cases[id] = captured
		}
	}
	targets := make([]liveCheckTarget, 0, len(ids))
	for _, id := range ids {
		target := cases[id]
		target.ID, target.Roles = id, validate.PlaybookRoles(root, id)
		targets = append(targets, target)
	}
	return targets
}

// checkPlaybooksLive runs each playbook's Diagnose on a fresh snapshot with
// the bindings this incident captured and, for the plan its Mitigate returns,
// every Ensure action's observation, which is read only by contract. When the
// plan on the recovered cluster has no observation (a Mitigate that plans
// nothing once healthy), the plan it made on the incident's captured
// fault-time snapshot is checked instead.
//
// Only an observation's own exit code blocks: it must answer "holds" (Expect)
// or "needs the change". A check that cannot run (an exec or transport error
// such as a Pod replaced mid-check, a timeout, a Diagnose error) is retried
// once and then reported as a note, so one hiccup never costs the playbook.
func checkPlaybooksLive(ctx context.Context, loop liveCheckLoop, targets []liveCheckTarget) (blocking, notes []validate.Diagnostic) {
	fail := func(id, message string) {
		blocking = append(blocking, validate.Diagnostic{Rule: LivePlaybookCheckRule, Path: layout.PlaybookDir(id), Message: message,
			Hint: "the check ran read only on the recovered cluster; fix it so it exits Expect when the state holds and the needs-change code otherwise, then run memory check again"})
	}
	note := func(id, message string) {
		notes = append(notes, validate.Diagnostic{Rule: LivePlaybookCheckNote, Path: layout.PlaybookDir(id), Message: message})
	}
	for _, target := range targets {
		id := target.ID
		if len(target.Roles) > 0 && !target.Captured {
			note(id, fmt.Sprintf("playbook %s was not run live: it declares object roles %s and this incident has no captured case that binds them", id, strings.Join(target.Roles, ", ")))
			continue
		}
		if err := loop.loadWorkingPrograms(ctx, id); err != nil {
			fail(id, fmt.Sprintf("playbook %s did not build for the live check: %v", id, err))
			continue
		}
		bindings := target.Bindings
		if bindings.Values == nil {
			bindings.Values = map[string]any{}
		}
		var view program.ReadView
		if err := retryOnce(ctx, func() error {
			var err error
			view, err = loop.snapshotView(ctx)
			if err != nil {
				return err
			}
			if diagnoser, ok := loop.programs().Diagnoser(id); ok {
				_, err = diagnoser.Diagnose(ctx, view, bindings)
			}
			return err
		}); err != nil {
			note(id, fmt.Sprintf("playbook %s was not run live: its Diagnose did not complete twice: %v", id, err))
			continue
		}
		mitigator, ok := loop.programs().Mitigator(id)
		if !ok {
			continue
		}
		client, ok := loop.operations()
		if !ok {
			continue
		}
		actions := liveCheckActions(ctx, mitigator, view, bindings)
		if len(actions) == 0 && target.Open != nil {
			actions = liveCheckActions(ctx, mitigator, target.Open, bindings)
		}
		for index, action := range actions {
			observe, _, expect := externalPair(action)
			var code int32
			if err := retryOnce(ctx, func() error {
				var err error
				code, err = client.RunOperation(ctx, observe)
				return err
			}); err != nil {
				note(id, fmt.Sprintf("playbook %s action %d: its check %s could not run live twice: %v", id, index+1, operationText(observe), err))
				continue
			}
			if code != expect && !program.ObservationNeedsChange(action, code) {
				fail(id, fmt.Sprintf("playbook %s action %d: its read-only check %s exited %d live; it must exit %d when the state holds or the needs-change code when it does not", id, index+1, operationText(observe), code, expect))
			}
		}
	}
	return blocking, notes
}

// liveCheckActions is the Ensure actions, with an observation, of the plan
// mitigator makes on view; a Mitigate that errors or plans nothing gives none.
func liveCheckActions(ctx context.Context, mitigator program.Mitigator, view program.ReadView, bindings program.Bindings) []program.Action {
	plan, err := BuildMitigationPlan(ctx, mitigator, view, bindings, liveCheckPlanDeadline)
	if err != nil {
		return nil
	}
	var actions []program.Action
	for _, action := range plan.Actions {
		action = concreteExternalAction(action)
		if observe, _, _ := externalPair(action); observe.Exec != nil || observe.HTTP != nil {
			actions = append(actions, action)
		}
	}
	return actions
}

// retryOnce runs try and, if it fails while ctx is still live, once more.
func retryOnce(ctx context.Context, try func() error) error {
	err := try()
	if err != nil && ctx.Err() == nil {
		err = try()
	}
	return err
}

func operationText(op program.ExternalOperation) string {
	if op.Exec != nil {
		text := strings.Join(op.Exec.Args, " ")
		if len(text) > 80 {
			text = text[:80] + "…"
		}
		return "`" + text + "`"
	}
	if op.HTTP != nil {
		return op.HTTP.Method + " " + op.HTTP.URL
	}
	return "?"
}
