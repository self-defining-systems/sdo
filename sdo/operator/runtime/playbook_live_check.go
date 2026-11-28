package runtime

import (
	"context"
	"errors"
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
// read-only check fails, or cannot run, on the live cluster.
const LivePlaybookCheckRule = "playbook.check-cannot-run"

// LivePlaybookCheckNote is the memory-check note of a changed playbook whose
// live check was not run: it declares object roles this incident did not
// bind, or its Mitigate planned nothing it could check. It never blocks.
const LivePlaybookCheckNote = "playbook.check-not-run-live"

// LiveCheckReport is what a live check found: Blocking diagnostics for checks
// whose own exit code answered neither "holds" nor "needs the change" or that
// could not run twice, and non-blocking Notes for checks that were not run.
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
// the incident and the checkout under check. incidentOpen reports whether the
// controller has an incident open now; the closing turn runs after its own
// incident closed, so an open one is new and the cluster is not recovered.
func NewLivePlaybookCheck(newLoop func(IncidentRequest) *ResponderLoop, namespace string, incidentOpen func() bool) LivePlaybookCheck {
	return func(ctx context.Context, incidentID, root, base string) (LiveCheckReport, error) {
		ids, err := memory.ChangedPlaybooks(ctx, root, base)
		if err != nil || len(ids) == 0 {
			return LiveCheckReport{}, err
		}
		loop := newLoop(IncidentRequest{IncidentID: incidentID, Namespace: namespace, RepositoryWorktree: root})
		loop.WorkingProgramRoot = root
		blocking, notes := checkPlaybooksLive(ctx, loop, liveCheckTargets(root, incidentID, ids), incidentOpen)
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
// nothing, or refuses, once healthy), the plan it made on the incident's
// captured fault-time snapshot is checked instead; that plan names objects
// the repair may have replaced, so its failures are notes.
//
// An observation of a plan made on the live cluster must answer "holds"
// (Expect) or "needs the change". A step that cannot run (an exec or
// transport error, a timeout, a Diagnose error) is retried once, so a Pod
// replaced mid-check never costs the playbook, and then blocks: the automatic
// stage runs the same reads through the same transport and would fail the
// same way on first reuse. A Diagnose that Matches the recovered cluster on
// two looks blocks too: the stage would apply the repair to healthy state
// (a cache of counts flushed as if poisoned). While a new incident is open
// (incidentOpen) the cluster is not the recovered one: no playbook runs live
// beside its responder, and a live failure seen once it opened is a note. A Mitigate that fails on both views is a note: it
// may refuse healthy state by design. The memory check's own time limit
// running out (ctx done) is a note for every playbook it did not finish.
func checkPlaybooksLive(ctx context.Context, loop liveCheckLoop, targets []liveCheckTarget, incidentOpen func() bool) (blocking, notes []validate.Diagnostic) {
	fail := func(id, message, hint string) {
		blocking = append(blocking, validate.Diagnostic{Rule: LivePlaybookCheckRule, Path: layout.PlaybookDir(id), Message: message, Hint: hint})
	}
	const wrongCode = "the check ran read only on the recovered cluster; fix it so it exits Expect when the state holds and the needs-change code otherwise, then run memory check again"
	const diagnoseCannotRun = "it failed twice on the recovered cluster, where state is healthy: on healthy state Diagnose should return NoMatch, not an error; fix the read (target, container, binary, timeout, output parsing) or remove the program and keep the playbook README-only, then run memory check again"
	const matchesHealthy = "the incident is fixed, so on this state Diagnose must return NoMatch, or the automatic stage applies the repair to healthy state; key it on a state the healthy application never has (check the object it matched holds what you assumed), then run memory check again"
	const cannotRun = "it failed twice on the recovered cluster through the transport the automatic stage uses, so the stage would fail the same way on first reuse; fix the read (target, container, binary, timeout, output parsing) or remove the program and keep the playbook README-only, then run memory check again"
	note := func(id, message string) {
		notes = append(notes, validate.Diagnostic{Rule: LivePlaybookCheckNote, Path: layout.PlaybookDir(id), Message: message})
	}
	// A new incident makes the cluster not the recovered one: what fails there
	// says nothing about the playbook, so it is a note, never a block.
	opened := func() bool { return incidentOpen != nil && incidentOpen() }
	failLive := func(id, message, hint string) {
		if opened() {
			note(id, message+"; not checked: a new incident is open, so the cluster is not the recovered one")
			return
		}
		fail(id, message, hint)
	}
	outOfTime := func(id string) {
		reason := "was cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "reached its time limit"
		}
		notes = append(notes, validate.Diagnostic{Rule: LivePlaybookCheckNote, Path: layout.PlaybookDir(id),
			Message: fmt.Sprintf("playbook %s was not fully checked live: the memory check %s first (a timeout, not a fault in the playbook)", id, reason)})
	}
	for _, target := range targets {
		id := target.ID
		if ctx.Err() != nil {
			outOfTime(id)
			continue
		}
		if len(target.Roles) > 0 && !target.Captured {
			note(id, fmt.Sprintf("playbook %s was not run live: it declares object roles %s and this incident has no captured case that binds them", id, strings.Join(target.Roles, ", ")))
			continue
		}
		if err := loop.loadWorkingPrograms(ctx, id); err != nil {
			fail(id, fmt.Sprintf("playbook %s did not build for the live check: %v", id, err), "fix the build error, then run memory check again")
			continue
		}
		// Nothing runs live beside a new incident's responder.
		if opened() {
			note(id, fmt.Sprintf("playbook %s was not run live: a new incident is open, so the cluster is not the recovered one", id))
			continue
		}
		bindings := target.Bindings
		if bindings.Values == nil {
			bindings.Values = map[string]any{}
		}
		var view program.ReadView
		var verdict program.Verdict
		diagnose := func() error {
			var err error
			view, err = loop.snapshotView(ctx)
			if err != nil {
				return err
			}
			verdict = program.Verdict{}
			if diagnoser, ok := loop.programs().Diagnoser(id); ok {
				verdict, err = diagnoser.Diagnose(ctx, view, bindings)
			}
			return err
		}
		err := retryOnce(ctx, diagnose)
		// The incident is fixed here, so a Match means the automatic stage
		// would apply the repair to healthy state. Look once more, so a read
		// taken as the cluster settles never costs the playbook.
		if err == nil && verdict.Result == program.Match {
			err = retryOnce(ctx, diagnose)
			if err == nil && verdict.Result == program.Match {
				failLive(id, fmt.Sprintf("playbook %s: its Diagnose Matches on the recovered cluster: %s", id, verdictObjects(verdict)), matchesHealthy)
				continue
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				outOfTime(id)
				continue
			}
			failLive(id, fmt.Sprintf("playbook %s: its Diagnose did not complete twice on the live cluster: %v", id, err), diagnoseCannotRun)
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
		var actions []program.Action
		liveErr := retryOnce(ctx, func() error {
			var err error
			actions, err = liveCheckActions(ctx, mitigator, view, bindings)
			return err
		})
		if ctx.Err() != nil {
			outOfTime(id)
			continue
		}
		// fromCapture: the live cluster planned nothing, so the fault-time
		// plan is checked; it can name Pods the repair replaced.
		fromCapture := false
		if len(actions) == 0 && target.Open != nil {
			var openErr error
			actions, openErr = liveCheckActions(ctx, mitigator, target.Open, bindings)
			fromCapture = len(actions) > 0
			if liveErr != nil && openErr != nil {
				note(id, fmt.Sprintf("playbook %s: its Mitigate failed on the recovered cluster (%v) and on the fault-time capture (%v); its checks were not run", id, liveErr, openErr))
				continue
			}
		} else if liveErr != nil && len(actions) == 0 {
			note(id, fmt.Sprintf("playbook %s: its Mitigate failed on the recovered cluster twice (%v); its checks were not run", id, liveErr))
			continue
		}
		report := func(message, hint string) {
			if fromCapture {
				note(id, message+" (on the plan Mitigate made on the fault-time capture, since it planned nothing live; that plan can name objects the repair replaced)")
				return
			}
			failLive(id, message, hint)
		}
		for index, action := range actions {
			observe, _, expect := externalPair(action)
			var code int32
			if err := retryOnce(ctx, func() error {
				var err error
				code, err = client.RunOperation(ctx, observe)
				return err
			}); err != nil {
				if ctx.Err() != nil {
					outOfTime(id)
					break
				}
				report(fmt.Sprintf("playbook %s action %d: its check %s could not run live twice: %v", id, index+1, operationText(observe), err), cannotRun)
				continue
			}
			if code != expect && !program.ObservationNeedsChange(action, code) {
				report(fmt.Sprintf("playbook %s action %d: its read-only check %s exited %d live; it must exit %d when the state holds or the needs-change code when it does not", id, index+1, operationText(observe), code, expect), wrongCode)
			}
		}
	}
	return blocking, notes
}

// liveCheckActions is the Ensure actions, with an observation, of the plan
// mitigator makes on view, or the error that made the plan fail.
func liveCheckActions(ctx context.Context, mitigator program.Mitigator, view program.ReadView, bindings program.Bindings) ([]program.Action, error) {
	plan, err := BuildMitigationPlan(ctx, mitigator, view, bindings, liveCheckPlanDeadline)
	if err != nil {
		return nil, err
	}
	var actions []program.Action
	for _, action := range plan.Actions {
		action = concreteExternalAction(action)
		if observe, _, _ := externalPair(action); observe.Exec != nil || observe.HTTP != nil {
			actions = append(actions, action)
		}
	}
	return actions, nil
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

// verdictObjects names the objects a verdict cites, at most four.
func verdictObjects(verdict program.Verdict) string {
	var names []string
	for _, evidence := range verdict.Evidence {
		ref := evidence.Resource
		if ref.Name == "" {
			continue
		}
		name := ref.Kind + "/" + ref.Namespace + "/" + ref.Name
		if slices.Contains(names, name) {
			continue
		}
		if len(names) == 4 {
			names = append(names, "…")
			break
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return "no object cited"
	}
	return strings.Join(names, ", ")
}
