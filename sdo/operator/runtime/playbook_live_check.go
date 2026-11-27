package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk/program"
)

// liveCheckPlanDeadline bounds one new playbook's Mitigate in the live check.
const liveCheckPlanDeadline = time.Minute

// LivePlaybookCheckRule is the memory-check rule of a new playbook whose
// read-only check cannot run on the live cluster.
const LivePlaybookCheckRule = "playbook.check-cannot-run"

// LivePlaybookCheck runs, read only, the checks of the playbooks a closing
// turn added in the checkout root (relative to base) against the recovered
// cluster, and returns a diagnostic for each check that cannot run. The
// static memory check builds a playbook and replays its Diagnose on captured
// cases but never runs its commands, so a check that fails on first reuse
// fails here instead, while the session that wrote it can still fix it.
type LivePlaybookCheck func(ctx context.Context, incidentID, root, base string) ([]*contracts.MemoryDiagnostic, error)

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

// NewLivePlaybookCheck checks with a fresh responder loop per call, bound to
// the incident and the checkout under check.
func NewLivePlaybookCheck(newLoop func(IncidentRequest) *ResponderLoop, namespace string) LivePlaybookCheck {
	return func(ctx context.Context, incidentID, root, base string) ([]*contracts.MemoryDiagnostic, error) {
		ids, err := memory.NewPlaybooks(ctx, root, base)
		if err != nil || len(ids) == 0 {
			return nil, err
		}
		loop := newLoop(IncidentRequest{IncidentID: incidentID, Namespace: namespace, RepositoryWorktree: root})
		loop.WorkingProgramRoot = root
		var roleless []string
		for _, id := range ids {
			// A playbook with object roles has bindings only an incident
			// gives; the recovered cluster has none to offer.
			if len(validate.PlaybookRoles(root, id)) == 0 {
				roleless = append(roleless, id)
			}
		}
		return validate.ProtoDiagnostics(checkPlaybooksLive(ctx, loop, roleless)), nil
	}
}

// checkPlaybooksLive runs each playbook's Diagnose on a fresh snapshot and,
// for the plan its Mitigate returns, every Ensure action's observation, which
// is read only by contract. An observation must answer "holds" (Expect) or
// "needs the change"; any other exit, or a Diagnose that errors, is a check
// that cannot run. A Mitigate that returns no plan on the recovered cluster
// is not a failure: there may be nothing to change.
func checkPlaybooksLive(ctx context.Context, loop liveCheckLoop, ids []string) []validate.Diagnostic {
	var diagnostics []validate.Diagnostic
	fail := func(id, message string) {
		diagnostics = append(diagnostics, validate.Diagnostic{Rule: LivePlaybookCheckRule, Path: layout.PlaybookDir(id), Message: message,
			Hint: "the check ran read only on the recovered cluster; fix it so it exits Expect when the state holds and the needs-change code otherwise, then run memory check again"})
	}
	for _, id := range ids {
		if err := loop.loadWorkingPrograms(ctx, id); err != nil {
			fail(id, fmt.Sprintf("playbook %s did not build for the live check: %v", id, err))
			continue
		}
		view, err := loop.snapshotView(ctx)
		if err != nil {
			return diagnostics
		}
		bindings := program.Bindings{Values: map[string]any{}}
		if diagnoser, ok := loop.programs().Diagnoser(id); ok {
			if _, err := diagnoser.Diagnose(ctx, view, bindings); err != nil {
				fail(id, fmt.Sprintf("playbook %s Diagnose failed live: %v", id, err))
				continue
			}
		}
		mitigator, ok := loop.programs().Mitigator(id)
		if !ok {
			continue
		}
		plan, err := BuildMitigationPlan(ctx, mitigator, view, bindings, liveCheckPlanDeadline)
		if err != nil {
			continue
		}
		client, ok := loop.operations()
		if !ok {
			continue
		}
		for index, action := range plan.Actions {
			action = concreteExternalAction(action)
			observe, _, expect := externalPair(action)
			if observe.Exec == nil && observe.HTTP == nil {
				continue
			}
			code, err := client.RunOperation(ctx, observe)
			if err != nil {
				fail(id, fmt.Sprintf("playbook %s action %d: its check could not run live: %v", id, index+1, err))
				continue
			}
			if code != expect && !program.ObservationNeedsChange(action, code) {
				fail(id, fmt.Sprintf("playbook %s action %d: its read-only check %s exited %d live; it must exit %d when the state holds or the needs-change code when it does not", id, index+1, operationText(observe), code, expect))
			}
		}
	}
	return diagnostics
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
