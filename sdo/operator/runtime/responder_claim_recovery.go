package runtime

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
)

// beforeFirstResponderClaim is shared by ordinary, prepared/restarted and
// verification-follow-up dispatch. Frozen task bytes alone are not a claim.
// Already-owned workers always rejoin; only a never-started worker can be skipped.
func (d KubernetesJobDispatcher) beforeFirstResponderClaim(ctx context.Context, request IncidentRequest, task *contracts.AgentTask, loop *ResponderLoop) (*IncidentState, error) {
	if loop == nil || deterministicStageReport(request.PriorStages) == nil {
		return nil, nil
	}
	if d.Client != nil {
		names, err := worker_registry.IncidentExecutionNames(ctx, d.Client, d.Namespace, d.RepositoryMountPath, request.IncidentID, false)
		if err != nil {
			return nil, fmt.Errorf("observe responder ownership: %w", err)
		}
		// The inventory always contains the legacy task name. Additional names
		// come from durable bindings/execution identities, including vanished
		// workers whose result must still be recovered.
		if len(names) > 1 {
			return nil, nil
		}
		names = append(names, worker_registry.TaskJobName(task))
		for _, name := range names {
			_, err := d.Client.BatchV1().Jobs(d.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err == nil {
				return nil, nil
			}
			if !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("observe responder claim: %w", err)
			}
			for _, suffix := range []string{"-request", "-result", "-execution"} {
				_, err := d.Client.CoreV1().ConfigMaps(d.Namespace).Get(ctx, name+suffix, metav1.GetOptions{})
				if err == nil {
					return nil, nil
				}
				if !apierrors.IsNotFound(err) {
					return nil, fmt.Errorf("observe published responder task: %w", err)
				}
			}
		}
	}

	loop.operationMu.Lock()
	defer loop.operationMu.Unlock()
	loop.mu.Lock()
	defer loop.mu.Unlock()
	if loop.menuSession.GetProviderSessionId() != "" || loop.continuation.Attempts > 0 || loop.continuation.Result != nil {
		return nil, nil
	}
	report := proto.Clone(deterministicStageReport(request.PriorStages)).(*contracts.StageReport)
	progress := loop.stageProgress["deterministic"]
	if progress.Report != nil {
		report = proto.Clone(progress.Report).(*contracts.StageReport)
	}
	applied := false
	for _, attempt := range report.Mitigations {
		applied = applied || attempt.GetApplyReceipt().GetApplied()
	}
	if !applied {
		return nil, nil
	}
	state := &IncidentState{Request: request, Findings: append([]sdk.Finding(nil), request.Findings...), Coverage: cloneCoverage(request.PlaybookCoverage), PriorStages: cloneStageReports(request.PriorStages)}
	for i, prior := range state.PriorStages {
		if prior.Stage == "deterministic" {
			state.PriorStages[i] = report
		}
	}
	response := ToolResponse{}
	loop.verify(ctx, nil, &response, true)
	updateStageFindings(state, response)
	if response.Error != "" {
		return nil, fmt.Errorf("observe saved recovery before responder claim: %s", response.Error)
	}
	_, refreshed := loop.settleRecoveryApplicationsLocked(ctx, IncidentResult{PlanApplications: loop.applicationEvidenceLocked()}, state.PriorStages)
	if updated := deterministicStageReport(refreshed); updated != nil {
		proto.Reset(report)
		proto.Merge(report, updated)
	}
	stage := &DeterministicResponseStage{}
	// Old checkpoints lacking the private deadline cannot mint a fresh wait on
	// every dispatch. Bound observation from their last immutable write receipt.
	if progress.RecoveryDeadline.IsZero() {
		for _, attempt := range report.Mitigations {
			if at := attempt.GetApplyReceipt().GetCompletedAt(); at != nil && at.AsTime().After(progress.RecoveryDeadline) {
				progress.RecoveryDeadline = at.AsTime()
			}
		}
		if !progress.RecoveryDeadline.IsZero() {
			progress.RecoveryDeadline = progress.RecoveryDeadline.Add(DefaultMitigationRolloutTimeout)
		} else {
			// Missing historical timing is not permission for a new wait.
			progress.RecoveryDeadline = time.Now()
		}
	}
	complete := progress.Complete
	save := func(bool) error {
		progress.Report = report
		progress.Complete = complete
		progress.Coverage = cloneCoverage(state.Coverage)
		loop.stageProgress["deterministic"] = progress
		report.RemainingFindingFingerprints = findingFingerprints(state.Findings)
		return loop.persistToolState(ctx)
	}
	if err := stage.awaitApplications(ctx, loop, state, report, &progress, save); err != nil {
		return nil, err
	}
	state.ProgramToolCalls = append([]ToolCall(nil), loop.calls...)
	state.RepairActions = ProgramRepairReceipts(loop.applicationEvidenceLocked(), nil)
	return state, nil
}

func automaticClaimResult(state *IncidentState) (IncidentResult, error) {
	result, err := responsePipelineResult(state, time.Now().UTC(), time.Now().UTC())
	result.automaticStages = cloneStageReports(state.PriorStages)
	return result, err
}
