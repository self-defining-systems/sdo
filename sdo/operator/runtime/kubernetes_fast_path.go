package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// tryFastPath records its one-attempt decision before mutation. Failed markers
// retain the complete attempt and pre-images until compensation is durable.
func (d KubernetesJobDispatcher) tryFastPath(ctx context.Context, request IncidentRequest, loop *ResponderLoop) (IncidentResult, string, bool, error) {
	if d.Client == nil {
		return IncidentResult{}, "", false, nil
	}
	name := IncidentJobName(request.IncidentID) + "-fast-path"
	maps := d.Client.CoreV1().ConfigMaps(d.Namespace)
	marker, err := maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"state": "attempting"}}, metav1.CreateOptions{})
	existing := apierrors.IsAlreadyExists(err)
	if existing {
		marker, err = maps.Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		return IncidentResult{}, "", false, fmt.Errorf("record fast-path attempt: %w", err)
	}
	// Update never replaces Data: trust evidence and pre-images survive every state.
	save := func(ctx context.Context) error {
		snapshot, err := json.Marshal(loop.toolState())
		if err != nil {
			return err
		}
		marker.Data["tool_state"] = string(snapshot)
		updated, err := maps.Update(ctx, marker, metav1.UpdateOptions{})
		if err == nil {
			marker = updated
		}
		return err
	}
	if existing {
		if marker.Data["state"] == "resolved" {
			for _, entry := range loop.applicationsLog {
				if entry.ID == "fast-path-1" && (entry.Pending || entry.RolledBack || entry.Result.RolledBack || entry.Result.RollingBack || entry.Result.PendingAction != nil) {
					return IncidentResult{}, "", false, fmt.Errorf("resolved fast-path marker conflicts with application journal")
				}
			}
			message := new(contracts.IncidentResult)
			if err := protojson.Unmarshal([]byte(marker.Data["result"]), message); err != nil {
				return IncidentResult{}, "", false, fmt.Errorf("decode durable fast-path result: %w", err)
			}
			result := incidentResultState(message)
			if err := validateIncidentResultFor(result, request); err != nil {
				return IncidentResult{}, "", false, err
			}
			return result, "", true, nil
		}
		if err := restoreFastPathMarker(loop, marker.Data); err != nil {
			return IncidentResult{}, "", false, err
		}
		summary := marker.Data["summary"]
		if summary == "" {
			summary = "previous trusted program fast path was interrupted before application"
			if marker.Data["preimages"] != "" && marker.Data["application"] == "" {
				summary = "previous trusted program fast path was interrupted; rollback requires missing field journal"
			}
		}
		for i := range loop.applicationsLog {
			entry := &loop.applicationsLog[i]
			if entry.ID != "fast-path-1" {
				continue
			}
			if !entry.RolledBack && !entry.Result.RolledBack && (entry.Result.Applied || entry.Result.PendingAction != nil || len(entry.Result.ActionsApplied) > 0) {
				executor := loop.applicationExecutor(entry.ID)
				result, recoveryErr := executor.Reconcile(ctx, entry.Result)
				if recoveryErr == nil {
					recoveryErr = executor.rollback(ctx, &result)
				}
				if recoveryErr != nil {
					result.RollbackError = recoveryErr.Error()
					result.RollingBack = true
					summary = "previous trusted program fast path failed; rollback failed: " + recoveryErr.Error()
				} else {
					result.RolledBack = true
					result.RollingBack = false
					result.RollbackError = ""
					summary = "previous trusted program fast path failed; rollback complete"
				}
				// A checkpoint can update the entry, so reacquire it after recovery.
				loop.applicationsLog[i].Result = result
				loop.applicationsLog[i].RolledBack = result.RolledBack
				loop.applicationsLog[i].RollbackError = result.RollbackError
				loop.applicationsLog[i].Pending = recoveryErr != nil
				loop.applications["fast-path-1"] = result
			}
		}
		marker.Data["state"] = "failed"
		marker.Data["summary"] = summary
		if err := loop.persistToolState(ctx); err != nil {
			return IncidentResult{}, "", false, err
		}
		if err := save(ctx); err != nil {
			return IncidentResult{}, "", false, fmt.Errorf("record fast-path recovery: %w", err)
		}
		return IncidentResult{}, summary, false, nil
	}
	started := time.Now().UTC()
	minimum := d.ProgramTrustThreshold
	if minimum <= 0 {
		minimum = 3
	}
	loop.BeforeFastPathApply = func(ctx context.Context, ids []string, preImages []PlanPreImage) error {
		images, err := json.Marshal(preImages)
		if err != nil {
			return err
		}
		encodedIDs, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		// Group B's marker wire contract: revision-bound suspicion is frozen before
		// writes, retained on failure, and excluded from successful/skipped outcomes.
		encodedSignals, err := json.Marshal(loop.fastPathTrustFailureSignals(ids))
		if err != nil {
			return err
		}
		marker.Data["preimages"] = string(images)
		marker.Data["program_ids"] = string(encodedIDs)
		marker.Data["trust_failure_signals"] = string(encodedSignals)
		return save(ctx)
	}
	var checkpointed PlanApplicationResult
	previousCheckpoint := loop.Executor.Checkpoint
	loop.Executor.Checkpoint = func(ctx context.Context, result PlanApplicationResult) error {
		checkpointed = result
		if previousCheckpoint != nil {
			if err := previousCheckpoint(ctx, result); err != nil {
				return err
			}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return err
		}
		marker.Data["application"] = string(encoded)
		return save(ctx)
	}
	defer func() { loop.Executor.Checkpoint = previousCheckpoint }()
	attempt := loop.TryTrustedFastPath(ctx, minimum)
	// The fast-path loop uses the error-only Rollback API. Retain the executor's
	// advanced compensation cursor when that loop finalizes its value copy.
	for i := range loop.applicationsLog {
		entry := &loop.applicationsLog[i]
		if entry.ID == "fast-path-1" && checkpointed.RollbackCompleted > entry.Result.RollbackCompleted {
			entry.Result.RollbackCompleted = checkpointed.RollbackCompleted
			entry.Result.RollingBack = checkpointed.RollingBack
			entry.Result.RolledBack = checkpointed.RolledBack
			entry.RolledBack = checkpointed.RolledBack
			loop.applications[entry.ID] = entry.Result
		}
	}
	if !attempt.Attempted {
		log.Printf("incident %s trusted program fast path skipped: %s", request.IncidentID, attempt.SkippedReason)
	}
	if attempt.Resolved {
		result := fastPathResult(request, loop, attempt, started)
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		if err := validateIncidentResultFor(result, request); err != nil {
			return IncidentResult{}, "", false, fmt.Errorf("validate fast-path result: %w", err)
		}
		message, err := incidentResultContract(result)
		if err != nil {
			return IncidentResult{}, "", false, err
		}
		encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
		if err != nil {
			return IncidentResult{}, "", false, err
		}
		// Finalization clears Pending and records verification/tool receipts. Make
		// that journal durable before resolution can become visible to a rejoin.
		if err := loop.persistToolState(ctx); err != nil {
			return IncidentResult{}, "", false, err
		}
		marker.Data["state"] = "resolved"
		marker.Data["result"] = string(encoded)
		if err := save(ctx); err != nil {
			return IncidentResult{}, "", false, fmt.Errorf("record resolved fast path: %w", err)
		}
		return result, "", true, nil
	}
	summary := attempt.Summary
	if summary == "" {
		summary = "trusted program fast path skipped: " + string(attempt.SkippedReason)
	}
	marker.Data["state"] = "failed"
	marker.Data["summary"] = summary
	marker.Data["skip_reason"] = string(attempt.SkippedReason)
	if attempt.Attempted {
		if err := loop.restoreFastPathTrustFailureSignals(marker.Data["trust_failure_signals"]); err != nil {
			return IncidentResult{}, "", false, err
		}
	}
	if err := loop.persistToolState(ctx); err != nil {
		return IncidentResult{}, "", false, err
	}
	if err := save(ctx); err != nil {
		return IncidentResult{}, "", false, fmt.Errorf("record fast-path handoff: %w", err)
	}
	return IncidentResult{}, summary, false, nil
}

func restoreFastPathMarker(loop *ResponderLoop, data map[string]string) error {
	if data["tool_state"] != "" {
		restored := NewResponderLoop(loop.Request, nil, nil, nil, PlanExecutor{}, loop.MaxBatches)
		if err := restored.restoreToolSnapshot([]byte(data["tool_state"])); err != nil {
			return err
		}
		// The incident tool journal may include newer responder work. Merge only
		// missing fast-path evidence instead of replacing the newer tool state.
		for _, entry := range restored.applicationsLog {
			if entry.ID != "fast-path-1" {
				continue
			}
			found := false
			for _, known := range loop.applicationsLog {
				found = found || known.ID == entry.ID
			}
			if !found {
				loop.applicationsLog = append(loop.applicationsLog, entry)
				loop.applications[entry.ID] = entry.Result
			}
		}
		for _, signal := range restored.failureSignals {
			loop.addFailureSignal(signal)
		}
		if loop.batches < restored.batches {
			loop.batches = restored.batches
		}
		loop.fastPathPrograms = restored.fastPathPrograms
		loop.fastPathRelationCoveredFindings = restored.fastPathRelationCoveredFindings
		if len(loop.runs) == 0 {
			loop.runs = restored.runs
		}
	}
	var ids []string
	if data["program_ids"] != "" {
		if err := json.Unmarshal([]byte(data["program_ids"]), &ids); err != nil {
			return fmt.Errorf("decode fast-path program IDs: %w", err)
		}
	}
	if len(ids) > 0 {
		loop.fastPathPrograms = ids
		if loop.batches == 0 {
			loop.batches = 1
		}
	}
	if data["application"] != "" {
		found := false
		for _, entry := range loop.applicationsLog {
			found = found || entry.ID == "fast-path-1"
		}
		if !found {
			var application PlanApplicationResult
			if err := json.Unmarshal([]byte(data["application"]), &application); err != nil {
				return err
			}
			loop.applications["fast-path-1"] = application
			loop.applicationsLog = append(loop.applicationsLog, RecordedPlanApplication{ID: "fast-path-1", ProgramIDs: ids, Result: application, Error: data["summary"], RollbackError: application.RollbackError, RolledBack: application.RolledBack})
		}
	}
	loop.fastPathSkippedReason = FastPathSkipReason(data["skip_reason"])
	if err := loop.restoreFastPathTrustFailureSignals(data["trust_failure_signals"]); err != nil {
		return err
	}
	if data["trust_failure_signals"] == "" {
		for _, id := range ids {
			loop.addFailureSignal("fast_path_failure:" + id)
		}
	}
	return nil
}

// restoreFastPathResolution reconciles a published resolution with the tool
// journal before restart recovery can mutate the application. Only the pending
// fast-path entry is superseded; newer responder work and recovery are retained.
func (d KubernetesJobDispatcher) restoreFastPathResolution(ctx context.Context, request IncidentRequest, loop *ResponderLoop) error {
	marker, err := d.Client.CoreV1().ConfigMaps(d.Namespace).Get(ctx, IncidentJobName(request.IncidentID)+"-fast-path", metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read fast-path resolution: %w", err)
	}
	if marker.Data["state"] != "resolved" {
		return nil
	}
	message := new(contracts.IncidentResult)
	if err := protojson.Unmarshal([]byte(marker.Data["result"]), message); err != nil {
		return fmt.Errorf("decode durable fast-path result: %w", err)
	}
	if err := validateIncidentResultFor(incidentResultState(message), request); err != nil {
		return err
	}
	finalized := NewResponderLoop(request, nil, nil, nil, PlanExecutor{}, loop.MaxBatches)
	if err := finalized.restoreToolSnapshot([]byte(marker.Data["tool_state"])); err != nil {
		return err
	}
	for _, entry := range finalized.applicationsLog {
		if entry.ID != "fast-path-1" {
			continue
		}
		if entry.Pending || entry.RolledBack || entry.Result.RolledBack || entry.Result.RollingBack || entry.Result.PendingAction != nil || !entry.Result.VerificationCompleted || !entry.Result.VerificationPassed {
			return fmt.Errorf("resolved fast-path marker has no finalized successful application")
		}
		for i, known := range loop.applicationsLog {
			if known.ID != entry.ID {
				continue
			}
			if known.RolledBack || known.Result.RolledBack || known.Result.RollingBack || known.Result.RollbackCompleted > 0 {
				return fmt.Errorf("resolved fast-path marker conflicts with application recovery")
			}
			if !known.Pending && known.Result.PendingAction == nil {
				return nil
			}
			loop.applicationsLog[i] = entry
			loop.applications[entry.ID] = entry.Result
			return loop.persistToolState(ctx)
		}
		loop.applicationsLog = append(loop.applicationsLog, entry)
		loop.applications[entry.ID] = entry.Result
		return loop.persistToolState(ctx)
	}
	return fmt.Errorf("resolved fast-path marker is missing its application journal")
}
