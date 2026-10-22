package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// tryFastPath permits one mutation attempt per incident. Restart uses only live
// diagnosis and independent verification; a marker never authorizes rollback or
// supplies a cached successful result.
func (d KubernetesJobDispatcher) tryFastPath(ctx context.Context, request IncidentRequest, loop *ResponderLoop) (IncidentResult, string, bool, error) {
	if d.Client == nil {
		return IncidentResult{}, "", false, nil
	}
	name := IncidentJobName(request.IncidentID) + "-fast-path"
	maps := d.Client.CoreV1().ConfigMaps(d.Namespace)
	marker, err := maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}, Data: map[string]string{"state": "attempting"}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		marker, err = maps.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return IncidentResult{}, "", false, err
		}
		// Even an interrupted write consumed its attempt. Never reset the responder
		// budget when the separate attempt-state save was interrupted.
		var runs []ProgramRun
		if marker.Data["program_runs"] != "" {
			if err = json.Unmarshal([]byte(marker.Data["program_runs"]), &runs); err != nil {
				return IncidentResult{}, "", false, fmt.Errorf("decode fast-path diagnoses: %w", err)
			}
		}
		if len(runs) > 0 || marker.Data["program_ids"] != "" {
			if loop.batches < 1 {
				loop.batches = 1
			}
		}
		if marker.Data["state"] == "failed" {
			if err = loop.restoreFastPathTrustFailureSignals(marker.Data["trust_failure_signals"]); err != nil {
				return IncidentResult{}, "", false, err
			}
		}
		// Legacy markers have no bound diagnoses. They are handed to the responder;
		// their application, result and rollback journals are never decoded.
		attempt := loop.recheckFastPath(ctx, runs)
		if attempt.Resolved {
			result := fastPathResult(request, loop, attempt, time.Now().UTC())
			fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
			if err = validateIncidentResultFor(result, request); err != nil {
				return IncidentResult{}, "", false, err
			}
			return result, "", true, nil
		}
		if err = loop.restoreFastPathTrustFailureSignals(marker.Data["trust_failure_signals"]); err != nil {
			return IncidentResult{}, "", false, err
		}
		if err = loop.persistToolState(ctx); err != nil {
			return IncidentResult{}, "", false, err
		}
		return IncidentResult{}, "previous trusted program attempt requires responder live diagnosis: " + attempt.Summary, false, nil
	}
	if err != nil {
		return IncidentResult{}, "", false, fmt.Errorf("record fast-path attempt: %w", err)
	}
	save := func() error {
		updated, err := maps.Update(ctx, marker, metav1.UpdateOptions{})
		if err == nil {
			marker = updated
		}
		return err
	}
	minimum := d.ProgramTrustThreshold
	if minimum <= 0 {
		minimum = 3
	}
	loop.BeforeFastPathApply = func(ctx context.Context, runs []ProgramRun) error {
		ids := make([]string, 0, len(runs))
		for _, run := range runs {
			ids = append(ids, run.ProgramID)
		}
		encoded, err := json.Marshal(runs)
		if err != nil {
			return err
		}
		signals, err := json.Marshal(loop.fastPathTrustFailureSignals(ids))
		if err != nil {
			return err
		}
		marker.Data["program_runs"] = string(encoded)
		marker.Data["trust_failure_signals"] = string(signals)
		return save()
	}
	defer func() { loop.BeforeFastPathApply = nil }()
	started := time.Now().UTC()
	attempt := loop.TryTrustedFastPath(ctx, minimum)
	if !attempt.Attempted {
		log.Printf("incident %s trusted program fast path skipped: %s", request.IncidentID, attempt.SkippedReason)
	}
	if attempt.Resolved {
		result := fastPathResult(request, loop, attempt, started)
		fillProgramRevisions(ctx, result.ProgramRuns, request.RepositoryWorktree, request.SourceCommit)
		if err = validateIncidentResultFor(result, request); err != nil {
			return IncidentResult{}, "", false, err
		}
		marker.Data["state"] = "resolved"
		if err = loop.persistToolState(ctx); err != nil {
			return IncidentResult{}, "", false, err
		}
		if err = save(); err != nil {
			return IncidentResult{}, "", false, err
		}
		return result, "", true, nil
	}
	summary := attempt.Summary
	if summary == "" {
		summary = "trusted program fast path skipped: " + string(attempt.SkippedReason)
	}
	marker.Data["state"] = "failed"
	marker.Data["summary"] = summary
	if attempt.Attempted {
		if err = loop.restoreFastPathTrustFailureSignals(marker.Data["trust_failure_signals"]); err != nil {
			return IncidentResult{}, "", false, err
		}
	}
	if err = loop.persistToolState(ctx); err != nil {
		return IncidentResult{}, "", false, err
	}
	if err = save(); err != nil {
		return IncidentResult{}, "", false, err
	}
	return IncidentResult{}, summary, false, nil
}
