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
	"sdo.dev/operator/sdk/program"
)

// tryFastPath records its one-attempt decision before mutating the cluster.
// A completed result is reusable when a controller rejoins the dispatch effect.
func (d KubernetesJobDispatcher) tryFastPath(ctx context.Context, request IncidentRequest, loop *ResponderLoop) (IncidentResult, string, bool, error) {
	if d.Client == nil {
		return IncidentResult{}, "", false, nil
	}
	name := IncidentJobName(request.IncidentID) + "-fast-path"
	maps := d.Client.CoreV1().ConfigMaps(d.Namespace)
	marker, err := maps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name},
		Data: map[string]string{"state": "attempting"}}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		marker, err = maps.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return IncidentResult{}, "", false, err
		}
		switch marker.Data["state"] {
		case "resolved":
			message := new(contracts.IncidentResult)
			if err := protojson.Unmarshal([]byte(marker.Data["result"]), message); err != nil {
				return IncidentResult{}, "", false, fmt.Errorf("decode durable fast-path result: %w", err)
			}
			result := incidentResultState(message)
			if err := validateIncidentResultFor(result, request); err != nil {
				return IncidentResult{}, "", false, err
			}
			return result, "", true, nil
		case "failed":
			loop.fastPathSkippedReason = FastPathSkipReason(marker.Data["skip_reason"])
			if loop.fastPathSkippedReason != "" {
				log.Printf("incident %s trusted program fast path skipped: %s", request.IncidentID, loop.fastPathSkippedReason)
			}
			return IncidentResult{}, marker.Data["summary"], false, nil
		default:
			summary := "previous trusted program fast path was interrupted before application"
			if marker.Data["preimages"] != "" {
				var preImages []PlanPreImage
				if err := json.Unmarshal([]byte(marker.Data["preimages"]), &preImages); err != nil {
					return IncidentResult{}, "", false, fmt.Errorf("decode interrupted fast-path pre-images: %w", err)
				}
				application := PlanApplicationResult{PreImages: preImages}
				for _, preImage := range preImages {
					application.ActionsApplied = append(application.ActionsApplied, program.ReconcileFromSource{Ref: preImage.Ref})
				}
				rollbackErr := loop.Executor.Rollback(ctx, application)
				summary = "previous trusted program fast path was interrupted; rollback true"
				if rollbackErr != nil {
					summary = "previous trusted program fast path was interrupted; rollback failed: " + rollbackErr.Error()
				}
				var ids []string
				if err := json.Unmarshal([]byte(marker.Data["program_ids"]), &ids); err != nil {
					return IncidentResult{}, "", false, fmt.Errorf("decode interrupted fast-path programs: %w", err)
				}
				loop.fastPathPrograms = ids
				for _, id := range ids {
					loop.addFailureSignal("fast_path_failure:" + id)
				}
			}
			marker.Data = map[string]string{"state": "failed", "summary": summary}
			if _, err := maps.Update(ctx, marker, metav1.UpdateOptions{}); err != nil {
				return IncidentResult{}, "", false, fmt.Errorf("record interrupted fast-path rollback: %w", err)
			}
			return IncidentResult{}, summary, false, nil
		}
	}
	if err != nil {
		return IncidentResult{}, "", false, fmt.Errorf("record fast-path attempt: %w", err)
	}
	started := time.Now().UTC()
	minimum := d.ProgramTrustThreshold
	if minimum <= 0 {
		minimum = 3
	}
	loop.BeforeFastPathApply = func(ctx context.Context, ids []string, preImages []PlanPreImage) error {
		encoded, err := json.Marshal(preImages)
		if err != nil {
			return err
		}
		programIDs, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		marker.Data = map[string]string{"state": "attempting", "preimages": string(encoded), "program_ids": string(programIDs)}
		var updated *corev1.ConfigMap
		updated, err = maps.Update(ctx, marker, metav1.UpdateOptions{})
		if err == nil {
			marker = updated
		}
		return err
	}
	attempt := loop.TryTrustedFastPath(ctx, minimum)
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
		marker.Data = map[string]string{"state": "resolved", "result": string(encoded)}
		if _, err := maps.Update(ctx, marker, metav1.UpdateOptions{}); err != nil {
			return IncidentResult{}, "", false, fmt.Errorf("record resolved fast path: %w", err)
		}
		return result, "", true, nil
	}
	summary := attempt.Summary
	if summary == "" {
		summary = "trusted program fast path skipped: " + string(attempt.SkippedReason)
	}
	marker.Data = map[string]string{"state": "failed", "summary": summary, "skip_reason": string(attempt.SkippedReason)}
	if _, err := maps.Update(ctx, marker, metav1.UpdateOptions{}); err != nil {
		return IncidentResult{}, "", false, fmt.Errorf("record fast-path handoff: %w", err)
	}
	return IncidentResult{}, summary, false, nil
}
