package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// maxApplyAttempts bounds how often one write is retried against a fresh
	// resourceVersion while a controller keeps updating the object.
	maxApplyAttempts = 5
	applyBackoffBase = 20 * time.Millisecond
)

// patchWithRetry writes one planned patch under a uid/resourceVersion precondition.
// A controller bumps resourceVersion for status, conditions and annotations even
// while it rolls out the object, so a conflict is retried against a fresh read as
// long as the object keeps its uid and every field the patch owns still holds the
// value the plan was computed from. baseline is the object the patch was computed
// against. A changed owned field or a new uid ends the application.
func (e PlanExecutor) patchWithRetry(ctx context.Context, index int, patch *PlanPatch, baseline *unstructured.Unstructured) error {
	var err error
	for attempt := 1; attempt <= maxApplyAttempts; attempt++ {
		if err = e.Client.Patch(ctx, patch.Ref, types.MergePatchType, patch.Patch); err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) {
			return fmt.Errorf("apply action %d to %s/%s: %w", index, patch.Ref.Kind, patch.Ref.Name, err)
		}
		if attempt == maxApplyAttempts {
			break
		}
		fresh, getErr := e.Client.Get(ctx, patch.Ref)
		if getErr != nil {
			return fmt.Errorf("re-read %s/%s after conflict: %w", patch.Ref.Kind, patch.Ref.Name, getErr)
		}
		if baseline != nil && fresh.GetUID() != baseline.GetUID() {
			return fmt.Errorf("apply action %d: %s/%s was recreated", index, patch.Ref.Kind, patch.Ref.Name)
		}
		if baseline != nil {
			if mismatch, found, checkErr := findPlanOwnedFieldChange(patch.Patch, baseline, fresh); checkErr != nil {
				return checkErr
			} else if found {
				return fmt.Errorf("concurrent change before action %d: %s", index, mismatch)
			}
		}
		if patch.Patch, err = conditionalPlanPatch(patch.Patch, fresh); err != nil {
			return err
		}
		if err = e.sleep(ctx, applyBackoff(attempt)); err != nil {
			return err
		}
	}
	return fmt.Errorf("apply action %d to %s/%s: resourceVersion kept changing for %d attempts: %w", index, patch.Ref.Kind, patch.Ref.Name, maxApplyAttempts, err)
}

// applyBackoff grows linearly with the attempt and carries jitter so concurrent
// executors do not retry in step.
func applyBackoff(attempt int) time.Duration {
	base := time.Duration(attempt) * applyBackoffBase
	return base + time.Duration(rand.Int64N(int64(base)))
}

func (e PlanExecutor) sleep(ctx context.Context, d time.Duration) error {
	if e.Sleep != nil {
		return e.Sleep(ctx, d)
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// planFieldChange names a field the plan owns whose live value left its pre-image.
type planFieldChange struct {
	Path string
	From any
	To   any
}

func (c planFieldChange) String() string {
	return fmt.Sprintf("%s changed from %s to %s", c.Path, planValueText(c.From), planValueText(c.To))
}

// findPlanOwnedFieldChange reports whether a field the merge patch writes differs
// between the object it was computed from and the live object. Metadata and
// status are not the plan's; every other top-level key the patch carries is.
func findPlanOwnedFieldChange(patch []byte, baseline, live *unstructured.Unstructured) (planFieldChange, bool, error) {
	var document map[string]any
	if err := json.Unmarshal(patch, &document); err != nil {
		return planFieldChange{}, false, err
	}
	delete(document, "metadata")
	delete(document, "status")
	owned := planOwnedPreImage(document, baseline.Object)
	mismatch, found := findPlanOwnedFieldMismatch("", live.Object, owned)
	if !found {
		return planFieldChange{}, false, nil
	}
	return planFieldChange{Path: mismatch.Path[1:], From: mismatch.Written, To: mismatch.Live}, true, nil
}

// planOwnedPreImage projects the baseline onto the keys a merge patch writes.
// Values the patch replaces or deletes are owned whole; nested maps it merges
// into are owned key by key. A key the baseline lacks is owned as absent.
func planOwnedPreImage(patch, baseline map[string]any) map[string]any {
	owned := make(map[string]any, len(patch))
	for key, value := range patch {
		written, writtenMap := value.(map[string]any)
		base, baseMap := baseline[key].(map[string]any)
		if writtenMap && baseMap {
			owned[key] = planOwnedPreImage(written, base)
			continue
		}
		owned[key] = baseline[key]
	}
	return owned
}
