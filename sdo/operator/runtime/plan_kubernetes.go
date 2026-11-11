package runtime

import (
	"context"
	"errors"
	"fmt"
	kclock "k8s.io/utils/clock"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
)

// KubernetesPlanRollouts waits for touched Deployments, StatefulSets and
// DaemonSets to finish rolling out.
type KubernetesPlanRollouts struct {
	// Clock controls rollout polling; nil uses production timers.
	Clock  kclock.WithTicker
	Client kubernetes.Interface
}

// Observe checks touched workloads once. Pending never consumes a rollout wait.
func (w KubernetesPlanRollouts) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if w.Client == nil {
		return errors.New("Kubernetes rollout client is required")
	}
	started := lifecycleio.Now()
	var blockers []*contracts.RolloutBlocker
	stalled := 0
	for _, ref := range refs {
		blocker, stuck, err := w.rolloutState(ctx, ref, started)
		if err != nil {
			return err
		}
		if blocker != nil {
			blockers = append(blockers, blocker)
			if stuck {
				stalled++
			}
		}
	}
	if len(blockers) > 0 && stalled == len(blockers) {
		return &rolloutBlockedError{blockers: blockers, cause: errRolloutStalled}
	}
	if len(blockers) > 0 {
		return &rolloutBlockedError{blockers: blockers, cause: context.DeadlineExceeded}
	}
	return nil
}

// rolloutStallGrace lets transient scheduling failures (a node joining, a
// volume binding) clear before a stall is handed off.
const rolloutStallGrace = 2 * time.Second

// errRolloutStalled means waiting cannot complete a rollout; it never means
// the rollout failed or that its writes should be undone.
var errRolloutStalled = errors.New("rollout cannot progress without another actor")

// Wait settles each workload on its own: it returns once every one has
// completed or stalled, so a stalled rollout never spends the bounded wait.
func (w KubernetesPlanRollouts) Wait(ctx context.Context, refs []sdk.ObjectRef) error {
	if w.Client == nil {
		return errors.New("Kubernetes rollout client is required")
	}
	ticker := timerClock(w.Clock).NewTicker(time.Second)
	defer ticker.Stop()
	started := lifecycleio.Now()
	stalledSince := map[sdk.ObjectRef]time.Time{}
	for {
		var blockers, stalled []*contracts.RolloutBlocker
		for _, ref := range refs {
			blocker, stuck, err := w.rolloutState(ctx, ref, started)
			if err != nil {
				return err
			}
			if blocker == nil {
				delete(stalledSince, ref)
				continue
			}
			blockers = append(blockers, blocker)
			if !stuck {
				delete(stalledSince, ref)
				continue
			}
			if stalledSince[ref].IsZero() {
				stalledSince[ref] = lifecycleio.Now()
			}
			if lifecycleio.Since(stalledSince[ref]) >= rolloutStallGrace {
				stalled = append(stalled, blocker)
			}
		}
		if len(blockers) == 0 {
			return nil
		}
		if len(stalled) == len(blockers) {
			return &rolloutBlockedError{blockers: stalled, cause: errRolloutStalled}
		}
		select {
		case <-ctx.Done():
			return &rolloutBlockedError{blockers: blockers, cause: ctx.Err()}
		case <-ticker.C():
		}
	}
}

// rolloutState reads one touched object. A nil blocker means it is not a
// workload with a rollout or its rollout is complete. Only a Deployment's
// ReplicaSet and Pod join can prove a stall; a StatefulSet or DaemonSet
// rollout is reported pending until it completes or the budget ends.
func (w KubernetesPlanRollouts) rolloutState(ctx context.Context, ref sdk.ObjectRef, started time.Time) (*contracts.RolloutBlocker, bool, error) {
	if ref.APIVersion != "apps/v1" {
		return nil, false, nil
	}
	switch ref.Kind {
	case "Deployment":
		object, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, false, fmt.Errorf("observe rollout %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		if deployment.DeploymentRolloutComplete(object) {
			return nil, false, nil
		}
		blocker, stuck, admission := w.rolloutBlocker(ctx, object, started)
		if admission != nil {
			return nil, false, admission
		}
		return blocker, stuck, nil
	case "StatefulSet":
		object, err := w.Client.AppsV1().StatefulSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, false, fmt.Errorf("observe rollout %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		if deployment.StatefulSetRolloutComplete(object) {
			return nil, false, nil
		}
		desired := int32(1)
		if object.Spec.Replicas != nil {
			desired = *object.Spec.Replicas
		}
		return &contracts.RolloutBlocker{Ref: planRefProto(ref), Reason: fmt.Sprintf("%d/%d updated, %d ready; revision %s of %s; observed generation %d/%d", object.Status.UpdatedReplicas, desired, object.Status.ReadyReplicas, object.Status.CurrentRevision, object.Status.UpdateRevision, object.Status.ObservedGeneration, object.Generation)}, false, nil
	case "DaemonSet":
		object, err := w.Client.AppsV1().DaemonSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, false, fmt.Errorf("observe rollout %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		if deployment.DaemonSetRolloutComplete(object) {
			return nil, false, nil
		}
		return &contracts.RolloutBlocker{Ref: planRefProto(ref), Reason: fmt.Sprintf("%d/%d scheduled nodes updated, %d available; observed generation %d/%d", object.Status.UpdatedNumberScheduled, object.Status.DesiredNumberScheduled, object.Status.NumberAvailable, object.Status.ObservedGeneration, object.Generation)}, false, nil
	}
	return nil, false, nil
}
