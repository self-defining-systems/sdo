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

// KubernetesPlanRollouts waits for touched Deployments to finish rolling out.
type KubernetesPlanRollouts struct {
	// Clock controls rollout polling; nil uses production timers.
	Clock  kclock.WithTicker
	Client kubernetes.Interface
}

// Observe checks touched Deployments once. Pending never consumes a rollout wait.
func (w KubernetesPlanRollouts) Observe(ctx context.Context, refs []sdk.ObjectRef) error {
	if w.Client == nil {
		return errors.New("Kubernetes rollout client is required")
	}
	started := lifecycleio.Now()
	var blockers []*contracts.RolloutBlocker
	stalled := 0
	for _, ref := range refs {
		if ref.Kind != "Deployment" || ref.APIVersion != "apps/v1" {
			continue
		}
		object, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("observe rollout %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		if !deployment.DeploymentRolloutComplete(object) {
			blocker, stuck, admission := w.rolloutBlocker(ctx, object, started)
			if admission != nil {
				return admission
			}
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

// Wait settles each Deployment on its own: it returns once every one has
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
			if ref.Kind != "Deployment" || ref.APIVersion != "apps/v1" {
				continue
			}
			deploymentObject, err := w.Client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("observe rollout %s/%s: %w", ref.Namespace, ref.Name, err)
			}
			if deployment.DeploymentRolloutComplete(deploymentObject) {
				delete(stalledSince, ref)
				continue
			}
			blocker, stuck, admission := w.rolloutBlocker(ctx, deploymentObject, started)
			if admission != nil {
				return admission
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
