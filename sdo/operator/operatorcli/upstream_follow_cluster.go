package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/install"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/upstream"
)

// configMapHolds persists follower holds in the control namespace's
// sdo-upstream-holds ConfigMap (upstream.EncodeHolds owns the data format).
type configMapHolds struct {
	Client    kubernetes.Interface
	Namespace string
}

func (s configMapHolds) Load(ctx context.Context) ([]upstream.Hold, error) {
	configMap, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, upstream.HoldsConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return upstream.DecodeHolds(configMap.Data)
}

func (s configMapHolds) Save(ctx context.Context, holds []upstream.Hold) error {
	data, err := upstream.EncodeHolds(holds)
	if err != nil {
		return err
	}
	configMaps := s.Client.CoreV1().ConfigMaps(s.Namespace)
	want := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: upstream.HoldsConfigMapName, Namespace: s.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Data: data}
	prior, err := configMaps.Get(ctx, upstream.HoldsConfigMapName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		_, err = configMaps.Create(ctx, want, metav1.CreateOptions{})
	case err == nil:
		want.ResourceVersion = prior.ResourceVersion
		_, err = configMaps.Update(ctx, want, metav1.UpdateOptions{})
	}
	return err
}

// advanceMaintenancePrefix marks maintenance generations the follower wrote,
// so a pause it left behind (the parent died mid-advance) is recognized and
// a pause someone else declared is never lifted by an advance.
const advanceMaintenancePrefix = "upstream-advance-"

// advanceMaintenance pauses and resumes controller evaluation around an
// advance through the existing maintenance ConfigMap (install.SetMaintenance).
// The pause stops observing the application namespace, so the rollout is
// never an incident and the controller relists the new state on resume.
type advanceMaintenance struct {
	Client    kubernetes.Interface
	Namespace string
	Now       func() time.Time
}

func (m advanceMaintenance) generation(state string) string {
	return fmt.Sprintf("%s%s-%d", advanceMaintenancePrefix, state, m.Now().UnixNano())
}

// owner reports whether the maintenance ConfigMap is paused and whether the
// follower wrote its current generation.
func (m advanceMaintenance) state(ctx context.Context) (paused, ours bool, err error) {
	configMap, err := m.Client.CoreV1().ConfigMaps(m.Namespace).Get(ctx, install.MaintenanceConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("read controller maintenance: %w", err)
	}
	paused = strings.TrimSpace(configMap.Data["state"]) == "paused"
	return paused, strings.HasPrefix(strings.TrimSpace(configMap.Data["generation"]), advanceMaintenancePrefix), nil
}

// Pause refuses (an error: the follower retries next poll) while someone
// else holds the controller in maintenance.
func (m advanceMaintenance) Pause(ctx context.Context, _ string) error {
	paused, ours, err := m.state(ctx)
	if err != nil {
		return err
	}
	if paused && !ours {
		return errors.New("controller maintenance is paused by another owner")
	}
	return install.SetMaintenance(ctx, m.Client, m.Namespace, true, m.generation("paused"))
}

func (m advanceMaintenance) Resume(ctx context.Context) error {
	return install.SetMaintenance(ctx, m.Client, m.Namespace, false, m.generation("resumed"))
}

// RecoverStalePause resumes a pause an earlier follower process left behind.
// A newly started parent has no advance in flight, so such a pause only
// stops detection.
func (m advanceMaintenance) RecoverStalePause(ctx context.Context) (bool, error) {
	paused, ours, err := m.state(ctx)
	if err != nil || !paused || !ours {
		return false, err
	}
	return true, m.Resume(ctx)
}

// controllerActivity reads what the running controller child persisted and
// reports work an advance must not overlap: an open incident, a closure in
// flight, queued reflection and a pending namespace onboarding.
type controllerActivity struct {
	Client    kubernetes.Interface
	Namespace string // control namespace
	// Application is the application namespace an onboarding is keyed by.
	Application string
}

// controllerStateConfigMap and controllerStateKey locate the child's
// persisted runtime state (runtime's lifecycleio ConfigMap state store).
const (
	controllerStateConfigMap = "sdo-controller-state"
	controllerStateKey       = "runtime-state.json"
)

func (a controllerActivity) Busy(ctx context.Context) (string, bool, error) {
	configMap, err := a.Client.CoreV1().ConfigMaps(a.Namespace).Get(ctx, controllerStateConfigMap, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return "", false, fmt.Errorf("read controller state: %w", err)
	default:
		payload, ok := configMap.Data[controllerStateKey]
		if !ok {
			return "", false, fmt.Errorf("controller state ConfigMap has no %s", controllerStateKey)
		}
		state := &contracts.ControllerState{}
		// A read-only projection, like the controller observation: the
		// runtime validated the document when it wrote it.
		if err := protojson.Unmarshal([]byte(payload), state); err != nil {
			return "", false, fmt.Errorf("decode controller state: %w", err)
		}
		if reason, busy := stateBusy(state); busy {
			return reason, true, nil
		}
	}
	return a.onboardingBusy(ctx)
}

func stateBusy(state *contracts.ControllerState) (string, bool) {
	if state.GetIncidentOpen() {
		return fmt.Sprintf("incident %s is open", state.GetIncidentRequest().GetIncidentId()), true
	}
	switch state.GetClosureState() {
	case contracts.ClosureState_CLOSURE_STATE_PENDING, contracts.ClosureState_CLOSURE_STATE_PROCESSING, contracts.ClosureState_CLOSURE_STATE_COMMITTED, contracts.ClosureState_CLOSURE_STATE_ACKNOWLEDGING:
		return fmt.Sprintf("closure of incident %s is in flight", state.GetPendingClosure().GetRequest().GetIncidentId()), true
	}
	if len(state.GetQueuedReflectionIncidentIds()) > 0 || state.GetQueuedReflectionsUnknown() {
		return "reflection is queued", true
	}
	return "", false
}

// onboardingBusy reports a staged namespace onboarding whose single install
// evaluation has neither completed nor been rejected.
func (a controllerActivity) onboardingBusy(ctx context.Context) (string, bool, error) {
	pending, err := onboarding.Pending(ctx, a.Client, a.Namespace, a.Application)
	if err != nil || pending == nil {
		return "", false, err
	}
	completed, err := onboarding.Load(ctx, a.Client, a.Namespace, a.Application)
	if err != nil || completed != nil {
		return "", false, err
	}
	for _, name := range []string{onboarding.RejectedConfigMapName, onboarding.WindowConfigMapName} {
		marked, err := onboarding.Marked(ctx, a.Client, a.Namespace, name)
		if err != nil || marked {
			return "", false, err
		}
	}
	return "namespace onboarding is pending", true, nil
}
