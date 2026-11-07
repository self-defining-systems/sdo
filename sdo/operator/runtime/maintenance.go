package runtime

import (
	"context"
	"fmt"
	kclock "k8s.io/utils/clock"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// MaintenanceConfigMapName is the operator-owned ConfigMap, in the controller's
// own namespace, that pauses and resumes observation of the application.
const MaintenanceConfigMapName = "sdo-controller-maintenance"

const (
	maintenanceStateKey      = "state"
	maintenanceGenerationKey = "generation"
	maintenanceStatePaused   = "paused"
	maintenanceStateActive   = "active"
)

// MaintenanceState is the operator's declared observation mode. While paused
// (for example during a planned application redeploy), the controller keeps
// finishing its in-flight incident lifecycle (closure, reflection,
// acknowledgment) but stops evaluating detectors. By default it also stops
// observing the application namespace, so an application that is intentionally absent is
// never treated as an incident. A new generation marks each resume so external
// observers can correlate the first evaluation after it.
// PreserveBaseline instead retains informers and their pending changes until resume.
type MaintenanceState struct {
	Paused     bool
	Generation string
	// PreserveBaseline suspends observation without adopting changes made while paused.
	PreserveBaseline bool
}

// Record is the controller log line that announces the mode it operates under.
func (state MaintenanceState) Record() map[string]any {
	mode := maintenanceStateActive
	if state.Paused {
		mode = maintenanceStatePaused
	}
	return map[string]any{"controller_maintenance": mode, "maintenance_generation": state.Generation}
}

type MaintenanceWatcher struct {
	// Clock controls polling; nil uses production timers.
	Clock     kclock.WithTicker
	Client    kubernetes.Interface
	Namespace string
	Name      string
}

// Read returns the declared mode; an absent ConfigMap means active operation.
func (w MaintenanceWatcher) Read(ctx context.Context) (MaintenanceState, error) {
	if w.Client == nil || w.Namespace == "" || w.Name == "" {
		return MaintenanceState{}, fmt.Errorf("maintenance watcher requires a client, namespace, and ConfigMap name")
	}
	configMap, err := w.Client.CoreV1().ConfigMaps(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return MaintenanceState{}, nil
	}
	if err != nil {
		return MaintenanceState{}, fmt.Errorf("read maintenance ConfigMap %s/%s: %w", w.Namespace, w.Name, err)
	}
	generation := strings.TrimSpace(configMap.Data[maintenanceGenerationKey])
	switch strings.TrimSpace(configMap.Data[maintenanceStateKey]) {
	case "", maintenanceStateActive:
		return MaintenanceState{Generation: generation}, nil
	case maintenanceStatePaused:
		preserve := strings.TrimSpace(configMap.Data["preserve_baseline"])
		if preserve != "" && preserve != "false" && preserve != "true" {
			return MaintenanceState{}, fmt.Errorf("invalid preserve_baseline %q", preserve)
		}
		return MaintenanceState{Paused: true, Generation: generation, PreserveBaseline: preserve == "true"}, nil
	default:
		return MaintenanceState{}, fmt.Errorf(
			"maintenance ConfigMap %s/%s has unsupported state %q",
			w.Namespace, w.Name, configMap.Data[maintenanceStateKey],
		)
	}
}

// Watch polls the ConfigMap and emits every declared mode that differs from the
// previously emitted one. Read errors are reported and the last mode is kept.
func (w MaintenanceWatcher) Watch(
	ctx context.Context,
	initial MaintenanceState,
	interval time.Duration,
	onError func(error),
) <-chan MaintenanceState {
	if interval <= 0 {
		interval = time.Second
	}
	changes := make(chan MaintenanceState)
	go func() {
		defer close(changes)
		last := initial
		ticker := timerClock(w.Clock).NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C():
			}
			state, err := w.Read(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if onError != nil {
					onError(err)
				}
				continue
			}
			if state == last {
				continue
			}
			select {
			case changes <- state:
				last = state
			case <-ctx.Done():
				return
			}
		}
	}()
	return changes
}

// MaintenanceAckConfigMapName is the controller-owned ConfigMap, in the control
// namespace, where the controller records the pause generation it stopped
// under. It is the other half of the pause barrier: the maintenance ConfigMap
// declares, this one confirms. The operator side names it install.MaintenanceAckConfigMap.
const MaintenanceAckConfigMapName = "sdo-controller-maintenance-ack"

const maintenanceAckGenerationKey = "generation"

// MaintenanceAcker records and reads the controller's pause acknowledgement.
type MaintenanceAcker struct {
	Client    kubernetes.Interface
	Namespace string
}

// Acknowledge records that the controller stopped observing under generation.
func (a MaintenanceAcker) Acknowledge(ctx context.Context, generation string) error {
	if generation == "" {
		return fmt.Errorf("maintenance acknowledgement requires a generation")
	}
	configMaps := a.Client.CoreV1().ConfigMaps(a.Namespace)
	want := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: MaintenanceAckConfigMapName, Namespace: a.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}},
		Data:       map[string]string{maintenanceAckGenerationKey: generation},
	}
	prior, err := configMaps.Get(ctx, MaintenanceAckConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, want, metav1.CreateOptions{})
	} else if err == nil {
		want.ResourceVersion = prior.ResourceVersion
		_, err = configMaps.Update(ctx, want, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("acknowledge maintenance generation %q: %w", generation, err)
	}
	return nil
}

// Acknowledged returns the last acknowledged generation, empty when none.
func (a MaintenanceAcker) Acknowledged(ctx context.Context) (string, error) {
	configMap, err := a.Client.CoreV1().ConfigMaps(a.Namespace).Get(ctx, MaintenanceAckConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read maintenance acknowledgement: %w", err)
	}
	return strings.TrimSpace(configMap.Data[maintenanceAckGenerationKey]), nil
}
