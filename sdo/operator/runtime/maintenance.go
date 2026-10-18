package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

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
// acknowledgment) but stops evaluating detectors and stops observing the
// application namespace, so an application that is intentionally absent is
// never treated as an incident. A new generation marks each resume so external
// observers can correlate the first evaluation after it.
type MaintenanceState struct {
	Paused     bool
	Generation string
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
		return MaintenanceState{Paused: true, Generation: generation}, nil
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
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
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
