package deploywire

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
)

// RepairMaintenancePrefix marks the maintenance generations a source repair
// wrote, so a pause it left behind (the controller died mid-deploy) is
// recognized, and a pause someone else declared is never lifted by a repair.
const RepairMaintenancePrefix = "source-repair-"

// Maintenance is the lifecycle.MaintenanceWindow of a source repair that
// deploys after its incident closed. The pause stops the controller observing
// the application namespace, so the rollout is never an incident; it is in
// force only once the controller acknowledged it. The controller's own loop
// acknowledges it, and the repair runs on the queued dispatch's goroutine, so
// the wait cannot deadlock.
type Maintenance struct {
	Client    kubernetes.Interface
	Namespace string
	Now       func() time.Time
}

var _ lifecycle.MaintenanceWindow = Maintenance{}

func (m Maintenance) generation(state string) string {
	return fmt.Sprintf("%s%s-%d", RepairMaintenancePrefix, state, m.Now().UnixNano())
}

func (m Maintenance) paused(ctx context.Context) (paused, ours bool, err error) {
	configMap, err := m.Client.CoreV1().ConfigMaps(m.Namespace).Get(ctx, install.MaintenanceConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("read controller maintenance: %w", err)
	}
	generation := strings.TrimSpace(configMap.Data["generation"])
	paused = strings.TrimSpace(configMap.Data["state"]) == "paused"
	return paused, strings.HasPrefix(generation, RepairMaintenancePrefix), nil
}

// Pause refuses while someone else holds the controller in maintenance.
func (m Maintenance) Pause(ctx context.Context, _ string, expiresAt time.Time) error {
	paused, ours, err := m.paused(ctx)
	if err != nil {
		return err
	}
	if paused && !ours {
		return errors.New("controller maintenance is paused by another owner")
	}
	return install.SetMaintenanceWithOptions(ctx, m.Client, m.Namespace, true, m.generation("paused"), install.MaintenanceOptions{ExpiresAt: expiresAt})
}

// Acknowledged reports whether the controller recorded the generation of the
// pause this repair declared.
func (m Maintenance) Acknowledged(ctx context.Context) (bool, error) {
	configMaps := m.Client.CoreV1().ConfigMaps(m.Namespace)
	maintenance, err := configMaps.Get(ctx, install.MaintenanceConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read controller maintenance: %w", err)
	}
	generation := strings.TrimSpace(maintenance.Data["generation"])
	if strings.TrimSpace(maintenance.Data["state"]) != "paused" || !strings.HasPrefix(generation, RepairMaintenancePrefix) {
		return false, nil
	}
	ack, err := configMaps.Get(ctx, install.MaintenanceAckConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read controller maintenance acknowledgement: %w", err)
	}
	return strings.TrimSpace(ack.Data["generation"]) == generation, nil
}

// Resume lifts the pause.
func (m Maintenance) Resume(ctx context.Context) error {
	return install.SetMaintenance(ctx, m.Client, m.Namespace, false, m.generation("resumed"))
}
