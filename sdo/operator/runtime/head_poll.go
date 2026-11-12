package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
)

// DefaultHeadPollInterval is how often the controller looks for a branch head
// SDO did not write.
const DefaultHeadPollInterval = 30 * time.Second

// headHoldConfigMap persists the one refused head, so a broken source is not
// applied again on every poll, and survives a controller restart.
const headHoldConfigMap = "sdo-head-hold"

// HeadPoller is what the controller needs to deploy a branch head: the
// deployer, and the interval between looks.
type HeadPoller struct {
	Deployer lifecycle.HeadDeployer
	// Config reads the install's namespace and current health objective at
	// each look, so an objective the designer changed is the one verified.
	Config   func() (lifecycle.RepairConfig, error)
	Interval time.Duration
}

// headPollState is the controller's bookkeeping of one head deploy at a time.
type headPollState struct {
	poller  *HeadPoller
	running bool
	nextAt  time.Time
}

// SetHeadPoller makes the controller deploy branch heads SDO did not write
// from its reconcile loop. It shares the queue's single-flight slot, so a
// head deploy and a source repair never run together.
func (c *Controller) SetHeadPoller(poller *HeadPoller) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.head = headPollState{poller: poller}
}

// driveHeadPoll starts one head deploy when the controller is idle and the
// interval has passed. It runs from StepEvents only, never while paused.
func (c *Controller) driveHeadPoll(ctx context.Context, now time.Time) {
	c.mu.Lock()
	poller := c.head.poller
	if poller == nil || c.head.running || now.Before(c.head.nextAt) || c.reflections.running != "" || len(c.reflections.queued) > 0 || c.reflections.rollout != "" {
		c.mu.Unlock()
		return
	}
	if c.incidentOpen || c.closureBlocksNextLocked() {
		c.mu.Unlock()
		return
	}
	c.head.running = true
	interval := poller.Interval
	if interval <= 0 {
		interval = DefaultHeadPollInterval
	}
	c.head.nextAt = now.Add(interval)
	c.mu.Unlock()
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		c.mu.Lock()
		c.head.running = false
		c.mu.Unlock()
		return
	}
	c.goBackground(func() {
		defer cancel()
		var result lifecycle.HeadResult
		config, err := poller.Config()
		if err == nil {
			result, err = poller.Deployer.Deploy(actionCtx, config)
		}
		if err == nil && result.Outcome != lifecycle.HeadCurrent {
			slog.Info("head_deployment", "outcome", string(result.Outcome), "commit", result.Commit, "hold", string(result.Hold), "detail", result.Detail)
		}
		c.reflectionResults <- reflectionCompletion{head: true, err: err}
	})
}

// handleHeadCompletion records the end of a head deploy. An error is logged
// and tried again at the next interval: a failed look never blocks the queue.
func (c *Controller) handleHeadCompletion(completion reflectionCompletion) {
	c.mu.Lock()
	c.head.running = false
	c.mu.Unlock()
	if completion.err != nil {
		c.reportBrokerError("deploy branch head", completion.err)
	}
}

// headHoldRecord is the stored hold.
type headHoldRecord struct {
	Commit        string `json:"commit"`
	EffectiveTree string `json:"effective_tree"`
	PatchesDigest string `json:"patches_digest"`
	Reason        string `json:"reason"`
	Detail        string `json:"detail"`
	At            string `json:"at"`
}

// ConfigMapHeadHolds is lifecycle.HeadHolds over one control-namespace ConfigMap.
type ConfigMapHeadHolds struct {
	Client    kubernetes.Interface
	Namespace string
	Now       func() time.Time
}

var _ lifecycle.HeadHolds = ConfigMapHeadHolds{}

func (h ConfigMapHeadHolds) load(ctx context.Context) (*headHoldRecord, error) {
	configMap, err := h.Client.CoreV1().ConfigMaps(h.Namespace).Get(ctx, headHoldConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read head hold: %w", err)
	}
	var record headHoldRecord
	if err := json.Unmarshal([]byte(configMap.Data["hold"]), &record); err != nil {
		return nil, fmt.Errorf("decode head hold: %w", err)
	}
	return &record, nil
}

// Held reports the hold on this effective source.
func (h ConfigMapHeadHolds) Held(ctx context.Context, change broker.HeadChange) (string, bool, error) {
	record, err := h.load(ctx)
	if err != nil || record == nil {
		return "", false, err
	}
	if record.EffectiveTree != change.EffectiveTree || record.PatchesDigest != change.PatchesDigest {
		return "", false, nil
	}
	return record.Reason + ": " + record.Detail, true, nil
}

// Hold records a refused head, replacing any earlier hold.
func (h ConfigMapHeadHolds) Hold(ctx context.Context, change broker.HeadChange, reason lifecycle.HoldReason, detail string) error {
	at := ""
	if h.Now != nil {
		at = h.Now().UTC().Format(time.RFC3339)
	}
	data, err := json.Marshal(headHoldRecord{Commit: change.Commit, EffectiveTree: change.EffectiveTree, PatchesDigest: change.PatchesDigest, Reason: string(reason), Detail: detail, At: at})
	if err != nil {
		return err
	}
	configMaps := h.Client.CoreV1().ConfigMaps(h.Namespace)
	configMap, err := configMaps.Get(ctx, headHoldConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: headHoldConfigMap, Namespace: h.Namespace, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo"}}, Data: map[string]string{"hold": string(data)}}, metav1.CreateOptions{})
		return err
	}
	if err != nil {
		return err
	}
	configMap.Data = map[string]string{"hold": string(data)}
	_, err = configMaps.Update(ctx, configMap, metav1.UpdateOptions{})
	return err
}

// headPoller builds the controller's head deployer from the source repairer's
// deployer, sharing its pause, clock and busy gate. nil when this
// configuration deploys nothing (frozen memory, shell detectors) or has no
// place to keep holds.
func (n *NativeIncidentBroker) headPoller() *HeadPoller {
	repairer, ok := n.Repairs.(DeploymentRepairer)
	if !ok || n.HeadHolds == nil {
		return nil
	}
	holds := n.HeadHolds
	if configMap, ok := holds.(ConfigMapHeadHolds); ok && configMap.Now == nil {
		configMap.Now = n.clock
		holds = configMap
	}
	deployer := repairer.Deployer
	if repairer.Gate != nil {
		deployer.Busy = func(context.Context) (string, error) { return repairer.Gate.Busy(), nil }
	}
	return &HeadPoller{
		Deployer: lifecycle.HeadDeployer{Broker: n.Repository, Deployer: deployer, Holds: holds},
		Config: func() (lifecycle.RepairConfig, error) {
			objective, err := memory.AcceptedHealthObjective(repairer.AppRoot)
			return lifecycle.RepairConfig{Namespace: repairer.Namespace, HealthObjective: objective}, err
		},
	}
}
