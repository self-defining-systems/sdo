package rehearsal

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/sdk/generichealth"
)

// ControllerModel stands in for the controller's detector and incident loop
// during a rehearsal, one pass per injected wait. It models what the loop does
// around a maintenance window, not the detectors themselves: while active it
// evaluates two generic detectors (a Service whose selector matches no ready
// pod, a Deployment or StatefulSet below its ready replicas) and records an
// incident for each finding that appears; while paused it acknowledges the
// pause and evaluates nothing.
//
// DrainTicks keeps the race of the real loop: a pass already running when the
// pause was declared still evaluates for that many ticks, and only then is the
// pause acknowledged. A caller that applies before the acknowledgement is
// therefore caught.
type ControllerModel struct {
	Client    kubernetes.Interface
	Namespace string
	Now       func() time.Time
	// Enabled starts the loop (after onboarding, so the first rollout is not
	// an incident).
	Enabled    bool
	DrainTicks int
	// Maintenance reads the declared mode and its generation.
	Maintenance func(ctx context.Context) (paused bool, generation string, err error)
	// Acknowledge records the generation the loop stopped under.
	Acknowledge func(ctx context.Context, generation string) error

	Incidents []ModelIncident
	Errors    []string

	seen     string
	short    map[string]time.Time
	draining int
	acked    string
	active   map[string]bool
}

// ModelIncident is one finding the active loop opened an incident for.
type ModelIncident struct {
	At       time.Time
	Findings []string
}

// Tick is one pass of the loop.
func (c *ControllerModel) Tick(ctx context.Context) {
	if !c.Enabled {
		return
	}
	paused, generation, err := c.Maintenance(ctx)
	if err != nil {
		c.Errors = append(c.Errors, "read maintenance: "+err.Error())
		return
	}
	if !paused {
		c.seen = ""
		c.evaluate(ctx)
		return
	}
	if generation != c.seen {
		c.seen, c.draining = generation, c.DrainTicks
	}
	if c.draining > 0 {
		c.draining--
		c.evaluate(ctx)
		return
	}
	if generation != c.acked {
		if err := c.Acknowledge(ctx, generation); err != nil {
			c.Errors = append(c.Errors, "acknowledge: "+err.Error())
			return
		}
		c.acked = generation
	}
}

// Opened returns the findings of the incidents opened since index from.
func (c *ControllerModel) Opened(from int) []string {
	var out []string
	for _, incident := range c.Incidents[from:] {
		out = append(out, incident.Findings...)
	}
	return out
}

func (c *ControllerModel) evaluate(ctx context.Context) {
	findings, err := c.findings(ctx)
	if err != nil {
		c.Errors = append(c.Errors, "evaluate: "+err.Error())
		return
	}
	next := map[string]bool{}
	var fresh []string
	for _, f := range findings {
		next[f] = true
		if !c.active[f] {
			fresh = append(fresh, f)
		}
	}
	c.active = next
	if len(fresh) > 0 {
		c.Incidents = append(c.Incidents, ModelIncident{At: c.Now(), Findings: fresh})
	}
}

func (c *ControllerModel) findings(ctx context.Context) ([]string, error) {
	var out []string
	services, err := c.Client.CoreV1().Services(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods, err := c.Client.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, service := range services.Items {
		if len(service.Spec.Selector) == 0 {
			continue
		}
		selector := labels.SelectorFromSet(service.Spec.Selector)
		ready := false
		for i := range pods.Items {
			if selector.Matches(labels.Set(pods.Items[i].Labels)) && modelPodReady(&pods.Items[i]) {
				ready = true
			}
		}
		if !ready {
			out = append(out, fmt.Sprintf("service-endpoints Service/%s/%s has no ready endpoints", c.Namespace, service.Name))
		}
	}
	deployments, err := c.Client.AppsV1().Deployments(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, d := range deployments.Items {
		if want := replicasOf(d.Spec.Replicas); d.Status.ReadyReplicas < want {
			out = append(out, fmt.Sprintf("workload-health Deployment/%s/%s %d/%d ready", c.Namespace, d.Name, d.Status.ReadyReplicas, want))
		}
	}
	sets, err := c.Client.AppsV1().StatefulSets(c.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, s := range sets.Items {
		if want := replicasOf(s.Spec.Replicas); s.Status.ReadyReplicas < want {
			out = append(out, fmt.Sprintf("workload-health StatefulSet/%s/%s %d/%d ready", c.Namespace, s.Name, s.Status.ReadyReplicas, want))
		}
	}
	return c.persisted(out), nil
}

// persisted drops a workload-health shortfall that has not lasted the
// detector's progress window: the real detector tolerates a workload that is
// still starting pods (a rollout, an autoscaler scaling up) and fires only
// past generichealth.ProgressWindow.
func (c *ControllerModel) persisted(findings []string) []string {
	now, current := c.Now(), map[string]time.Time{}
	var out []string
	for _, f := range findings {
		if !strings.HasPrefix(f, "workload-health ") {
			out = append(out, f)
			continue
		}
		key := strings.Join(strings.Fields(f)[:2], " ")
		first, ok := c.short[key]
		if !ok {
			first = now
		}
		current[key] = first
		if now.Sub(first) >= generichealth.ProgressWindow {
			out = append(out, f)
		}
	}
	c.short = current
	sort.Strings(out)
	return out
}

func replicasOf(n *int32) int32 {
	if n == nil {
		return 1
	}
	return *n
}

func modelPodReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
