package deployment

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// RolloutTimeoutError reports that a workload did not finish rolling out
// within the observe limit. Stalled distinguishes pods that are failing
// (image pull errors, crash loops) from pods that are merely still starting
// (a long init container, a slow image pull): only the second is an
// infrastructure outcome the deployer must not answer by rewriting the plan.
type RolloutTimeoutError struct {
	Resource string
	Limit    time.Duration
	// Detail is the bounded pod phase and init-container status at the deadline.
	Detail  string
	Stalled bool
}

func (e *RolloutTimeoutError) Error() string {
	detail := e.Detail
	if detail == "" {
		detail = "no pod status could be read"
	}
	if e.Stalled {
		return fmt.Sprintf("workload %s did not become ready within %s and its pods are failing, not merely starting: %s", e.Resource, e.Limit, detail)
	}
	return fmt.Sprintf("workload %s is still rolling out after %s (its pods are starting, none failing): %s. THE PLAN WAS FINE: this is a time limit, not a plan error. "+
		"Resubmit exactly the same plan and leave the manifests, images and init containers unchanged; the operator waits for the rollout again. Rewriting a plan after this error makes it worse", e.Resource, e.Limit, detail)
}

// InfrastructureTimeout marks a still-starting rollout for the coordinator's
// "not a plan rejection" feedback. A rollout whose pods fail is a plan outcome.
func (e *RolloutTimeoutError) InfrastructureTimeout() bool { return !e.Stalled }

const (
	maxRolloutPods   = 6
	maxRolloutDetail = 1200
)

var stalledWaitingReasons = map[string]bool{
	"ErrImagePull": true, "ImagePullBackOff": true, "InvalidImageName": true, "CrashLoopBackOff": true,
	"CreateContainerConfigError": true, "CreateContainerError": true, "RunContainerError": true,
}

// rolloutTimeout builds the typed error from the last observed workload
// object, reading its pods once with a fresh short context.
func (b CommandBackend) rolloutTimeout(resourceID string, limit time.Duration, workload []byte) error {
	parts := strings.Split(resourceID, "/")
	failure := &RolloutTimeoutError{Resource: resourceID, Limit: limit}
	var object struct {
		Spec struct {
			Selector struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"selector"`
		} `json:"spec"`
	}
	if json.Unmarshal(workload, &object) != nil || len(object.Spec.Selector.MatchLabels) == 0 {
		return failure
	}
	pairs := make([]string, 0, len(object.Spec.Selector.MatchLabels))
	for key, value := range object.Spec.Selector.MatchLabels {
		pairs = append(pairs, key+"="+value)
	}
	sort.Strings(pairs)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	output, err := b.run(ctx, b.kubectl(), b.kubectlArgs("get", "pods", "--namespace", parts[1], "--selector", strings.Join(pairs, ","), "--output=json"), nil)
	if err != nil {
		return failure
	}
	failure.Detail, failure.Stalled = summarizePods(output)
	return failure
}

type containerStatus struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
	State struct {
		Waiting    *struct{ Reason, Message string } `json:"waiting"`
		Running    *struct{}                         `json:"running"`
		Terminated *struct {
			ExitCode int
			Reason   string
		} `json:"terminated"`
	} `json:"state"`
}

func (c containerStatus) describe(stalled *bool) string {
	switch {
	case c.State.Waiting != nil:
		if stalledWaitingReasons[c.State.Waiting.Reason] {
			*stalled = true
		}
		reason := c.State.Waiting.Reason
		if reason == "" {
			reason = "waiting"
		}
		return fmt.Sprintf("%s waiting (%s)", c.Name, reason)
	case c.State.Terminated != nil:
		if c.State.Terminated.ExitCode != 0 {
			*stalled = true
		}
		return fmt.Sprintf("%s terminated (exit %d %s)", c.Name, c.State.Terminated.ExitCode, c.State.Terminated.Reason)
	case c.State.Running != nil:
		return fmt.Sprintf("%s running (ready=%t)", c.Name, c.Ready)
	}
	return c.Name + " not started"
}

func summarizePods(data []byte) (string, bool) {
	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Phase                 string            `json:"phase"`
				InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
				ContainerStatuses     []containerStatus `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if json.Unmarshal(data, &list) != nil {
		return "", false
	}
	if len(list.Items) == 0 {
		return "no pods exist for the workload", false
	}
	stalled := false
	lines := []string{}
	for i, pod := range list.Items {
		if i == maxRolloutPods {
			lines = append(lines, fmt.Sprintf("... %d more pods", len(list.Items)-i))
			break
		}
		parts := []string{}
		for _, status := range pod.Status.InitContainerStatuses {
			parts = append(parts, "init "+status.describe(&stalled))
		}
		for _, status := range pod.Status.ContainerStatuses {
			parts = append(parts, status.describe(&stalled))
		}
		lines = append(lines, fmt.Sprintf("pod %s phase=%s %s", pod.Metadata.Name, pod.Status.Phase, strings.Join(parts, ", ")))
	}
	detail := strings.Join(lines, "; ")
	if runes := []rune(detail); len(runes) > maxRolloutDetail {
		detail = string(runes[:maxRolloutDetail]) + "..."
	}
	return detail, stalled
}
