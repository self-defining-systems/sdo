package deployment

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/deployment/internal/deploymentio"
)

// RolloutTimeoutError reports that a workload did not become ready. Stalled
// is false only on positive evidence that every unready pod is still making
// progress (an init container running, a volume or image being prepared, a
// container inside its start window): only that is an infrastructure outcome
// the deployer must not answer by rewriting the plan. Anything else (pods
// that never become ready, crash loops, unschedulable pods, missing mounts,
// no pods at all) is a plan outcome reported with its concrete evidence.
type RolloutTimeoutError struct {
	Resource string
	Limit    time.Duration
	// Waited is how long the rollout was observed before the error; it is
	// shorter than Limit when the rollout was clearly stuck.
	Waited time.Duration
	// Detail is the bounded pod, condition and event evidence.
	Detail  string
	Advice  string
	Stalled bool
}

func (e *RolloutTimeoutError) Error() string {
	detail := e.Detail
	if detail == "" {
		detail = "no pod status could be read"
	}
	if e.Stalled {
		waited := e.Waited
		if waited <= 0 {
			waited = e.Limit
		}
		message := fmt.Sprintf("workload %s did not become ready after %s (limit %s) and its pods are not progressing: %s. This is a plan problem, not a time limit", e.Resource, waited.Round(time.Second), e.Limit, detail)
		if e.Advice != "" {
			message += ": " + e.Advice
		}
		return message
	}
	return fmt.Sprintf("workload %s is still rolling out after %s (its pods are starting, none failing): %s. THE PLAN WAS FINE: this is a time limit, not a plan error. "+
		"Resubmit exactly the same plan and leave the manifests, images and init containers unchanged; the operator waits for the rollout again. Rewriting a plan after this error makes it worse", e.Resource, e.Limit, detail)
}

// InfrastructureOutcome marks a still-progressing rollout for the coordinator's
// bounded retry of the trusted step. A rollout that is not progressing is a plan outcome.
func (e *RolloutTimeoutError) InfrastructureOutcome() bool { return !e.Stalled }

const (
	maxRolloutPods   = 6
	maxRolloutDetail = 1800
	maxRolloutEvents = 5
	// A rollout is only checked for a clear stall after rolloutCheckAfter and
	// then every rolloutCheckEvery; the thresholds below are pod ages.
	rolloutCheckAfter = 30 * time.Second
	rolloutCheckEvery = 15 * time.Second
	// startWindow is how long a started, restart-free container may stay
	// unready before that is no longer evidence of progress.
	startWindow     = 90 * time.Second
	scheduleGrace   = 60 * time.Second
	mountGrace      = 90 * time.Second
	imagePullGrace  = 2 * time.Minute
	crashLoopRepeat = 3
)

func (b CommandBackend) now() time.Time {
	if b.Now != nil {
		return b.Now()
	}
	return deploymentio.Now()
}

// readRollout reads the workload's pods and their warning events once with a
// fresh short context.
func (b CommandBackend) readRollout(resourceID string, workload []byte) (pods, events []byte, ok bool) {
	parts := strings.Split(resourceID, "/")
	var object struct {
		Spec struct {
			Selector struct {
				MatchLabels map[string]string `json:"matchLabels"`
			} `json:"selector"`
		} `json:"spec"`
	}
	if json.Unmarshal(workload, &object) != nil || len(object.Spec.Selector.MatchLabels) == 0 {
		return nil, nil, false
	}
	pairs := make([]string, 0, len(object.Spec.Selector.MatchLabels))
	for key, value := range object.Spec.Selector.MatchLabels {
		pairs = append(pairs, key+"="+value)
	}
	sort.Strings(pairs)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pods, err := b.run(ctx, b.kubectl(), b.kubectlArgs("get", "pods", "--namespace", parts[1], "--selector", strings.Join(pairs, ","), "--output=json"), nil)
	if err != nil {
		return nil, nil, false
	}
	// Events are best effort: pod conditions alone still classify the rollout.
	events, _ = b.run(ctx, b.kubectl(), b.kubectlArgs("get", "events", "--namespace", parts[1], "--field-selector", "type=Warning", "--output=json"), nil)
	return pods, events, true
}

// rolloutTimeout builds the typed error at the deadline from the last
// observed workload object.
func (b CommandBackend) rolloutTimeout(resourceID string, limit time.Duration, workload []byte) error {
	failure := &RolloutTimeoutError{Resource: resourceID, Limit: limit, Waited: limit}
	pods, events, ok := b.readRollout(resourceID, workload)
	if !ok {
		return failure
	}
	assessment := assessRollout(resourceID, pods, events, b.now())
	failure.Detail, failure.Stalled, failure.Advice = assessment.Detail, !assessment.Progressing, assessment.Advice
	return failure
}

// stuckRollout reports a rollout whose pods are clearly not going to become
// ready, so the observer stops waiting for the full limit.
func (b CommandBackend) stuckRollout(resourceID string, limit, waited time.Duration, workload []byte) error {
	pods, events, ok := b.readRollout(resourceID, workload)
	if !ok {
		return nil
	}
	assessment := assessRollout(resourceID, pods, events, b.now())
	if !assessment.Stuck {
		return nil
	}
	return &RolloutTimeoutError{Resource: resourceID, Limit: limit, Waited: waited, Detail: assessment.Detail, Advice: assessment.Advice, Stalled: true}
}

type containerStatus struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int    `json:"restartCount"`
	State        struct {
		Waiting *struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"waiting"`
		Running *struct {
			StartedAt time.Time `json:"startedAt"`
		} `json:"running"`
		Terminated *terminatedState `json:"terminated"`
	} `json:"state"`
	LastState struct {
		Terminated *terminatedState `json:"terminated"`
	} `json:"lastState"`
}

type terminatedState struct {
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason"`
}

type podCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type podObject struct {
	Metadata struct {
		Name              string    `json:"name"`
		CreationTimestamp time.Time `json:"creationTimestamp"`
	} `json:"metadata"`
	Status struct {
		Phase                 string            `json:"phase"`
		Conditions            []podCondition    `json:"conditions"`
		InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
		ContainerStatuses     []containerStatus `json:"containerStatuses"`
	} `json:"status"`
}

type rolloutEvent struct {
	Reason         string `json:"reason"`
	Message        string `json:"message"`
	Count          int    `json:"count"`
	InvolvedObject struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"involvedObject"`
}

// rolloutAssessment is the classification of one observation of a workload's
// pods. Progressing requires positive evidence for every unready pod. Stuck
// is the stricter "no point waiting any longer" verdict.
type rolloutAssessment struct {
	Detail      string
	Advice      string
	Progressing bool
	Stuck       bool
}

// podVerdict is what one unready pod's evidence says.
type podVerdict struct {
	progress string // positive evidence of progress, empty if none
	stuck    bool
	advice   []string
	notes    []string
}

func podReady(pod podObject) bool {
	if pod.Status.Phase != "Running" || len(pod.Status.ContainerStatuses) == 0 {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func hasEvent(events []rolloutEvent, reasons ...string) bool {
	for _, event := range events {
		for _, reason := range reasons {
			if event.Reason == reason {
				return true
			}
		}
	}
	return false
}

const (
	adviceSchedule = "the pod cannot be scheduled: fix its resource requests, nodeSelector/affinity/tolerations or the PersistentVolumeClaim and storageClass it needs"
	adviceMount    = "a volume cannot be mounted: every ConfigMap, Secret and PVC the pod mounts must be defined in the plan with that exact name and keys"
	advicePull     = "the image cannot be pulled: use the image reference the build produced, with a valid name and tag"
	adviceConfig   = "the container cannot be created: fix its env valueFrom/ConfigMap/Secret references, command or securityContext"
	adviceCrash    = "the container keeps crashing: read its exit code and fix the command, environment, config or the dependency host/port it connects to"
	adviceProbe    = "the container runs but never reports ready: fix the readinessProbe port, path and timing, or the configuration the app needs to start serving"
	adviceNoPods   = "the controller cannot create pods: fix the ServiceAccount, namespace, quota or pod security settings named in the events"
)

func assessPod(pod podObject, events []rolloutEvent, now time.Time) podVerdict {
	var v podVerdict
	age := now.Sub(pod.Metadata.CreationTimestamp)
	unscheduled, scheduleMessage := false, ""
	for _, c := range pod.Status.Conditions {
		if c.Type == "PodScheduled" && c.Status == "False" {
			unscheduled, scheduleMessage = true, c.Reason+": "+c.Message
		}
	}
	mountFailing := hasEvent(events, "FailedMount", "FailedAttachVolume", "FailedCreatePodSandBox")
	if unscheduled {
		v.notes = append(v.notes, "not scheduled ("+clip(scheduleMessage, 200)+")")
		v.advice = append(v.advice, adviceSchedule)
		v.stuck = age >= scheduleGrace
		return v
	}
	if pod.Status.Phase == "Pending" && len(pod.Status.ContainerStatuses) == 0 && len(pod.Status.InitContainerStatuses) == 0 {
		v.notes = append(v.notes, "Pending with no container statuses")
		if hasEvent(events, "FailedScheduling") {
			v.advice = append(v.advice, adviceSchedule)
			v.stuck = age >= scheduleGrace
		} else if mountFailing {
			v.advice = append(v.advice, adviceMount)
			v.stuck = age >= mountGrace
		}
		return v
	}
	for _, s := range pod.Status.InitContainerStatuses {
		switch {
		case s.State.Running != nil && s.RestartCount == 0:
			v.progress = "init " + s.Name + " running"
		case s.State.Terminated != nil && s.State.Terminated.ExitCode != 0:
			v.stuck = true
			v.advice = append(v.advice, "init container "+s.Name+" failed: fix its command or the dependency it waits for")
		case s.RestartCount >= crashLoopRepeat:
			v.stuck = true
			v.advice = append(v.advice, "init container "+s.Name+" keeps failing: fix its command or the dependency it waits for")
		}
	}
	for _, s := range pod.Status.ContainerStatuses {
		if s.Ready {
			continue
		}
		switch {
		case s.State.Waiting != nil:
			reason := s.State.Waiting.Reason
			switch reason {
			case "PodInitializing", "ContainerCreating", "":
				if mountFailing {
					v.advice = append(v.advice, adviceMount)
					v.stuck = v.stuck || age >= mountGrace
				} else if v.progress == "" {
					v.progress = s.Name + " " + strings.ToLower(reason) + " (no failure events)"
				}
			case "ErrImagePull", "ImagePullBackOff":
				v.advice = append(v.advice, advicePull)
				v.stuck = v.stuck || age >= imagePullGrace
			case "InvalidImageName":
				v.advice = append(v.advice, advicePull)
				v.stuck = true
			case "CreateContainerConfigError", "CreateContainerError", "RunContainerError":
				v.advice = append(v.advice, adviceConfig)
				v.stuck = v.stuck || age >= 30*time.Second
			case "CrashLoopBackOff":
				v.advice = append(v.advice, adviceCrash)
				v.stuck = true
			default:
				v.advice = append(v.advice, adviceCrash)
			}
		case s.State.Terminated != nil:
			if s.State.Terminated.ExitCode != 0 {
				v.advice = append(v.advice, adviceCrash)
				v.stuck = true
			}
		case s.State.Running != nil:
			if s.RestartCount > 0 && s.LastState.Terminated != nil {
				v.advice = append(v.advice, adviceCrash)
				v.stuck = v.stuck || s.RestartCount >= crashLoopRepeat
			} else if s.RestartCount == 0 && now.Sub(s.State.Running.StartedAt) < startWindow {
				if v.progress == "" {
					v.progress = s.Name + " started " + now.Sub(s.State.Running.StartedAt).Round(time.Second).String() + " ago, still within its start window"
				}
			} else {
				v.advice = append(v.advice, adviceProbe)
			}
		}
	}
	if len(v.advice) > 0 {
		// Any failure evidence outweighs another container's progress.
		v.progress = ""
	}
	return v
}

func describeStatus(prefix string, c containerStatus) string {
	var b strings.Builder
	b.WriteString(prefix + c.Name)
	switch {
	case c.State.Waiting != nil:
		reason := c.State.Waiting.Reason
		if reason == "" {
			reason = "waiting"
		}
		fmt.Fprintf(&b, " waiting (%s)", reason)
		if c.State.Waiting.Message != "" {
			fmt.Fprintf(&b, " %s", clip(c.State.Waiting.Message, 120))
		}
	case c.State.Terminated != nil:
		fmt.Fprintf(&b, " terminated (exit %d %s)", c.State.Terminated.ExitCode, c.State.Terminated.Reason)
	case c.State.Running != nil:
		fmt.Fprintf(&b, " running (ready=%t)", c.Ready)
	default:
		b.WriteString(" not started")
	}
	if c.RestartCount > 0 {
		fmt.Fprintf(&b, " restarts=%d", c.RestartCount)
		if t := c.LastState.Terminated; t != nil {
			fmt.Fprintf(&b, " last exit %d %s", t.ExitCode, t.Reason)
		}
	}
	return b.String()
}

func assessRollout(resourceID string, podsJSON, eventsJSON []byte, now time.Time) rolloutAssessment {
	var list struct {
		Items []podObject `json:"items"`
	}
	if json.Unmarshal(podsJSON, &list) != nil {
		return rolloutAssessment{}
	}
	var eventList struct {
		Items []rolloutEvent `json:"items"`
	}
	_ = json.Unmarshal(eventsJSON, &eventList)
	workload := resourceID[strings.LastIndex(resourceID, "/")+1:]
	podNames := map[string]bool{}
	for _, pod := range list.Items {
		podNames[pod.Metadata.Name] = true
	}
	podEvents := map[string][]rolloutEvent{}
	var controllerEvents []rolloutEvent
	for _, event := range eventList.Items {
		switch {
		case event.InvolvedObject.Kind == "Pod" && podNames[event.InvolvedObject.Name]:
			podEvents[event.InvolvedObject.Name] = append(podEvents[event.InvolvedObject.Name], event)
		case event.InvolvedObject.Kind != "Pod" && strings.HasPrefix(event.InvolvedObject.Name, workload):
			controllerEvents = append(controllerEvents, event)
		}
	}
	if len(list.Items) == 0 {
		a := rolloutAssessment{Detail: "no pods exist for the workload"}
		if len(controllerEvents) > 0 {
			a.Detail += "; " + eventLines(controllerEvents)
		}
		a.Advice = adviceNoPods
		a.Stuck = hasEvent(controllerEvents, "FailedCreate")
		return a
	}
	result := rolloutAssessment{Progressing: true}
	advice := map[string]bool{}
	lines := []string{}
	var shownEvents []rolloutEvent
	for i, pod := range list.Items {
		if i == maxRolloutPods {
			lines = append(lines, fmt.Sprintf("... %d more pods", len(list.Items)-i))
			break
		}
		parts := []string{}
		for _, c := range pod.Status.Conditions {
			if c.Status == "False" && (c.Type == "PodScheduled" || c.Type == "Initialized" || c.Type == "ContainersReady") && c.Reason != "" {
				parts = append(parts, fmt.Sprintf("condition %s=False (%s)", c.Type, c.Reason))
			}
		}
		for _, c := range pod.Status.InitContainerStatuses {
			parts = append(parts, describeStatus("init ", c))
		}
		for _, c := range pod.Status.ContainerStatuses {
			parts = append(parts, describeStatus("", c))
		}
		verdict := assessPod(pod, podEvents[pod.Metadata.Name], now)
		if podReady(pod) {
			parts = append(parts, "ready")
		} else {
			parts = append(parts, verdict.notes...)
			if verdict.progress == "" {
				result.Progressing = false
			}
			if verdict.stuck {
				result.Stuck = true
			}
			if len(verdict.advice) == 0 && verdict.progress == "" {
				// No failure evidence and no progress either: an unready pod with nothing to wait for.
				verdict.advice = []string{"the pod shows no sign of starting: check its events and conditions"}
			}
			for _, a := range verdict.advice {
				if !advice[a] {
					advice[a] = true
					if result.Advice != "" {
						result.Advice += "; "
					}
					result.Advice += a
				}
			}
		}
		shownEvents = append(shownEvents, podEvents[pod.Metadata.Name]...)
		lines = append(lines, fmt.Sprintf("pod %s phase=%s %s", pod.Metadata.Name, pod.Status.Phase, strings.Join(parts, ", ")))
	}
	detail := strings.Join(lines, "; ")
	if text := eventLines(shownEvents); text != "" {
		detail += "; events: " + text
	}
	if runes := []rune(detail); len(runes) > maxRolloutDetail {
		detail = string(runes[:maxRolloutDetail]) + "..."
	}
	result.Detail = detail
	return result
}

// eventLines renders the last few distinct warning events.
func eventLines(events []rolloutEvent) string {
	seen := map[string]bool{}
	lines := []string{}
	for i := len(events) - 1; i >= 0 && len(lines) < maxRolloutEvents; i-- {
		line := events[i].Reason + ": " + clip(strings.TrimSpace(events[i].Message), 160)
		if !seen[line] {
			seen[line] = true
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, " | ")
}
