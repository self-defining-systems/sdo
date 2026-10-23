// Package workloadhealth holds an app-agnostic health check over Kubernetes
// workload state: failed, stuck, crashing, or unready pods and Deployments
// that do not reach their desired availability. It reports workloads, not
// Services; servicehealth covers Services without ready endpoints.
//
// Every age is measured against sdk.ObservationTime, never the wall clock, so
// one snapshot always yields the same findings.
package workloadhealth

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/sdk"
)

// Rule IDs are stable. A finding is emitted once per rule and primary
// resource: the owning Deployment when its ReplicaSet chain resolves in the
// snapshot, otherwise the Pod itself.
const (
	// RulePodFailed: a pod reached phase Failed.
	RulePodFailed = "workload-pod-failed"
	// RulePodUnknown: the kubelet stopped reporting a pod (phase Unknown).
	RulePodUnknown = "workload-pod-unknown"
	// RulePodPending: a scheduled pod stayed Pending past PendingGrace.
	RulePodPending = "workload-pod-pending"
	// RulePodUnschedulable: a pod stayed Pending past PendingGrace because
	// the scheduler found no node (PodScheduled=False, Unschedulable).
	RulePodUnschedulable = "workload-pod-unschedulable"
	// RuleCrashLooping: a container waits in CrashLoopBackOff.
	RuleCrashLooping = "workload-container-crash-looping"
	// RuleImagePull: a container waits in ImagePullBackOff or ErrImagePull.
	RuleImagePull = "workload-container-image-pull-failing"
	// RuleConfigError: a container cannot be created from its spec
	// (CreateContainerConfigError, CreateContainerError, InvalidImageName).
	RuleConfigError = "workload-container-config-error"
	// RuleWaiting: a container waits for any other non-startup reason.
	RuleWaiting = "workload-container-waiting"
	// RuleOOMKilled: a container is, or within RestartWindow was, OOMKilled.
	RuleOOMKilled = "workload-container-oom-killed"
	// RuleTerminatedError: a container's current state is terminated with a
	// reason other than Completed.
	RuleTerminatedError = "workload-container-terminated-error"
	// RuleNotReady: a running container of a Running pod is unready past
	// NotReadyGrace after it started.
	RuleNotReady = "workload-container-not-ready"
	// RuleInitStuck: an init container has not completed within
	// InitContainerGrace.
	RuleInitStuck = "workload-init-container-stuck"
	// RuleRestartChurn: a container restarted at least RestartThreshold times,
	// last terminated within RestartWindow, and has not since run ready for
	// NotReadyGrace.
	RuleRestartChurn = "workload-container-restart-churn"
	// RuleDeploymentUnavailable: a Deployment has fewer available replicas
	// than desired past DeploymentGrace since its last progress, and no pod
	// finding explains it.
	RuleDeploymentUnavailable = "workload-deployment-unavailable"
	// RuleProgressDeadlineExceeded: a Deployment reports Progressing=False
	// with reason ProgressDeadlineExceeded.
	RuleProgressDeadlineExceeded = "workload-deployment-progress-deadline-exceeded"
)

const maxEvidenceItems = 5

// Watches are the resources whose changes can change the verdict.
func Watches() []sdk.WatchKind {
	return []sdk.WatchKind{
		{APIVersion: "v1", Kind: "Pod"},
		{APIVersion: "apps/v1", Kind: "Deployment"},
		{APIVersion: "apps/v1", Kind: "ReplicaSet"},
		{APIVersion: "v1", Kind: "Event"},
	}
}

// Policy holds the detector's thresholds. All durations are measured against
// the snapshot's observation time.
type Policy struct {
	// PendingGrace is how long a pod may stay Pending after creation.
	PendingGrace time.Duration
	// NotReadyGrace is how long a started container may stay unready, and
	// how long a restarted container must run ready to count as recovered.
	NotReadyGrace time.Duration
	// InitContainerGrace is how long an init container may take.
	InitContainerGrace time.Duration
	// DeploymentGrace is how long a Deployment may stay below its desired
	// availability after its last recorded progress.
	DeploymentGrace time.Duration
	// RestartWindow bounds how recent a last termination must be to count
	// for restart churn and for a past OOM kill.
	RestartWindow time.Duration
	// RestartThreshold is the restart count at which recent restarts churn.
	RestartThreshold int32
}

// DefaultPolicy returns the thresholds the generic health detector uses.
func DefaultPolicy() Policy {
	return Policy{
		PendingGrace: time.Minute, NotReadyGrace: time.Minute, InitContainerGrace: 2 * time.Minute,
		DeploymentGrace: 2 * time.Minute, RestartWindow: 5 * time.Minute, RestartThreshold: 3,
	}
}

// Validate rejects non-positive thresholds.
func (p Policy) Validate() error {
	var problems []error
	for name, value := range map[string]time.Duration{
		"PendingGrace": p.PendingGrace, "NotReadyGrace": p.NotReadyGrace, "InitContainerGrace": p.InitContainerGrace,
		"DeploymentGrace": p.DeploymentGrace, "RestartWindow": p.RestartWindow,
	} {
		if value <= 0 {
			problems = append(problems, fmt.Errorf("workload health policy %s must be positive, got %s", name, value))
		}
	}
	if p.RestartThreshold <= 0 {
		problems = append(problems, fmt.Errorf("workload health policy RestartThreshold must be positive, got %d", p.RestartThreshold))
	}
	sort.Slice(problems, func(left, right int) bool { return problems[left].Error() < problems[right].Error() })
	return errors.Join(problems...)
}

type detector struct {
	spec   sdk.DetectorSpec
	policy Policy
}

// NewDetector returns the workload health detector with DefaultPolicy.
func NewDetector(spec sdk.DetectorSpec) sdk.Detector {
	return detector{spec: spec, policy: DefaultPolicy()}
}

// NewDetectorWithPolicy returns the workload health detector with policy.
func NewDetectorWithPolicy(spec sdk.DetectorSpec, policy Policy) (sdk.Detector, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return detector{spec: spec, policy: policy}, nil
}

func (d detector) Spec() sdk.DetectorSpec { return d.spec }

// group accumulates the offending pods and evidence of one finding.
type group struct {
	rule    string
	primary sdk.ObjectRef
	pods    map[string]struct{}
	items   []string
	reasons map[string]struct{}
}

type evaluation struct {
	d           detector
	ctx         sdk.DetectionContext
	now         time.Time
	namespace   string
	deployments map[string]appsv1.Deployment
	replicaSets map[string]appsv1.ReplicaSet
	groups      map[string]*group
}

func (d detector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	e := evaluation{
		d: d, ctx: ctx, now: sdk.ObservationTime(ctx), namespace: ctx.Namespace(),
		deployments: map[string]appsv1.Deployment{}, replicaSets: map[string]appsv1.ReplicaSet{}, groups: map[string]*group{},
	}
	for _, deployment := range ctx.Deployments() {
		if e.inNamespace(deployment.Namespace) {
			e.deployments[deployment.Name] = deployment
		}
	}
	for _, replicaSet := range ctx.ReplicaSets() {
		if e.inNamespace(replicaSet.Namespace) {
			e.replicaSets[replicaSet.Name] = replicaSet
		}
	}
	pods := ctx.Pods()
	succeededJobs := map[string]bool{}
	for _, pod := range pods {
		if owner := controllerOf(pod.ObjectMeta); owner != nil && owner.Kind == "Job" && pod.Status.Phase == corev1.PodSucceeded {
			succeededJobs[owner.Name] = true
		}
	}
	for _, pod := range pods {
		e.pod(pod, succeededJobs)
	}
	for _, name := range sortedKeys(e.deployments) {
		e.deployment(e.deployments[name], pods)
	}
	return e.findings(), nil
}

func (e *evaluation) inNamespace(namespace string) bool {
	return e.namespace == "" || namespace == e.namespace
}

// elapsed reports whether at least grace has passed since since. An unknown
// start or observation time never proves a grace period has elapsed.
func (e *evaluation) elapsed(since time.Time, grace time.Duration) bool {
	return !since.IsZero() && !e.now.IsZero() && e.now.Sub(since) >= grace
}

// owner resolves a pod's primary resource. ok is false when the pod belongs
// to a Deployment that is intentionally scaled to zero.
func (e *evaluation) owner(pod corev1.Pod) (ref sdk.ObjectRef, deployment *appsv1.Deployment, ok bool) {
	ref = sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}
	podOwner := controllerOf(pod.ObjectMeta)
	if podOwner == nil || podOwner.Kind != "ReplicaSet" {
		return ref, nil, true
	}
	replicaSet, found := e.replicaSets[podOwner.Name]
	if !found {
		return ref, nil, true
	}
	setOwner := controllerOf(replicaSet.ObjectMeta)
	if setOwner == nil || setOwner.Kind != "Deployment" {
		return ref, nil, true
	}
	owning, found := e.deployments[setOwner.Name]
	if !found {
		return ref, nil, true
	}
	if desiredReplicas(owning) == 0 {
		return ref, nil, false
	}
	return sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: owning.Namespace, Name: owning.Name}, &owning, true
}

func (e *evaluation) pod(pod corev1.Pod, succeededJobs map[string]bool) {
	if !e.inNamespace(pod.Namespace) || pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded {
		return
	}
	primary, deployment, ok := e.owner(pod)
	if !ok {
		return
	}
	add := func(rule, item, reason string) {
		e.add(rule, primary, pod.Name, fmt.Sprintf("Pod %s %s.", pod.Name, item), reason)
	}
	switch pod.Status.Phase {
	case corev1.PodFailed:
		if owner := controllerOf(pod.ObjectMeta); owner != nil && owner.Kind == "Job" && succeededJobs[owner.Name] {
			return
		}
		// A Failed pod of a fully available Deployment (an old eviction) has
		// already been replaced; it is history, not current health.
		if deployment != nil && deployment.Status.AvailableReplicas >= desiredReplicas(*deployment) {
			return
		}
		add(RulePodFailed, "is in phase Failed"+podReason(pod)+terminations(pod)+e.events("Pod", pod.Name), pod.Status.Reason)
		return
	case corev1.PodUnknown:
		add(RulePodUnknown, "is in phase Unknown"+podReason(pod)+e.events("Pod", pod.Name), pod.Status.Reason)
		return
	}
	found := e.containers(pod, add)
	if found || pod.Status.Phase != corev1.PodPending || !e.elapsed(pod.CreationTimestamp.Time, e.d.policy.PendingGrace) {
		return
	}
	created := pod.CreationTimestamp.UTC().Format(time.RFC3339)
	if condition := podCondition(pod, corev1.PodScheduled); condition != nil &&
		condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable {
		add(RulePodUnschedulable, fmt.Sprintf("is Pending since %s and Unschedulable: %s%s",
			created, condition.Message, e.events("Pod", pod.Name)), corev1.PodReasonUnschedulable)
		return
	}
	add(RulePodPending, fmt.Sprintf("is Pending since %s%s%s", created, waitingSummary(pod), e.events("Pod", pod.Name)), "Pending")
}

// containers adds container findings for pod and reports whether it found any.
func (e *evaluation) containers(pod corev1.Pod, add func(rule, item, reason string)) bool {
	found := false
	sidecars := map[string]bool{}
	for _, container := range pod.Spec.InitContainers {
		if container.RestartPolicy != nil && *container.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sidecars[container.Name] = true
		}
	}
	events := ""
	eventsLoaded := false
	report := func(rule, label string, status corev1.ContainerStatus, detail, reason string) {
		if !eventsLoaded {
			events, eventsLoaded = e.events("Pod", pod.Name), true
		}
		add(rule, fmt.Sprintf("%s %s %s%s%s", label, status.Name, detail, containerFacts(status), events), reason)
		found = true
	}
	for _, status := range pod.Status.InitContainerStatuses {
		label := "init container"
		if sidecars[status.Name] {
			label = "sidecar container"
		}
		if e.container(pod, status, label, report) || sidecars[status.Name] {
			continue
		}
		if status.State.Running != nil && e.elapsed(status.State.Running.StartedAt.Time, e.d.policy.InitContainerGrace) {
			report(RuleInitStuck, label, status, "has been running since "+
				status.State.Running.StartedAt.UTC().Format(time.RFC3339)+" without completing", "InitContainerRunning")
		} else if status.State.Waiting != nil && e.elapsed(pod.CreationTimestamp.Time, e.d.policy.InitContainerGrace) {
			report(RuleInitStuck, label, status, "is still waiting ("+status.State.Waiting.Reason+")", status.State.Waiting.Reason)
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		e.container(pod, status, "container", report)
	}
	return found
}

// container applies the per-container rules in precedence order and reports
// whether one matched.
func (e *evaluation) container(
	pod corev1.Pod,
	status corev1.ContainerStatus,
	label string,
	report func(rule, label string, status corev1.ContainerStatus, detail, reason string),
) bool {
	policy := e.d.policy
	waiting, terminated := status.State.Waiting, status.State.Terminated
	last := status.LastTerminationState.Terminated
	recentLast := last != nil && e.elapsed(last.FinishedAt.Time, 0) && !e.elapsed(last.FinishedAt.Time, policy.RestartWindow)
	waitingDetail := func() string {
		return "is waiting: " + waiting.Reason + optionalMessage(waiting.Message)
	}
	switch {
	case waiting != nil && isConfigError(waiting.Reason):
		report(RuleConfigError, label, status, waitingDetail(), waiting.Reason)
		return true
	case waiting != nil && (waiting.Reason == "ImagePullBackOff" || waiting.Reason == "ErrImagePull"):
		report(RuleImagePull, label, status, waitingDetail(), waiting.Reason)
		return true
	case terminated != nil && terminated.Reason == "OOMKilled":
		report(RuleOOMKilled, label, status, "is terminated: OOMKilled", "OOMKilled")
		return true
	case recentLast && last.Reason == "OOMKilled":
		detail := "was OOMKilled"
		if waiting != nil {
			detail += "; now waiting: " + waiting.Reason
		}
		report(RuleOOMKilled, label, status, detail, "OOMKilled")
		return true
	}
	matched := false
	stable := status.State.Running != nil && status.Ready && e.elapsed(status.State.Running.StartedAt.Time, policy.NotReadyGrace)
	if recentLast && status.RestartCount >= policy.RestartThreshold && !stable {
		report(RuleRestartChurn, label, status, fmt.Sprintf("restarted %d times, most recently within %s", status.RestartCount, policy.RestartWindow), "RestartChurn")
		matched = true
	}
	switch {
	case waiting != nil && waiting.Reason == "CrashLoopBackOff":
		report(RuleCrashLooping, label, status, waitingDetail(), waiting.Reason)
		return true
	case waiting != nil && waiting.Reason != "" && waiting.Reason != "ContainerCreating" && waiting.Reason != "PodInitializing":
		report(RuleWaiting, label, status, waitingDetail(), waiting.Reason)
		return true
	case terminated != nil && terminated.Reason != "Completed" && !(terminated.Reason == "" && terminated.ExitCode == 0):
		if !matched {
			report(RuleTerminatedError, label, status, "is terminated: "+terminationSummary(*terminated), terminated.Reason)
		}
		return true
	}
	if !matched && label == "container" && pod.Status.Phase == corev1.PodRunning && status.State.Running != nil &&
		!status.Ready && e.elapsed(status.State.Running.StartedAt.Time, policy.NotReadyGrace) {
		report(RuleNotReady, label, status, "is running since "+
			status.State.Running.StartedAt.UTC().Format(time.RFC3339)+" but not ready", "NotReady")
		return true
	}
	return matched
}

func (e *evaluation) deployment(deployment appsv1.Deployment, pods []corev1.Pod) {
	desired := desiredReplicas(deployment)
	if deployment.DeletionTimestamp != nil || desired == 0 {
		return
	}
	ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: deployment.Namespace, Name: deployment.Name}
	status := deployment.Status
	availability := fmt.Sprintf("%d/%d replicas available (%d updated, %d ready)",
		status.AvailableReplicas, desired, status.UpdatedReplicas, status.ReadyReplicas)
	progressing := deploymentCondition(deployment, appsv1.DeploymentProgressing)
	if progressing != nil && progressing.Status == corev1.ConditionFalse && progressing.Reason == "ProgressDeadlineExceeded" {
		e.addDeployment(RuleProgressDeadlineExceeded, ref, deployment, pods, fmt.Sprintf(
			"Deployment %s is not progressing: ProgressDeadlineExceeded: %s; %s.%s%s", deployment.Name, progressing.Message,
			availability, replicaFailure(deployment), e.deploymentEvents(deployment)), "ProgressDeadlineExceeded")
		return
	}
	if status.AvailableReplicas >= desired || e.hasPodFinding(ref) {
		return
	}
	since := deployment.CreationTimestamp.Time
	if progressing != nil && !progressing.LastUpdateTime.IsZero() {
		since = progressing.LastUpdateTime.Time
	}
	if !e.elapsed(since, e.d.policy.DeploymentGrace) {
		return
	}
	e.addDeployment(RuleDeploymentUnavailable, ref, deployment, pods, fmt.Sprintf(
		"Deployment %s has %s; last progress at %s.%s%s", deployment.Name, availability,
		since.UTC().Format(time.RFC3339), replicaFailure(deployment), e.deploymentEvents(deployment)), "Unavailable")
}

func (e *evaluation) addDeployment(rule string, ref sdk.ObjectRef, deployment appsv1.Deployment, pods []corev1.Pod, item, reason string) {
	g := e.group(rule, ref)
	g.items = append(g.items, item)
	g.reasons[reason] = struct{}{}
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil && e.inNamespace(pod.Namespace) {
			if owner, owning, _ := e.owner(pod); owning != nil && owner == ref {
				g.pods[pod.Name] = struct{}{}
			}
		}
	}
}

func (e *evaluation) hasPodFinding(ref sdk.ObjectRef) bool {
	for _, g := range e.groups {
		if g.primary == ref {
			return true
		}
	}
	return false
}

func (e *evaluation) group(rule string, primary sdk.ObjectRef) *group {
	key := rule + "\x00" + primary.Kind + "\x00" + primary.Name
	g, ok := e.groups[key]
	if !ok {
		g = &group{rule: rule, primary: primary, pods: map[string]struct{}{}, reasons: map[string]struct{}{}}
		e.groups[key] = g
	}
	return g
}

func (e *evaluation) add(rule string, primary sdk.ObjectRef, pod, item, reason string) {
	g := e.group(rule, primary)
	g.pods[pod] = struct{}{}
	g.items = append(g.items, item)
	if reason != "" {
		g.reasons[reason] = struct{}{}
	}
}

func (e *evaluation) findings() []sdk.Finding {
	groups := make([]*group, 0, len(e.groups))
	for _, g := range e.groups {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(left, right int) bool {
		a, b := groups[left].primary, groups[right].primary
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return groups[left].rule < groups[right].rule
	})
	findings := make([]sdk.Finding, 0, len(groups))
	for _, g := range groups {
		pods := sortedKeys(g.pods)
		items := append([]string(nil), g.items...)
		sort.Strings(items)
		if len(items) > maxEvidenceItems {
			items = append(items[:maxEvidenceItems], fmt.Sprintf("%d more not shown.", len(items)-maxEvidenceItems))
		}
		var related []sdk.ObjectRef
		if g.primary.Kind != "Pod" {
			for _, pod := range pods {
				related = append(related, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: g.primary.Namespace, Name: pod})
			}
		}
		findings = append(findings, sdk.Finding{
			DetectorID: e.d.spec.ID, RuleID: g.rule, Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
			Summary:  fmt.Sprintf("%s %s: %s", g.primary.Kind, g.primary.Name, summaries[g.rule]),
			Evidence: strings.Join(items, " "), PrimaryResource: g.primary, RelatedResources: related,
			Playbooks: append([]string(nil), e.d.spec.Playbooks...),
			Metadata:  map[string]any{"pods": pods, "reasons": sortedKeys(g.reasons)},
		})
	}
	return findings
}

var summaries = map[string]string{
	RulePodFailed:                "pod failed",
	RulePodUnknown:               "pod state unknown",
	RulePodPending:               "pod stuck Pending",
	RulePodUnschedulable:         "pod cannot be scheduled",
	RuleCrashLooping:             "container in CrashLoopBackOff",
	RuleImagePull:                "container image cannot be pulled",
	RuleConfigError:              "container cannot be created from its configuration",
	RuleWaiting:                  "container stuck waiting",
	RuleOOMKilled:                "container OOMKilled",
	RuleTerminatedError:          "container terminated with an error",
	RuleNotReady:                 "container running but not ready",
	RuleInitStuck:                "init container not completing",
	RuleRestartChurn:             "container restarting repeatedly",
	RuleDeploymentUnavailable:    "fewer available replicas than desired",
	RuleProgressDeadlineExceeded: "rollout exceeded its progress deadline",
}

// events renders the most recent Warning events for an object.
func (e *evaluation) events(kind, name string) string {
	events := make([]corev1.Event, 0)
	for _, event := range e.ctx.RecentEventsFor(e.namespace, kind, name) {
		if event.Type == corev1.EventTypeWarning {
			events = append(events, event)
		}
	}
	sort.SliceStable(events, func(left, right int) bool {
		a, b := eventTime(events[left]), eventTime(events[right])
		if !a.Equal(b) {
			return a.After(b)
		}
		return events[left].Name < events[right].Name
	})
	if len(events) > 3 {
		events = events[:3]
	}
	parts := make([]string, 0, len(events))
	for _, event := range events {
		count := ""
		if event.Count > 1 {
			count = fmt.Sprintf(" x%d", event.Count)
		}
		parts = append(parts, fmt.Sprintf("%s%s: %s", event.Reason, count, strings.TrimSpace(event.Message)))
	}
	if len(parts) == 0 {
		return ""
	}
	return "; recent warning events on " + kind + " " + name + ": " + strings.Join(parts, " | ")
}

func (e *evaluation) deploymentEvents(deployment appsv1.Deployment) string {
	rendered := e.events("Deployment", deployment.Name)
	for _, name := range sortedKeys(e.replicaSets) {
		owner := controllerOf(e.replicaSets[name].ObjectMeta)
		if owner != nil && owner.Kind == "Deployment" && owner.Name == deployment.Name {
			rendered += e.events("ReplicaSet", name)
		}
	}
	return rendered
}

func eventTime(event corev1.Event) time.Time {
	switch {
	case event.Series != nil && !event.Series.LastObservedTime.IsZero():
		return event.Series.LastObservedTime.Time
	case !event.LastTimestamp.IsZero():
		return event.LastTimestamp.Time
	case !event.EventTime.IsZero():
		return event.EventTime.Time
	default:
		return event.FirstTimestamp.Time
	}
}

func containerFacts(status corev1.ContainerStatus) string {
	facts := fmt.Sprintf("; restarts %d", status.RestartCount)
	if terminated := status.State.Terminated; terminated != nil && !terminated.FinishedAt.IsZero() {
		facts += "; terminated at " + terminated.FinishedAt.UTC().Format(time.RFC3339)
	}
	if last := status.LastTerminationState.Terminated; last != nil {
		facts += "; last terminated " + terminationSummary(*last)
	}
	return facts
}

func terminationSummary(terminated corev1.ContainerStateTerminated) string {
	reason := terminated.Reason
	if reason == "" {
		reason = "without a reason"
	}
	summary := fmt.Sprintf("%s, exit code %d", reason, terminated.ExitCode)
	if !terminated.FinishedAt.IsZero() {
		summary += " at " + terminated.FinishedAt.UTC().Format(time.RFC3339)
	}
	return summary + optionalMessage(terminated.Message)
}

func terminations(pod corev1.Pod) string {
	var parts []string
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.State.Terminated != nil {
				parts = append(parts, "container "+status.Name+" "+terminationSummary(*status.State.Terminated))
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "; " + strings.Join(parts, "; ")
}

func waitingSummary(pod corev1.Pod) string {
	var parts []string
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, status := range statuses {
			if status.State.Waiting != nil && status.State.Waiting.Reason != "" {
				parts = append(parts, "container "+status.Name+" waiting: "+status.State.Waiting.Reason+optionalMessage(status.State.Waiting.Message))
			}
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "; " + strings.Join(parts, "; ")
}

func podReason(pod corev1.Pod) string {
	reason := ""
	if pod.Status.Reason != "" {
		reason = " (" + pod.Status.Reason + ")"
	}
	return reason + optionalMessage(pod.Status.Message)
}

func replicaFailure(deployment appsv1.Deployment) string {
	condition := deploymentCondition(deployment, appsv1.DeploymentReplicaFailure)
	if condition == nil || condition.Status != corev1.ConditionTrue {
		return ""
	}
	return fmt.Sprintf(" ReplicaFailure %s: %s.", condition.Reason, condition.Message)
}

func optionalMessage(message string) string {
	message = strings.TrimSpace(message)
	if message == "" {
		return ""
	}
	return ": " + message
}

func isConfigError(reason string) bool {
	return reason == "CreateContainerConfigError" || reason == "CreateContainerError" || reason == "InvalidImageName"
}

func desiredReplicas(deployment appsv1.Deployment) int32 {
	if deployment.Spec.Replicas == nil {
		return 1
	}
	return *deployment.Spec.Replicas
}

func controllerOf(meta metav1.ObjectMeta) *metav1.OwnerReference {
	for index := range meta.OwnerReferences {
		owner := meta.OwnerReferences[index]
		if owner.Controller != nil && *owner.Controller {
			return &owner
		}
	}
	return nil
}

func podCondition(pod corev1.Pod, kind corev1.PodConditionType) *corev1.PodCondition {
	for index := range pod.Status.Conditions {
		if pod.Status.Conditions[index].Type == kind {
			return &pod.Status.Conditions[index]
		}
	}
	return nil
}

func deploymentCondition(deployment appsv1.Deployment, kind appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for index := range deployment.Status.Conditions {
		if deployment.Status.Conditions[index].Type == kind {
			return &deployment.Status.Conditions[index]
		}
	}
	return nil
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
