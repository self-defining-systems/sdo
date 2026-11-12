// Package workloadhealth holds an app-agnostic health check over Kubernetes
// workload state: failed, stuck, crashing, or unready pods and Deployments
// that do not reach their desired availability. It reports workloads, not
// Services; servicehealth covers Services without ready endpoints.
//
// Every rule except progress-deadline-exceeded reports a condition only once
// it outlived Kubernetes' own self-healing: it has held for the context's
// self-heal hold (sdk.HeldPastSelfHeal), measured from a Kubernetes timestamp.
// Ages use sdk.ObservationTime, never the wall clock, so one snapshot always
// yields the same findings. Container waiting states and not-ready are held
// from the Pod's ContainersReady=False transition, since a waiting state has
// no timestamp of its own.
package workloadhealth

import (
	"context"
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
	// RulePodPending: a pod stayed Pending past the hold since creation.
	RulePodPending = "workload-pod-pending"
	// RulePodUnschedulable: a pod stayed Pending because the scheduler found
	// no node (PodScheduled=False, Unschedulable) past the hold.
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
	// RuleNotReady: a running container of a Running pod is unready.
	RuleNotReady = "workload-container-not-ready"
	// RuleInitStuck: an init container has not completed.
	RuleInitStuck = "workload-init-container-stuck"
	// RuleDeploymentUnavailable: a Deployment reports Available=False with
	// fewer available replicas than desired, and no pod finding explains it.
	RuleDeploymentUnavailable = "workload-deployment-unavailable"
	// RuleReplicaFailure: a Deployment reports ReplicaFailure=True (a Pod
	// create was refused). Set on the first failed create, so it is held like
	// every other condition.
	RuleReplicaFailure = "workload-deployment-replica-failure"
	// RuleProgressDeadlineExceeded: a Deployment reports Progressing=False
	// with reason ProgressDeadlineExceeded, Kubernetes' own give-up signal.
	// It is not held.
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

type detector struct {
	spec sdk.DetectorSpec
}

// NewDetector returns the workload health detector.
func NewDetector(spec sdk.DetectorSpec) sdk.Detector {
	return detector{spec: spec}
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

// held reports whether a condition holding since since outlived the
// self-heal hold.
func (e *evaluation) held(since time.Time) bool {
	return sdk.HeldPastSelfHeal(e.ctx, since)
}

// podFalseSince is when a pod condition last turned False, or the pod's
// creation when the condition is absent or not False.
func podFalseSince(pod corev1.Pod, kind corev1.PodConditionType) time.Time {
	if condition := podCondition(pod, kind); condition != nil && condition.Status != corev1.ConditionTrue && !condition.LastTransitionTime.IsZero() {
		return condition.LastTransitionTime.Time
	}
	return pod.CreationTimestamp.Time
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
		if deployment != nil && deployment.Status.AvailableReplicas >= desiredReplicas(*deployment) || !e.held(podFalseSince(pod, corev1.PodReady)) {
			return
		}
		add(RulePodFailed, "is in phase Failed"+podReason(pod)+terminations(pod)+e.events("Pod", pod.Name), pod.Status.Reason)
		return
	case corev1.PodUnknown:
		if !e.held(podFalseSince(pod, corev1.PodReady)) {
			return
		}
		add(RulePodUnknown, "is in phase Unknown"+podReason(pod)+e.events("Pod", pod.Name), pod.Status.Reason)
		return
	}
	found := e.containers(pod, add)
	if found || pod.Status.Phase != corev1.PodPending {
		return
	}
	created := pod.CreationTimestamp.UTC().Format(time.RFC3339)
	if condition := podCondition(pod, corev1.PodScheduled); condition != nil &&
		condition.Status == corev1.ConditionFalse && condition.Reason == corev1.PodReasonUnschedulable {
		if !e.held(podFalseSince(pod, corev1.PodScheduled)) {
			return
		}
		add(RulePodUnschedulable, fmt.Sprintf("is Pending since %s and Unschedulable: %s%s",
			created, condition.Message, e.events("Pod", pod.Name)), corev1.PodReasonUnschedulable)
		return
	}
	if !e.held(pod.CreationTimestamp.Time) {
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
	}
	initialized := e.held(podFalseSince(pod, corev1.PodInitialized))
	for _, status := range pod.Status.InitContainerStatuses {
		label := "init container"
		if sidecars[status.Name] {
			label = "sidecar container"
		}
		if e.container(pod, status, label, report) {
			found = true
			continue
		}
		if sidecars[status.Name] {
			continue
		}
		if status.State.Running != nil {
			found = true
			if initialized {
				report(RuleInitStuck, label, status, "has been running since "+
					status.State.Running.StartedAt.UTC().Format(time.RFC3339)+" without completing", "InitContainerRunning")
			}
		} else if status.State.Waiting != nil {
			found = true
			if initialized {
				report(RuleInitStuck, label, status, "is still waiting ("+status.State.Waiting.Reason+")", status.State.Waiting.Reason)
			}
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		found = e.container(pod, status, "container", report) || found
	}
	return found
}

// container applies the per-container rules in precedence order and reports
// whether one matched. A matched rule is reported only once the Pod's
// ContainersReady=False transition is held past the self-heal hold.
func (e *evaluation) container(
	pod corev1.Pod,
	status corev1.ContainerStatus,
	label string,
	report func(rule, label string, status corev1.ContainerStatus, detail, reason string),
) bool {
	waiting := status.State.Waiting
	var rule, detail, reason string
	switch {
	case waiting != nil && isConfigError(waiting.Reason):
		rule = RuleConfigError
	case waiting != nil && (waiting.Reason == "ImagePullBackOff" || waiting.Reason == "ErrImagePull"):
		rule = RuleImagePull
	case waiting != nil && waiting.Reason == "CrashLoopBackOff":
		rule = RuleCrashLooping
	case waiting != nil && waiting.Reason != "" && waiting.Reason != "ContainerCreating" && waiting.Reason != "PodInitializing":
		rule = RuleWaiting
	case label == "container" && pod.Status.Phase == corev1.PodRunning && status.State.Running != nil && !status.Ready:
		rule, reason = RuleNotReady, "NotReady"
		detail = "is running since " + status.State.Running.StartedAt.UTC().Format(time.RFC3339) + " but not ready"
	default:
		return false
	}
	if waiting != nil {
		detail, reason = "is waiting: "+waiting.Reason+optionalMessage(waiting.Message), waiting.Reason
	}
	if e.held(podFalseSince(pod, corev1.ContainersReady)) {
		report(rule, label, status, detail, reason)
	}
	return true
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
	if failure := deploymentCondition(deployment, appsv1.DeploymentReplicaFailure); failure != nil &&
		failure.Status == corev1.ConditionTrue && e.held(failure.LastTransitionTime.Time) {
		e.addDeployment(RuleReplicaFailure, ref, deployment, pods, fmt.Sprintf(
			"Deployment %s cannot create replicas since %s; %s.%s%s", deployment.Name,
			failure.LastTransitionTime.UTC().Format(time.RFC3339), availability, replicaFailure(deployment),
			e.deploymentEvents(deployment)), failure.Reason)
		return
	}
	if status.AvailableReplicas >= desired || e.hasPodFinding(ref) {
		return
	}
	available := deploymentCondition(deployment, appsv1.DeploymentAvailable)
	if available == nil || available.Status != corev1.ConditionFalse || !e.held(available.LastTransitionTime.Time) {
		return
	}
	e.addDeployment(RuleDeploymentUnavailable, ref, deployment, pods, fmt.Sprintf(
		"Deployment %s has %s; unavailable since %s.%s%s", deployment.Name, availability,
		available.LastTransitionTime.UTC().Format(time.RFC3339), replicaFailure(deployment), e.deploymentEvents(deployment)), "Unavailable")
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
	RuleNotReady:                 "container running but not ready",
	RuleInitStuck:                "init container not completing",
	RuleDeploymentUnavailable:    "fewer available replicas than desired",
	RuleReplicaFailure:           "replicas cannot be created",
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
