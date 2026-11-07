//go:build integration

// Package rehearsal is the generic replay-rehearsal harness: it stands in for
// the parts of a cluster a real API server (envtest) lacks, the kubelet, the
// workload controllers and the garbage collector, so SDO's deployer and
// advancer run unchanged against real API-server semantics (immutable
// fields, defaulting, server-side apply, generation, RBAC) with no
// containers. Pods are not executed: their phase follows Behaviors on an
// injected clock. Test-only; nothing outside integration tests imports it.
package rehearsal

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/internal/rehearsal/rehearsalio"
)

// Behavior changes how the fake kubelet runs the pods of matching workloads.
// The zero Behavior is the default: pods become ready, and a Job's first
// attempt succeeds, one step after they are created.
type Behavior struct {
	// Kind is the workload kind (Deployment, StatefulSet, DaemonSet, Job); empty
	// matches every kind.
	Kind string `json:"kind,omitempty"`
	// Name is the workload name, or a prefix ending in "*".
	Name string `json:"name"`
	// ReadyAfter is how long a created pod stays unready (injected clock).
	ReadyAfter Duration `json:"ready_after,omitempty"`
	// JobFailures is how many attempts of a Job fail before one succeeds; a
	// negative value fails every attempt.
	JobFailures int `json:"job_failures,omitempty"`
	// AttemptDuration is how long one Job attempt runs before it ends
	// (default: until the next step).
	AttemptDuration Duration `json:"attempt_duration,omitempty"`
	// ExitCode is the failing attempts' exit code (1 when zero).
	ExitCode int32 `json:"exit_code,omitempty"`
	// FromRelease applies the behavior only from that release index on
	// (0 is the baseline), so a Job can succeed at install and fail when the
	// deployer recreates it.
	FromRelease int `json:"from_release,omitempty"`
	// UntilRelease, when positive, ends the behavior after that release.
	UntilRelease int `json:"until_release,omitempty"`
}

// Duration is a time.Duration that reads "90s"-style strings from JSON.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(text)
	*d = Duration(parsed)
	return err
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (b Behavior) matches(kind, name string, release int) bool {
	if release < b.FromRelease || (b.UntilRelease > 0 && release > b.UntilRelease) || (b.Kind != "" && b.Kind != kind) {
		return false
	}
	if prefix, ok := strings.CutSuffix(b.Name, "*"); ok {
		return strings.HasPrefix(name, prefix)
	}
	return b.Name == name
}

// Kubelet reconciles one namespace each Step: the Deployment, ReplicaSet,
// StatefulSet, DaemonSet and Job controllers' status and pods, the pods'
// phase, and the garbage collection of dependents whose owner is gone. Run
// starts the part that must not wait for a Step: the garbage collector's
// handling of orphan and foreground deletion finalizers, which a blocking
// `kubectl delete --wait` needs.
type Kubelet struct {
	Client    kubernetes.Interface
	Namespace string
	Behaviors []Behavior
	// Now is the injected clock pods are aged on.
	Now func() time.Time

	mu      sync.Mutex
	release int
	created map[types.UID]time.Time
	// attempt is when a Job pod's current container attempt started, and
	// restarts how often it restarted in place (restartPolicy OnFailure).
	attempt  map[types.UID]time.Time
	restarts map[types.UID]int32
	lastExit map[types.UID]*corev1.ContainerStateTerminated
	// scaled is the autoscaler targets already settled in a release.
	scaled map[string]bool
	// Errors are reconcile failures other than conflicts (each one once).
	Errors []string
	seen   map[string]bool
}

// SetRelease selects the release index Behaviors match against.
func (k *Kubelet) SetRelease(index int) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.release = index
}

func (k *Kubelet) behavior(kind, name string) Behavior {
	for _, b := range k.Behaviors {
		if b.matches(kind, name, k.release) {
			return b
		}
	}
	return Behavior{}
}

func (k *Kubelet) fail(err error) {
	if err == nil || apierrors.IsConflict(err) || apierrors.IsNotFound(err) || apierrors.IsAlreadyExists(err) {
		return
	}
	if k.seen == nil {
		k.seen = map[string]bool{}
	}
	if !k.seen[err.Error()] {
		k.seen[err.Error()] = true
		k.Errors = append(k.Errors, err.Error())
	}
}

// Step runs one reconcile pass at the clock's current time.
func (k *Kubelet) Step(ctx context.Context) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.created == nil {
		k.created = map[types.UID]time.Time{}
		k.attempt = map[types.UID]time.Time{}
		k.restarts = map[types.UID]int32{}
		k.lastExit = map[types.UID]*corev1.ContainerStateTerminated{}
	}
	k.collect(ctx)
	k.autoscale(ctx)
	deployments, err := k.Client.AppsV1().Deployments(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err == nil {
		for i := range deployments.Items {
			k.deployment(ctx, &deployments.Items[i])
		}
	}
	sets, err := k.Client.AppsV1().StatefulSets(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err == nil {
		for i := range sets.Items {
			k.statefulSet(ctx, &sets.Items[i])
		}
	}
	daemons, err := k.Client.AppsV1().DaemonSets(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err == nil {
		for i := range daemons.Items {
			k.daemonSet(ctx, &daemons.Items[i])
		}
	}
	jobs, err := k.Client.BatchV1().Jobs(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err == nil {
		for i := range jobs.Items {
			k.job(ctx, &jobs.Items[i])
		}
	}
	k.barePods(ctx)
}

func templateHash(template corev1.PodTemplateSpec) string {
	data, _ := json.Marshal(template)
	h := fnv.New32a()
	_, _ = h.Write(data)
	return fmt.Sprintf("%08x", h.Sum32())[:8]
}

func owner(kind string, gv schema.GroupVersion, meta metav1.ObjectMeta) metav1.OwnerReference {
	yes := true
	return metav1.OwnerReference{APIVersion: gv.String(), Kind: kind, Name: meta.Name, UID: meta.UID, Controller: &yes, BlockOwnerDeletion: &yes}
}

func controlledBy(meta metav1.ObjectMeta, uid types.UID) bool {
	for _, ref := range meta.OwnerReferences {
		if ref.Controller != nil && *ref.Controller && ref.UID == uid {
			return true
		}
	}
	return false
}

func (k *Kubelet) ownedPods(ctx context.Context, uid types.UID) []corev1.Pod {
	pods, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err != nil {
		return nil
	}
	var out []corev1.Pod
	for _, pod := range pods.Items {
		if controlledBy(pod.ObjectMeta, uid) && pod.DeletionTimestamp == nil {
			out = append(out, pod)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ensurePods keeps exactly the named pods of a controller at the template
// and returns how many of them are ready.
func (k *Kubelet) ensurePods(ctx context.Context, ref metav1.OwnerReference, names []string, template corev1.PodTemplateSpec, labels map[string]string, behavior Behavior) int {
	return k.ensureEachPod(ctx, ref, names, func(string) corev1.PodTemplateSpec { return template }, labels, behavior)
}

// ensureEachPod is ensurePods with a template per pod name.
func (k *Kubelet) ensureEachPod(ctx context.Context, ref metav1.OwnerReference, names []string, templateOf func(string) corev1.PodTemplateSpec, labels map[string]string, behavior Behavior) int {
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
	}
	ready := 0
	have := map[string]bool{}
	for _, pod := range k.ownedPods(ctx, ref.UID) {
		if !want[pod.Name] || !subset(labels, pod.Labels) {
			k.fail(k.Client.CoreV1().Pods(k.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
			continue
		}
		have[pod.Name] = true
		if k.runPod(ctx, &pod, behavior, true) {
			ready++
		}
	}
	for _, name := range names {
		if have[name] {
			continue
		}
		template := templateOf(name)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace, Labels: merge(template.Labels, labels), Annotations: template.Annotations, OwnerReferences: []metav1.OwnerReference{ref}}, Spec: *template.Spec.DeepCopy()}
		created, err := k.Client.CoreV1().Pods(k.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		k.fail(err)
		if err == nil {
			k.created[created.UID] = k.Now()
			if k.runPod(ctx, created, behavior, true) {
				ready++
			}
		}
	}
	return ready
}

// runPod moves a long-running pod to Running and, after the behavior's delay,
// Ready. It reports whether the pod is ready.
func (k *Kubelet) runPod(ctx context.Context, pod *corev1.Pod, behavior Behavior, _ bool) bool {
	started, ok := k.created[pod.UID]
	if !ok {
		started = k.Now()
		k.created[pod.UID] = started
	}
	ready := !k.Now().Before(started.Add(time.Duration(behavior.ReadyAfter)))
	if podReady(pod) == ready && pod.Status.Phase == corev1.PodRunning {
		return ready
	}
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	now := metav1.NewTime(k.Now())
	pod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning, StartTime: &metav1.Time{Time: started},
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.ContainersReady, Status: status, LastTransitionTime: now},
			{Type: corev1.PodReady, Status: status, LastTransitionTime: now},
		},
	}
	for _, c := range pod.Spec.Containers {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
			Name: c.Name, Image: c.Image, ImageID: c.Image, Ready: ready, Started: &ready,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Time{Time: started}}},
		})
	}
	_, err := k.Client.CoreV1().Pods(k.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	k.fail(err)
	return ready && err == nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func subset(want, have map[string]string) bool {
	for key, value := range want {
		if have[key] != value {
			return false
		}
	}
	return true
}

func merge(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for key, value := range m {
			out[key] = value
		}
	}
	return out
}

func replicas(n *int32) int32 {
	if n == nil {
		return 1
	}
	return *n
}

func (k *Kubelet) deployment(ctx context.Context, d *appsv1.Deployment) {
	if d.DeletionTimestamp != nil {
		return
	}
	hash := templateHash(d.Spec.Template)
	name := d.Name + "-" + hash
	desired := replicas(d.Spec.Replicas)
	sets, err := k.Client.AppsV1().ReplicaSets(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err != nil {
		return
	}
	var current *appsv1.ReplicaSet
	created := false
	for i := range sets.Items {
		rs := &sets.Items[i]
		if !controlledBy(rs.ObjectMeta, d.UID) {
			continue
		}
		if rs.Name == name {
			current = rs
			continue
		}
		// Recreate-style rollout: the old ReplicaSet goes at once.
		k.fail(k.Client.AppsV1().ReplicaSets(k.Namespace).Delete(ctx, rs.Name, metav1.DeleteOptions{}))
	}
	labels := map[string]string{"pod-template-hash": hash}
	if current == nil {
		template := *d.Spec.Template.DeepCopy()
		template.Labels = merge(template.Labels, labels)
		selector := d.Spec.Selector.DeepCopy()
		selector.MatchLabels = merge(selector.MatchLabels, labels)
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace, Labels: template.Labels, OwnerReferences: []metav1.OwnerReference{owner("Deployment", appsv1.SchemeGroupVersion, d.ObjectMeta)}},
			Spec:       appsv1.ReplicaSetSpec{Replicas: &desired, Selector: selector, Template: template},
		}
		current, err = k.Client.AppsV1().ReplicaSets(k.Namespace).Create(ctx, rs, metav1.CreateOptions{})
		k.fail(err)
		if err != nil {
			return
		}
		created = true
	}
	k.recordRevision(ctx, d, created)
	var names []string
	for i := int32(0); i < desired; i++ {
		names = append(names, fmt.Sprintf("%s-%05d", current.Name, i))
	}
	ready := int32(k.ensurePods(ctx, owner("ReplicaSet", appsv1.SchemeGroupVersion, current.ObjectMeta), names, current.Spec.Template, labels, k.behavior("Deployment", d.Name)))
	current.Status = appsv1.ReplicaSetStatus{Replicas: desired, FullyLabeledReplicas: desired, ReadyReplicas: ready, AvailableReplicas: ready, ObservedGeneration: current.Generation}
	_, err = k.Client.AppsV1().ReplicaSets(k.Namespace).UpdateStatus(ctx, current, metav1.UpdateOptions{})
	k.fail(err)
	now := metav1.NewTime(k.Now())
	available := corev1.ConditionFalse
	if ready >= desired {
		available = corev1.ConditionTrue
	}
	status := appsv1.DeploymentStatus{
		ObservedGeneration: d.Generation, Replicas: desired, UpdatedReplicas: desired, ReadyReplicas: ready, AvailableReplicas: ready, UnavailableReplicas: desired - ready,
		Conditions: []appsv1.DeploymentCondition{
			{Type: appsv1.DeploymentAvailable, Status: available, Reason: "MinimumReplicas", LastUpdateTime: now, LastTransitionTime: now},
			{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable", LastUpdateTime: now, LastTransitionTime: now},
		},
	}
	if statusEqual(d.Status, status) {
		return
	}
	d.Status = status
	_, err = k.Client.AppsV1().Deployments(k.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
	k.fail(err)
}

// statusEqual ignores condition timestamps, so an unchanged status is not
// rewritten (each write would bump resourceVersion only).
func statusEqual(a, b any) bool {
	strip := func(v any) string {
		data, _ := json.Marshal(v)
		var m any
		_ = json.Unmarshal(data, &m)
		var walk func(any)
		walk = func(x any) {
			switch x := x.(type) {
			case map[string]any:
				delete(x, "lastUpdateTime")
				delete(x, "lastTransitionTime")
				for _, v := range x {
					walk(v)
				}
			case []any:
				for _, v := range x {
					walk(v)
				}
			}
		}
		walk(m)
		out, _ := json.Marshal(m)
		return string(out)
	}
	return strip(a) == strip(b)
}

func (k *Kubelet) statefulSet(ctx context.Context, s *appsv1.StatefulSet) {
	if s.DeletionTimestamp != nil {
		return
	}
	hash := templateHash(s.Spec.Template)
	revision := s.Name + "-" + hash
	desired := replicas(s.Spec.Replicas)
	var names []string
	for i := int32(0); i < desired; i++ {
		names = append(names, fmt.Sprintf("%s-%d", s.Name, i))
	}
	labels := map[string]string{"controller-revision-hash": revision}
	// The StatefulSet controller creates one claim per template and pod
	// (kept after the pod, retention Retain) and mounts it by template name.
	templateOf := func(name string) corev1.PodTemplateSpec {
		pod := *s.Spec.Template.DeepCopy()
		for _, claim := range s.Spec.VolumeClaimTemplates {
			claimName := claim.Name + "-" + name
			k.ensureClaim(ctx, claim, claimName)
			pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: claim.Name, VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName}}})
		}
		return pod
	}
	ready := int32(k.ensureEachPod(ctx, owner("StatefulSet", appsv1.SchemeGroupVersion, s.ObjectMeta), names, templateOf, labels, k.behavior("StatefulSet", s.Name)))
	status := appsv1.StatefulSetStatus{
		ObservedGeneration: s.Generation, Replicas: desired, ReadyReplicas: ready, AvailableReplicas: ready, CurrentReplicas: desired, UpdatedReplicas: desired,
		CurrentRevision: revision, UpdateRevision: revision, CollisionCount: s.Status.CollisionCount,
	}
	if statusEqual(s.Status, status) {
		return
	}
	s.Status = status
	_, err := k.Client.AppsV1().StatefulSets(k.Namespace).UpdateStatus(ctx, s, metav1.UpdateOptions{})
	k.fail(err)
}

// ensureClaim creates a StatefulSet's claim once and binds it.
func (k *Kubelet) ensureClaim(ctx context.Context, template corev1.PersistentVolumeClaim, name string) {
	claims := k.Client.CoreV1().PersistentVolumeClaims(k.Namespace)
	claim, err := claims.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		created := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace, Labels: template.Labels}, Spec: *template.Spec.DeepCopy()}
		claim, err = claims.Create(ctx, created, metav1.CreateOptions{})
	}
	k.fail(err)
	if err != nil || claim.Status.Phase == corev1.ClaimBound {
		return
	}
	claim.Status.Phase = corev1.ClaimBound
	claim.Status.AccessModes = claim.Spec.AccessModes
	claim.Status.Capacity = claim.Spec.Resources.Requests
	_, err = claims.UpdateStatus(ctx, claim, metav1.UpdateOptions{})
	k.fail(err)
}

func (k *Kubelet) daemonSet(ctx context.Context, s *appsv1.DaemonSet) {
	if s.DeletionTimestamp != nil {
		return
	}
	hash := templateHash(s.Spec.Template)
	labels := map[string]string{"controller-revision-hash": hash}
	ready := int32(k.ensurePods(ctx, owner("DaemonSet", appsv1.SchemeGroupVersion, s.ObjectMeta), []string{s.Name + "-node0"}, s.Spec.Template, labels, k.behavior("DaemonSet", s.Name)))
	status := appsv1.DaemonSetStatus{
		ObservedGeneration: s.Generation, DesiredNumberScheduled: 1, CurrentNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberReady: ready, NumberAvailable: ready, NumberUnavailable: 1 - ready,
	}
	if statusEqual(s.Status, status) {
		return
	}
	s.Status = status
	_, err := k.Client.AppsV1().DaemonSets(k.Namespace).UpdateStatus(ctx, s, metav1.UpdateOptions{})
	k.fail(err)
}

func jobCondition(job *batchv1.Job, kind batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == kind && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// job runs a Job's attempts one pod at a time (restartPolicy Never): an
// attempt ends one step after its pod was created; it fails while the
// behavior's failures last; past backoffLimit failures the Job is Failed.
func (k *Kubelet) job(ctx context.Context, job *batchv1.Job) {
	if job.DeletionTimestamp != nil || jobCondition(job, batchv1.JobComplete) || jobCondition(job, batchv1.JobFailed) {
		return
	}
	behavior := k.behavior("Job", job.Name)
	exit := behavior.ExitCode
	if exit == 0 {
		exit = 1
	}
	limit := int32(6)
	if job.Spec.BackoffLimit != nil {
		limit = *job.Spec.BackoffLimit
	}
	pods := k.ownedPods(ctx, job.UID)
	var failed, succeeded, active, restarts int32
	for _, pod := range pods {
		restarts += k.restarts[pod.UID]
	}
	now := k.Now()
	for i := range pods {
		pod := &pods[i]
		switch pod.Status.Phase {
		case corev1.PodFailed:
			failed++
			continue
		case corev1.PodSucceeded:
			succeeded++
			continue
		}
		started, ok := k.attempt[pod.UID]
		if !ok {
			started = now
			k.attempt[pod.UID] = now
		}
		if now.Sub(started) <= 0 || now.Sub(started) < time.Duration(behavior.AttemptDuration) {
			active++
			k.runJobPod(ctx, pod, started)
			continue
		}
		attempt := failed + restarts
		fails := behavior.JobFailures < 0 || int(attempt) < behavior.JobFailures
		if fails && pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure && attempt < limit {
			// The kubelet restarts the container in the same pod; the Job
			// controller counts the restart toward backoffLimit.
			k.restarts[pod.UID]++
			restarts++
			k.attempt[pod.UID] = now
			k.lastExit[pod.UID] = &corev1.ContainerStateTerminated{ExitCode: exit, Reason: "Error", StartedAt: metav1.Time{Time: started}, FinishedAt: metav1.NewTime(now)}
			active++
			k.runJobPod(ctx, pod, now)
			continue
		}
		code := int32(0)
		pod.Status.Phase = corev1.PodSucceeded
		reason := "Completed"
		if fails {
			code, pod.Status.Phase, reason = exit, corev1.PodFailed, "Error"
		}
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: "PodCompleted", LastTransitionTime: metav1.NewTime(now)}}
		pod.Status.ContainerStatuses = nil
		for _, c := range pod.Spec.Containers {
			pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
				Name: c.Name, Image: c.Image, ImageID: c.Image,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Reason: reason, StartedAt: metav1.Time{Time: started}, FinishedAt: metav1.NewTime(now)}},
			})
		}
		_, err := k.Client.CoreV1().Pods(k.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
		k.fail(err)
		if fails {
			failed++
		} else {
			succeeded++
		}
	}
	completions := replicas(job.Spec.Completions)
	start := job.Status.StartTime
	if start == nil {
		start = &metav1.Time{Time: now}
	}
	status := batchv1.JobStatus{StartTime: start, Failed: failed, Succeeded: succeeded, Conditions: job.Status.Conditions}
	ready := int32(0)
	status.Ready = &ready
	stamp := metav1.NewTime(now)
	switch {
	case succeeded >= completions:
		status.Conditions = append(status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, Reason: "CompletionsReached", LastProbeTime: stamp, LastTransitionTime: stamp},
			batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, Reason: "CompletionsReached", LastProbeTime: stamp, LastTransitionTime: stamp})
		status.CompletionTime = &stamp
	case failed+restarts > limit:
		status.Conditions = append(status.Conditions,
			batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit", LastProbeTime: stamp, LastTransitionTime: stamp},
			batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit", LastProbeTime: stamp, LastTransitionTime: stamp})
	case active == 0:
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%d", job.Name, failed+succeeded), Namespace: k.Namespace, Labels: job.Spec.Template.Labels,
			Annotations: job.Spec.Template.Annotations, OwnerReferences: []metav1.OwnerReference{owner("Job", batchv1.SchemeGroupVersion, job.ObjectMeta)},
		}, Spec: *job.Spec.Template.Spec.DeepCopy()}
		created, err := k.Client.CoreV1().Pods(k.Namespace).Create(ctx, pod, metav1.CreateOptions{})
		k.fail(err)
		if err == nil {
			k.created[created.UID] = now
			k.attempt[created.UID] = now
			k.runJobPod(ctx, created, now)
			active = 1
		}
	default:
	}
	status.Active = active
	if statusEqual(job.Status, status) {
		return
	}
	job.Status = status
	_, err := k.Client.BatchV1().Jobs(k.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{})
	k.fail(err)
}

// runJobPod shows a Job pod's running attempt: Running, not ready (Job
// containers have no readiness), with its in-place restarts and last exit.
func (k *Kubelet) runJobPod(ctx context.Context, pod *corev1.Pod, attempt time.Time) {
	restarts := k.restarts[pod.UID]
	if pod.Status.Phase == corev1.PodRunning && len(pod.Status.ContainerStatuses) > 0 && pod.Status.ContainerStatuses[0].RestartCount == restarts {
		return
	}
	created := k.created[pod.UID]
	if created.IsZero() {
		created = attempt
	}
	now := metav1.NewTime(k.Now())
	pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, StartTime: &metav1.Time{Time: created}, Conditions: []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
		{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: now},
	}}
	for _, c := range pod.Spec.Containers {
		status := corev1.ContainerStatus{Name: c.Name, Image: c.Image, ImageID: c.Image, RestartCount: restarts,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Time{Time: attempt}}}}
		if last := k.lastExit[pod.UID]; last != nil {
			status.LastTerminationState = corev1.ContainerState{Terminated: last}
		}
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, status)
	}
	_, err := k.Client.CoreV1().Pods(k.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	k.fail(err)
}

// barePods runs pods no controller owns.
func (k *Kubelet) barePods(ctx context.Context) {
	pods, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{})
	k.fail(err)
	if err != nil {
		return
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if len(pod.OwnerReferences) == 0 && pod.DeletionTimestamp == nil {
			k.runPod(ctx, pod, k.behavior("Pod", pod.Name), true)
		}
	}
}

// collect deletes ReplicaSets and pods whose controller is gone (background
// cascade), as the garbage collector does.
func (k *Kubelet) collect(ctx context.Context) {
	alive := map[types.UID]bool{}
	note := func(items []metav1.ObjectMeta) {
		for _, meta := range items {
			alive[meta.UID] = true
		}
	}
	var metas []metav1.ObjectMeta
	if list, err := k.Client.AppsV1().Deployments(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			metas = append(metas, o.ObjectMeta)
		}
	}
	if list, err := k.Client.AppsV1().StatefulSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			metas = append(metas, o.ObjectMeta)
		}
	}
	if list, err := k.Client.AppsV1().DaemonSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			metas = append(metas, o.ObjectMeta)
		}
	}
	if list, err := k.Client.BatchV1().Jobs(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			metas = append(metas, o.ObjectMeta)
		}
	}
	sets, err := k.Client.AppsV1().ReplicaSets(k.Namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		for _, o := range sets.Items {
			metas = append(metas, o.ObjectMeta)
		}
	}
	note(metas)
	dangling := func(meta metav1.ObjectMeta) bool {
		for _, ref := range meta.OwnerReferences {
			if ref.Controller != nil && *ref.Controller && !alive[ref.UID] {
				return true
			}
		}
		return false
	}
	if err == nil {
		for _, rs := range sets.Items {
			if dangling(rs.ObjectMeta) {
				k.fail(k.Client.AppsV1().ReplicaSets(k.Namespace).Delete(ctx, rs.Name, metav1.DeleteOptions{}))
				delete(alive, rs.UID)
			}
		}
	}
	if pods, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, pod := range pods.Items {
			if dangling(pod.ObjectMeta) {
				k.fail(k.Client.CoreV1().Pods(k.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
			}
		}
	}
}

// RunFinalizers handles the garbage collector's orphan and foreground
// finalizers until ctx ends, polling every interval in real time: a deletion
// with --cascade=orphan or foreground blocks the deleting client until they
// are gone, so they cannot wait for the next Step.
func (k *Kubelet) RunFinalizers(ctx context.Context, interval time.Duration) {
	rehearsalio.Every(ctx, interval, func() { k.finalize(ctx) })
}

func (k *Kubelet) finalize(ctx context.Context) {
	type deleting struct {
		meta  metav1.ObjectMeta
		patch func(types.UID, []string) error
	}
	var objects []deleting
	appsPatch := func(resource string) func(string) func(types.UID, []string) error {
		return func(name string) func(types.UID, []string) error {
			return func(_ types.UID, finalizers []string) error {
				data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": finalizers}})
				_, err := k.Client.AppsV1().RESTClient().Patch(types.MergePatchType).Namespace(k.Namespace).Resource(resource).Name(name).Body(data).Do(ctx).Get()
				return err
			}
		}
	}
	if list, err := k.Client.AppsV1().Deployments(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			objects = append(objects, deleting{o.ObjectMeta, appsPatch("deployments")(o.Name)})
		}
	}
	if list, err := k.Client.AppsV1().StatefulSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			objects = append(objects, deleting{o.ObjectMeta, appsPatch("statefulsets")(o.Name)})
		}
	}
	if list, err := k.Client.AppsV1().DaemonSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			objects = append(objects, deleting{o.ObjectMeta, appsPatch("daemonsets")(o.Name)})
		}
	}
	if list, err := k.Client.AppsV1().ReplicaSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			objects = append(objects, deleting{o.ObjectMeta, appsPatch("replicasets")(o.Name)})
		}
	}
	if list, err := k.Client.BatchV1().Jobs(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			name := o.Name
			objects = append(objects, deleting{o.ObjectMeta, func(_ types.UID, finalizers []string) error {
				data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": finalizers}})
				_, err := k.Client.BatchV1().Jobs(k.Namespace).Patch(ctx, name, types.MergePatchType, data, metav1.PatchOptions{})
				return err
			}})
		}
	}
	for _, o := range objects {
		if o.meta.DeletionTimestamp == nil {
			continue
		}
		var keep []string
		handled := false
		for _, f := range o.meta.Finalizers {
			switch f {
			case metav1.FinalizerOrphanDependents:
				handled = true
				k.orphan(ctx, o.meta.UID)
			case metav1.FinalizerDeleteDependents:
				handled = true
				k.deleteDependents(ctx, o.meta.UID)
			default:
				keep = append(keep, f)
			}
		}
		if handled {
			if keep == nil {
				keep = []string{}
			}
			if err := o.patch(o.meta.UID, keep); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				k.mu.Lock()
				k.fail(err)
				k.mu.Unlock()
			}
		}
	}
}

func withoutOwner(refs []metav1.OwnerReference, uid types.UID) []metav1.OwnerReference {
	out := []metav1.OwnerReference{}
	for _, ref := range refs {
		if ref.UID != uid {
			out = append(out, ref)
		}
	}
	return out
}

func (k *Kubelet) orphan(ctx context.Context, uid types.UID) {
	patch := func(refs []metav1.OwnerReference) []byte {
		data, _ := json.Marshal(map[string]any{"metadata": map[string]any{"ownerReferences": withoutOwner(refs, uid)}})
		return data
	}
	if list, err := k.Client.AppsV1().ReplicaSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			if hasOwner(o.OwnerReferences, uid) {
				_, _ = k.Client.AppsV1().ReplicaSets(k.Namespace).Patch(ctx, o.Name, types.MergePatchType, patch(o.OwnerReferences), metav1.PatchOptions{})
			}
		}
	}
	if list, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			if hasOwner(o.OwnerReferences, uid) {
				_, _ = k.Client.CoreV1().Pods(k.Namespace).Patch(ctx, o.Name, types.MergePatchType, patch(o.OwnerReferences), metav1.PatchOptions{})
			}
		}
	}
}

func (k *Kubelet) deleteDependents(ctx context.Context, uid types.UID) {
	if list, err := k.Client.AppsV1().ReplicaSets(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			if hasOwner(o.OwnerReferences, uid) {
				k.deleteDependents(ctx, o.UID)
				_ = k.Client.AppsV1().ReplicaSets(k.Namespace).Delete(ctx, o.Name, metav1.DeleteOptions{})
			}
		}
	}
	if list, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, o := range list.Items {
			if hasOwner(o.OwnerReferences, uid) {
				_ = k.Client.CoreV1().Pods(k.Namespace).Delete(ctx, o.Name, metav1.DeleteOptions{})
			}
		}
	}
}

func hasOwner(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.UID == uid {
			return true
		}
	}
	return false
}
