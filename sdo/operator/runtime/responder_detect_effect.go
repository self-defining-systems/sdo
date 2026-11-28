package runtime

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/deployment"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/traffic"
)

// knownChange is the newest set of writes SDO knows the responder session
// made since `after`, read with the agent-cluster recording off: every agent
// write goes through the proxy, which forces sdk.ResponderFieldManager, so the
// objects' managedFields say what the agent wrote and when; `sdo mitigate`
// and stage applications are in the loop's own application log.
type knownChange struct {
	// At is the newest write's time.
	At time.Time
	// Workloads are the written Deployments, StatefulSets and DaemonSets,
	// whose rollouts the change's result waits on.
	Workloads []sdk.ObjectRef
	// Writes names what was written, for the wait line.
	Writes []string
}

// responderWriteAt is the newest time the responder's field manager wrote
// object; zero when it never did.
func responderWriteAt(meta metav1.ObjectMeta) time.Time {
	var latest time.Time
	for _, entry := range meta.ManagedFields {
		if entry.Manager == sdk.ResponderFieldManager && entry.Time != nil && entry.Time.After(latest) {
			latest = entry.Time.UTC()
		}
	}
	return latest
}

// latestKnownChange collects the writes after `after`; ok is false without one.
func latestKnownChange(view sdk.DetectionContext, applications []RecordedPlanApplication, after time.Time) (knownChange, bool) {
	var change knownChange
	note := func(kind string, meta metav1.ObjectMeta, workload bool) {
		at := responderWriteAt(meta)
		if !at.After(after) {
			return
		}
		ref := sdk.ObjectRef{APIVersion: "v1", Kind: kind, Namespace: meta.Namespace, Name: meta.Name}
		if workload {
			ref.APIVersion = "apps/v1"
			change.Workloads = append(change.Workloads, ref)
		}
		change.Writes = append(change.Writes, kind+"/"+meta.Name)
		if at.After(change.At) {
			change.At = at
		}
	}
	if view != nil {
		for _, o := range view.Deployments() {
			note("Deployment", o.ObjectMeta, true)
		}
		for _, o := range view.StatefulSets() {
			note("StatefulSet", o.ObjectMeta, true)
		}
		for _, o := range view.DaemonSets() {
			note("DaemonSet", o.ObjectMeta, true)
		}
		for _, o := range view.ConfigMaps() {
			note("ConfigMap", o.ObjectMeta, false)
		}
		for _, o := range view.Services() {
			note("Service", o.ObjectMeta, false)
		}
		for _, o := range view.NetworkPolicies() {
			note("NetworkPolicy", o.ObjectMeta, false)
		}
		for _, o := range view.PersistentVolumeClaims() {
			note("PersistentVolumeClaim", o.ObjectMeta, false)
		}
		for _, o := range view.Pods() {
			note("Pod", o.ObjectMeta, false)
		}
	}
	for _, application := range applications {
		at := application.CompletedAt
		if at.IsZero() {
			at = application.StartedAt
		}
		if !at.After(after) {
			continue
		}
		change.Writes = append(change.Writes, "plan "+strings.Join(application.PlanIDs, ","))
		if at.After(change.At) {
			change.At = at.UTC()
		}
	}
	sort.Strings(change.Writes)
	change.Writes = slices.Compact(change.Writes)
	return change, !change.At.IsZero()
}

// rolloutPending says why a workload's rollout is not done, "" when it is
// done; failed names a rollout that cannot finish (a Pod created by the
// change is crash looping or cannot pull its image), which is a result too.
func rolloutPending(view sdk.DetectionContext, ref sdk.ObjectRef, since time.Time) (pending string, failed string) {
	var selector *metav1.LabelSelector
	switch ref.Kind {
	case "Deployment":
		for i, o := range view.Deployments() {
			if o.Name == ref.Name {
				selector = o.Spec.Selector
				if !deployment.DeploymentRolloutComplete(&view.Deployments()[i]) {
					pending = fmt.Sprintf("%d/%d updated, %d available", o.Status.UpdatedReplicas, replicasOf(o.Spec.Replicas), o.Status.AvailableReplicas)
				}
				if c := deploymentCondition(o, appsv1.DeploymentProgressing); c != nil && c.Status == corev1.ConditionFalse {
					failed = c.Reason
				}
			}
		}
	case "StatefulSet":
		for i, o := range view.StatefulSets() {
			if o.Name == ref.Name {
				selector = o.Spec.Selector
				if !deployment.StatefulSetRolloutComplete(&view.StatefulSets()[i]) {
					pending = fmt.Sprintf("%d/%d updated, %d ready", o.Status.UpdatedReplicas, replicasOf(o.Spec.Replicas), o.Status.ReadyReplicas)
				}
			}
		}
	case "DaemonSet":
		for i, o := range view.DaemonSets() {
			if o.Name == ref.Name {
				selector = o.Spec.Selector
				if !deployment.DaemonSetRolloutComplete(&view.DaemonSets()[i]) {
					pending = fmt.Sprintf("%d/%d updated, %d available", o.Status.UpdatedNumberScheduled, o.Status.DesiredNumberScheduled, o.Status.NumberAvailable)
				}
			}
		}
	}
	if pending == "" || failed != "" {
		return pending, failed
	}
	if match, err := metav1.LabelSelectorAsSelector(selector); err == nil && selector != nil {
		for _, pod := range view.Pods() {
			if pod.CreationTimestamp.Time.Before(since.Add(-time.Second)) || !match.Matches(labels.Set(pod.Labels)) {
				continue
			}
			for _, status := range pod.Status.ContainerStatuses {
				if w := status.State.Waiting; w != nil && slices.Contains(rolloutFailureReasons, w.Reason) {
					return pending, fmt.Sprintf("Pod %s %s", pod.Name, w.Reason)
				}
			}
		}
	}
	return pending, ""
}

// rolloutFailureReasons are the waiting reasons of a new Pod that a rollout
// does not get past by itself.
var rolloutFailureReasons = []string{"CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError", "InvalidImageName"}

func replicasOf(replicas *int32) int32 {
	if replicas == nil {
		return 1
	}
	return *replicas
}

func deploymentCondition(o appsv1.Deployment, kind appsv1.DeploymentConditionType) *appsv1.DeploymentCondition {
	for i := range o.Status.Conditions {
		if o.Status.Conditions[i].Type == kind {
			return &o.Status.Conditions[i]
		}
	}
	return nil
}

// effectWatch follows one `sdo detect --wait` toward the result of the
// known change: its rollouts done (or failed) and one scheduled sample of
// each failing alert-set user test taken after that.
type effectWatch struct {
	// doneAt is when this wait first saw each workload's rollout done.
	doneAt map[sdk.ObjectRef]time.Time
}

// effectResult is one evaluation: in is set when the result is in; line
// says what came in, or what the wait is still waiting for.
type effectResult struct {
	in   bool
	line string
}

// evaluate decides whether the result of change is in on view. failing are
// the user tests of the alert set that fail now (scenario IDs); now is the
// time of this read.
func (w *effectWatch) evaluate(view sdk.DetectionContext, change knownChange, failing []string, now time.Time) effectResult {
	if w.doneAt == nil {
		w.doneAt = map[sdk.ObjectRef]time.Time{}
	}
	head := fmt.Sprintf("your change at %s (%s)", hms(change.At), strings.Join(change.Writes, ", "))
	settled := change.At.Add(time.Second) // managedFields times have one-second resolution
	var pending, failed, rolled []string
	for _, ref := range change.Workloads {
		why, failure := rolloutPending(view, ref, change.At)
		switch {
		case failure != "":
			failed = append(failed, fmt.Sprintf("%s/%s rollout failed: %s", ref.Kind, ref.Name, failure))
		case why != "":
			pending = append(pending, fmt.Sprintf("%s/%s rollout (%s)", ref.Kind, ref.Name, why))
		default:
			if w.doneAt[ref].IsZero() {
				w.doneAt[ref] = now
			}
			if w.doneAt[ref].After(settled) {
				settled = w.doneAt[ref]
			}
			rolled = append(rolled, fmt.Sprintf("%s/%s rolled out", ref.Kind, ref.Name))
		}
	}
	if len(failed) > 0 {
		return effectResult{in: true, line: "result of " + head + " is in: " + strings.Join(failed, "; ")}
	}
	if len(pending) > 0 {
		return effectResult{line: "waited for " + head + ": " + strings.Join(pending, "; ")}
	}
	samples, missing := freshSamples(view, failing, settled)
	if len(missing) > 0 {
		return effectResult{line: fmt.Sprintf("waited for %s: a sample of %s after %s", head, strings.Join(missing, ", "), hms(settled))}
	}
	parts := append(rolled, samples...)
	return effectResult{in: true, line: "result of " + head + " is in: " + strings.Join(parts, "; ")}
}

// freshSamples finds, for each failing test, its first scheduled sample at
// or after since; missing are the tests without one yet.
func freshSamples(view sdk.DetectionContext, failing []string, since time.Time) (found, missing []string) {
	set, _ := view.(traffic.WindowSet)
	for _, test := range failing {
		var first *traffic.Sample
		if set != nil {
			for _, window := range set.TrafficWindows() {
				for _, observed := range window.Scenarios {
					if observed.Scenario.ID != test {
						continue
					}
					for i := range observed.Samples {
						sample := observed.Samples[i]
						if !sample.At.Before(since) && (first == nil || sample.At.Before(first.At)) {
							first = &sample
						}
					}
				}
			}
		}
		if first == nil {
			missing = append(missing, test)
			continue
		}
		result := "PASS"
		if first.Outcome != traffic.OutcomeOK {
			result = "FAIL " + failureLabel(*first)
		}
		found = append(found, fmt.Sprintf("user test %s %s at %s", test, result, hms(first.At)))
	}
	return found, missing
}

// failingAlertTests are the alert set's user tests that fail in response.
func failingAlertTests(watch detectWatch) []string {
	var tests []string
	for _, label := range watch.active {
		if test, ok := strings.CutPrefix(label, "user test "); ok {
			tests = append(tests, test)
		}
	}
	sort.Strings(tests)
	return tests
}
