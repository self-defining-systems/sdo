package runtime

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// A ConfigMap a container reads through a subPath mount, an env valueFrom or
// an envFrom reaches it only when the container starts: Kubernetes never
// refreshes those, so restoring the object does not restore what the running
// container uses. reloadsOnlyAtStart and staleConsumers own that fact for every
// reader of source departures (`sdo detect`, the built-in restore and its
// post-apply Diagnose).

// reloadsOnlyAtStart reports whether a container of template reads the
// ConfigMap name in a way that takes effect only at container start.
func reloadsOnlyAtStart(template corev1.PodTemplateSpec, name string) bool {
	volumes := configMapVolumes(template, name)
	containers := append(slices.Clone(template.Spec.InitContainers), template.Spec.Containers...)
	for _, container := range containers {
		if containerReadsAtStart(container, name, volumes) {
			return true
		}
	}
	return false
}

// configMapVolumes are the names of template's volumes that carry the ConfigMap name.
func configMapVolumes(template corev1.PodTemplateSpec, name string) map[string]bool {
	volumes := map[string]bool{}
	for _, volume := range template.Spec.Volumes {
		if volume.ConfigMap != nil && volume.ConfigMap.Name == name {
			volumes[volume.Name] = true
		}
		if volume.Projected != nil {
			for _, source := range volume.Projected.Sources {
				if source.ConfigMap != nil && source.ConfigMap.Name == name {
					volumes[volume.Name] = true
				}
			}
		}
	}
	return volumes
}

func containerReadsAtStart(container corev1.Container, name string, volumes map[string]bool) bool {
	for _, env := range container.Env {
		if env.ValueFrom != nil && env.ValueFrom.ConfigMapKeyRef != nil && env.ValueFrom.ConfigMapKeyRef.Name == name {
			return true
		}
	}
	for _, from := range container.EnvFrom {
		if from.ConfigMapRef != nil && from.ConfigMapRef.Name == name {
			return true
		}
	}
	for _, mount := range container.VolumeMounts {
		if volumes[mount.Name] && (mount.SubPath != "" || mount.SubPathExpr != "") {
			return true
		}
	}
	return false
}

// startWorkload is one workload whose template reads a ConfigMap only at start.
type startWorkload struct {
	ref      sdk.ObjectRef
	template corev1.PodTemplateSpec
	selector *metav1.LabelSelector
}

func startReaders(view sdk.DetectionContext, configMap sdk.ObjectRef) []startWorkload {
	namespace := configMap.Namespace
	if namespace == "" {
		namespace = view.Namespace()
	}
	ref := func(kind, name string) sdk.ObjectRef {
		return sdk.ObjectRef{APIVersion: "apps/v1", Kind: kind, Namespace: namespace, Name: name}
	}
	var result []startWorkload
	add := func(kind string, meta metav1.ObjectMeta, template corev1.PodTemplateSpec, selector *metav1.LabelSelector) {
		if (meta.Namespace == "" || meta.Namespace == namespace) && reloadsOnlyAtStart(template, configMap.Name) {
			result = append(result, startWorkload{ref: ref(kind, meta.Name), template: template, selector: selector})
		}
	}
	for _, deployment := range view.Deployments() {
		add("Deployment", deployment.ObjectMeta, deployment.Spec.Template, deployment.Spec.Selector)
	}
	for _, statefulSet := range view.StatefulSets() {
		add("StatefulSet", statefulSet.ObjectMeta, statefulSet.Spec.Template, statefulSet.Spec.Selector)
	}
	for _, daemonSet := range view.DaemonSets() {
		add("DaemonSet", daemonSet.ObjectMeta, daemonSet.Spec.Template, daemonSet.Spec.Selector)
	}
	return result
}

// staleConsumers are the workloads reading the ConfigMap only at start that
// have a running Pod whose current container instance started at or after
// changedAt (so it loaded the changed content) and before restoredAt (zero:
// not restored yet). Terminating Pods are already being replaced.
func staleConsumers(view sdk.DetectionContext, configMap sdk.ObjectRef, changedAt, restoredAt time.Time) []sdk.ObjectRef {
	changedAt = changedAt.Truncate(time.Second)
	var result []sdk.ObjectRef
	for _, workload := range startReaders(view, configMap) {
		selector := labels.SelectorFromSet(workload.template.Labels)
		if workload.selector != nil {
			if parsed, err := metav1.LabelSelectorAsSelector(workload.selector); err == nil && !parsed.Empty() {
				selector = parsed
			}
		}
		for _, pod := range view.Pods() {
			if pod.DeletionTimestamp != nil || pod.Namespace != "" && pod.Namespace != workload.ref.Namespace || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			started := podContentLoadedAt(pod, workload.template, configMap.Name)
			if started.IsZero() || started.Before(changedAt) || !restoredAt.IsZero() && !started.Before(restoredAt) {
				continue
			}
			result = append(result, workload.ref)
			break
		}
	}
	return result
}

// podContentLoadedAt is the latest start of a container of pod that reads the
// ConfigMap only at start: its running instance's start, else the Pod's.
func podContentLoadedAt(pod corev1.Pod, template corev1.PodTemplateSpec, name string) time.Time {
	volumes := configMapVolumes(template, name)
	podStart := pod.CreationTimestamp.Time
	if pod.Status.StartTime != nil {
		podStart = pod.Status.StartTime.Time
	}
	var latest time.Time
	statuses := append(slices.Clone(pod.Status.InitContainerStatuses), pod.Status.ContainerStatuses...)
	for _, container := range append(slices.Clone(template.Spec.InitContainers), template.Spec.Containers...) {
		if !containerReadsAtStart(container, name, volumes) {
			continue
		}
		at := podStart
		for _, status := range statuses {
			if status.Name == container.Name && status.State.Running != nil && status.State.Running.StartedAt.After(at) {
				at = status.State.Running.StartedAt.Time
			}
		}
		if at.After(latest) {
			latest = at
		}
	}
	return latest
}

// dataPaths are the content fields (data.KEY, binaryData.KEY) a drift event changed.
func dataPaths(event *sdk.DriftEvent) []sdk.DriftField {
	var result []sdk.DriftField
	for _, field := range event.Fields {
		if strings.HasPrefix(field.Path, "data.") || strings.HasPrefix(field.Path, "binaryData.") {
			result = append(result, field)
		}
	}
	return result
}

// contentRestoredAt is when the ConfigMap's content stopped being what the
// foreign change wrote: zero when no data key changed, when the live content
// still equals what that change wrote (its value was adopted, so Pods started
// since run it), or when no field manager owns a changed key. Otherwise it is
// the latest write time among the managers that own a changed data key, so a
// later write that touched no data key (a label) is not a restore.
func contentRestoredAt(object *unstructured.Unstructured, event *sdk.DriftEvent) time.Time {
	fields := dataPaths(event)
	if len(fields) == 0 {
		return time.Time{}
	}
	adopted := true
	for _, field := range fields {
		section, key, _ := strings.Cut(field.Path, ".")
		value, found, _ := unstructured.NestedFieldNoCopy(object.Object, section, key)
		live := ""
		if found {
			live = renderDriftValue(field.Path, value)
		}
		if live != field.New {
			adopted = false
		}
	}
	if adopted {
		return time.Time{}
	}
	var at time.Time
	for _, entry := range object.GetManagedFields() {
		if entry.Time == nil || entry.Subresource == "status" || entry.FieldsV1 == nil || !entry.Time.Time.After(at) {
			continue
		}
		var owned map[string]map[string]any
		if json.Unmarshal(entry.FieldsV1.Raw, &owned) != nil {
			continue
		}
		for _, field := range fields {
			section, key, _ := strings.Cut(field.Path, ".")
			if _, ok := owned["f:"+section]["f:"+key]; ok {
				at = entry.Time.Time
				break
			}
		}
	}
	return at
}

// splitDataStores separates the workloads of data stores (holds_data or a
// data role in the application map) from the others.
func splitDataStores(view sdk.DetectionContext, refs []sdk.ObjectRef) (others, data []sdk.ObjectRef) {
	var m program.AppMap
	if source, ok := view.(interface {
		AppMap() (program.AppMap, error)
	}); ok {
		m, _ = source.AppMap()
	}
	for _, ref := range refs {
		if holdsData(m, ref.Kind+"/"+ref.Name) {
			data = append(data, ref)
		} else {
			others = append(others, ref)
		}
	}
	return others, data
}

// latestForeignDriftEvent is the latest drift event for ref written by a
// manager other than SDO.
func latestForeignDriftEvent(events []sdk.DriftEvent, ref sdk.ObjectRef) *sdk.DriftEvent {
	var best *sdk.DriftEvent
	for index := range events {
		event := &events[index]
		if event.Ref.Kind != ref.Kind || event.Ref.Name != ref.Name || sdk.IsSDOFieldManager(event.Manager) {
			continue
		}
		if best == nil || driftEventTime(event).After(driftEventTime(best)) {
			best = event
		}
	}
	return best
}
