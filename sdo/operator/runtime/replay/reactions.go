package replay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// finish models only controller-owned replacement following a successful
// DeletePod or a workload template change. Template bytes come from the observed owner,
// never Fixed. Running/Ready is an explicitly reported simulation assumption.
func (w *reaction) finish(ctx context.Context) error {
	for index, original := range w.plan.Actions {
		if w.done == nil {
			w.done = map[int]bool{}
		}
		if w.done[index] {
			continue
		}
		switch a := concrete(original).(type) {
		case program.DeletePod:
			old := w.cluster.initial[a.Ref]
			if old == nil {
				return fmt.Errorf("pod recreation lacks pre-image")
			}
			// A mixed plan may invoke verification between actions. Do not
			// synthesize a replacement before the production delete lands.
			if live, err := w.cluster.Get(ctx, a.Ref); err == nil && live.GetUID() == old.GetUID() {
				continue
			}
			template, err := w.ownerTemplate(ctx, old)
			if err != nil {
				return err
			}
			if err = w.replace(ctx, a.Ref, old, template, false); err != nil {
				return err
			}
		default:
			ref, ok := workloadRef(original)
			if !ok {
				break
			}
			owner, err := w.cluster.Get(ctx, ref)
			if err != nil {
				return err
			}
			template, found, err := unstructured.NestedMap(owner.Object, "spec", "template")
			if err != nil || !found {
				return fmt.Errorf("rollout template unavailable")
			}
			before := w.cluster.initial[ref]
			if before == nil {
				return fmt.Errorf("rollout pre-image unavailable")
			}
			previous, _, _ := unstructured.NestedMap(before.Object, "spec", "template")
			left, _ := json.Marshal(previous)
			right, _ := json.Marshal(template)
			if bytes.Equal(left, right) {
				continue
			}
			for podRef, old := range w.cluster.initial {
				if podRef.Kind != "Pod" || podRef.Namespace != ref.Namespace || w.cluster.simDeleted[podRef] {
					continue
				}
				owned, err := w.ownedBy(ctx, old, ref)
				if err != nil {
					return err
				}
				if owned {
					if err = w.replace(ctx, podRef, old, template, true); err != nil {
						return err
					}
				}
			}
		}
		w.done[index] = true
	}
	return nil
}

func workloadRef(action program.Action) (sdk.ObjectRef, bool) {
	var ref sdk.ObjectRef
	switch a := action.(type) {
	case program.RestartRollout:
		ref = a.Ref
	case program.SetField:
		ref = a.Ref
	case program.ReconcileFromSource:
		ref = a.Ref
	case program.Rollback:
		ref = a.Ref
	case *program.RestartRollout:
		if a != nil {
			return workloadRef(*a)
		}
	case *program.SetField:
		if a != nil {
			return workloadRef(*a)
		}
	case *program.ReconcileFromSource:
		if a != nil {
			return workloadRef(*a)
		}
	case *program.Rollback:
		if a != nil {
			return workloadRef(*a)
		}
	}
	return ref, ref.APIVersion == "apps/v1" && (ref.Kind == "Deployment" || ref.Kind == "StatefulSet")
}
func concrete(a program.Action) program.Action {
	switch v := a.(type) {
	case *program.DeletePod:
		if v != nil {
			return *v
		}
	case *program.RestartRollout:
		if v != nil {
			return *v
		}
	}
	return a
}
func (w *reaction) ownerTemplate(ctx context.Context, pod *unstructured.Unstructured) (map[string]any, error) {
	for _, owner := range pod.GetOwnerReferences() {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.APIVersion != "apps/v1" || (owner.Kind != "ReplicaSet" && owner.Kind != "StatefulSet") {
			return nil, fmt.Errorf("pod owner kind has no offline recreation model")
		}
		ref := sdk.ObjectRef{APIVersion: owner.APIVersion, Kind: owner.Kind, Namespace: pod.GetNamespace(), Name: owner.Name}
		o, err := w.ownerObject(ctx, ref)
		if err != nil {
			return nil, err
		}
		template, found, err := unstructured.NestedMap(o.Object, "spec", "template")
		if err != nil || !found {
			return nil, fmt.Errorf("pod owner template unavailable")
		}
		return template, nil
	}
	return nil, fmt.Errorf("standalone pod has no controller recreation model")
}
func (w *reaction) ownerObject(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if ref.Kind == "ReplicaSet" {
		o, err := w.cluster.fake.AppsV1().ReplicaSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(o)
		if err != nil {
			return nil, err
		}
		return &unstructured.Unstructured{Object: fields}, nil
	}
	return w.cluster.Get(ctx, ref)
}
func (w *reaction) ownedBy(ctx context.Context, pod *unstructured.Unstructured, target sdk.ObjectRef) (bool, error) {
	for _, owner := range pod.GetOwnerReferences() {
		if owner.Controller == nil || !*owner.Controller {
			continue
		}
		if owner.Kind == target.Kind && owner.Name == target.Name {
			return true, nil
		}
		if owner.Kind == "ReplicaSet" {
			ref := sdk.ObjectRef{APIVersion: owner.APIVersion, Kind: owner.Kind, Namespace: pod.GetNamespace(), Name: owner.Name}
			rs, err := w.ownerObject(ctx, ref)
			if err != nil {
				return false, err
			}
			for _, parent := range rs.GetOwnerReferences() {
				if parent.Controller != nil && *parent.Controller && parent.Kind == target.Kind && parent.Name == target.Name {
					return true, nil
				}
			}
		}
	}
	return false, nil
}
func (w *reaction) replace(ctx context.Context, ref sdk.ObjectRef, old *unstructured.Unstructured, template map[string]any, remove bool) error {
	pod := new(corev1.Pod)
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(template, pod); err != nil {
		return err
	}
	pod.Name = old.GetName() + "-replay"
	for _, owner := range old.GetOwnerReferences() {
		if owner.Kind == "StatefulSet" {
			pod.Name = old.GetName()
		}
	}
	pod.Namespace = old.GetNamespace()
	pod.OwnerReferences = old.GetOwnerReferences()
	pod.ResourceVersion = ""
	pod.UID = ""
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}, {Type: corev1.ContainersReady, Status: corev1.ConditionTrue}, {Type: corev1.PodInitialized, Status: corev1.ConditionTrue}}
	for _, container := range pod.Spec.Containers {
		pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: container.Name, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	}
	if remove {
		if err := w.cluster.Delete(ctx, ref, old.GetUID(), old.GetResourceVersion()); err != nil {
			return err
		}
	}
	created, err := w.cluster.fake.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	fields, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(created)
	if err != nil {
		return err
	}
	o := &unstructured.Unstructured{Object: fields}
	o.SetAPIVersion("v1")
	o.SetKind("Pod")
	if w.cluster.simCreated == nil {
		w.cluster.simCreated = map[sdk.ObjectRef]*unstructured.Unstructured{}
		w.cluster.simDeleted = map[sdk.ObjectRef]bool{}
	}
	w.cluster.simCreated[objectRef(o)] = o
	w.cluster.simDeleted[ref] = true
	*w.effects = append(*w.effects, "simulated:pod-recreation:"+ref.Name, "simulated:pod-ready:"+pod.Name)
	return nil
}
