package sdktest

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
)

const MaxCapturedSnapshotBytes = 4 << 20

var sensitiveEnvironmentName = regexp.MustCompile(`(?i)SECRET|TOKEN|PASSWORD|KEY`)

// CaptureSnapshot copies the namespace-local detection view for corpus replay.
func CaptureSnapshot(view sdk.DetectionContext, namespace string) (Snapshot, error) {
	if view == nil || namespace == "" || view.Namespace() != namespace {
		return Snapshot{}, errors.New("capture requires a matching application namespace")
	}
	s := Snapshot{NamespaceName: namespace}
	for _, object := range view.ConfigMaps() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.ConfigMapList = append(s.ConfigMapList, copy)
		}
	}
	for _, object := range view.Services() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.ServiceList = append(s.ServiceList, copy)
		}
	}
	for _, object := range view.Pods() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		compactMeta(&copy.ObjectMeta)
		redactPodSpec(&copy.Spec)
		s.PodList = append(s.PodList, copy)
	}
	for _, object := range view.Deployments() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		compactMeta(&copy.ObjectMeta)
		compactMeta(&copy.Spec.Template.ObjectMeta)
		redactPodSpec(&copy.Spec.Template.Spec)
		s.DeploymentList = append(s.DeploymentList, copy)
	}
	if extended, ok := view.(interface{ StatefulSets() []appsv1.StatefulSet }); ok {
		for _, object := range extended.StatefulSets() {
			if object.Namespace != namespace {
				continue
			}
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			compactMeta(&copy.Spec.Template.ObjectMeta)
			redactPodSpec(&copy.Spec.Template.Spec)
			s.StatefulSetList = append(s.StatefulSetList, copy)
		}
	}
	if extended, ok := view.(interface {
		HorizontalPodAutoscalers() []autoscalingv2.HorizontalPodAutoscaler
	}); ok {
		for _, object := range extended.HorizontalPodAutoscalers() {
			if object.Namespace == namespace {
				copy := *object.DeepCopy()
				compactMeta(&copy.ObjectMeta)
				s.HPAList = append(s.HPAList, copy)
			}
		}
	}
	if extended, ok := view.(interface {
		PodDisruptionBudgets() []policyv1.PodDisruptionBudget
	}); ok {
		for _, object := range extended.PodDisruptionBudgets() {
			if object.Namespace == namespace {
				copy := *object.DeepCopy()
				compactMeta(&copy.ObjectMeta)
				s.PDBList = append(s.PDBList, copy)
			}
		}
	}
	for _, object := range view.ReplicaSets() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		compactMeta(&copy.ObjectMeta)
		compactMeta(&copy.Spec.Template.ObjectMeta)
		redactPodSpec(&copy.Spec.Template.Spec)
		s.ReplicaSetList = append(s.ReplicaSetList, copy)
	}
	for _, object := range view.Endpoints() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.EndpointList = append(s.EndpointList, copy)
		}
	}
	for _, object := range view.EndpointSlices() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.EndpointSliceList = append(s.EndpointSliceList, copy)
		}
	}
	for _, object := range view.NetworkPolicies() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.NetworkPolicyList = append(s.NetworkPolicyList, copy)
		}
	}
	for _, object := range view.Events() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			copy.InvolvedObject.ResourceVersion = ""
			copy.InvolvedObject.UID = ""
			s.EventList = append(s.EventList, copy)
		}
	}
	data, err := json.Marshal(s)
	if err != nil {
		return Snapshot{}, fmt.Errorf("encode captured snapshot: %w", err)
	}
	if len(data) > MaxCapturedSnapshotBytes {
		return Snapshot{}, fmt.Errorf("captured snapshot exceeds %d bytes", MaxCapturedSnapshotBytes)
	}
	return s, nil
}

func compactMeta(meta *metav1.ObjectMeta) {
	meta.ManagedFields = nil
	meta.ResourceVersion = ""
	meta.UID = ""
	meta.Generation = 0
	delete(meta.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
}

func redactPodSpec(spec *corev1.PodSpec) {
	for i := range spec.InitContainers {
		redactContainer(&spec.InitContainers[i])
	}
	for i := range spec.Containers {
		redactContainer(&spec.Containers[i])
	}
	for i := range spec.EphemeralContainers {
		for j := range spec.EphemeralContainers[i].Env {
			if sensitiveEnvironmentName.MatchString(spec.EphemeralContainers[i].Env[j].Name) {
				spec.EphemeralContainers[i].Env[j].Value = ""
				spec.EphemeralContainers[i].Env[j].ValueFrom = nil
			}
		}
	}
}

func redactContainer(container *corev1.Container) {
	for i := range container.Env {
		if sensitiveEnvironmentName.MatchString(container.Env[i].Name) {
			container.Env[i].Value = ""
			container.Env[i].ValueFrom = nil
		}
	}
}
