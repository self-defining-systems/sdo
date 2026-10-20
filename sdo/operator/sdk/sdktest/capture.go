package sdktest

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	corev1 "k8s.io/api/core/v1"
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
			s.ConfigMapList = append(s.ConfigMapList, object)
		}
	}
	for _, object := range view.Services() {
		if object.Namespace == namespace {
			s.ServiceList = append(s.ServiceList, object)
		}
	}
	for _, object := range view.Pods() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		redactPodSpec(&copy.Spec)
		s.PodList = append(s.PodList, copy)
	}
	for _, object := range view.Deployments() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		redactPodSpec(&copy.Spec.Template.Spec)
		s.DeploymentList = append(s.DeploymentList, copy)
	}
	for _, object := range view.ReplicaSets() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		redactPodSpec(&copy.Spec.Template.Spec)
		s.ReplicaSetList = append(s.ReplicaSetList, copy)
	}
	for _, object := range view.Endpoints() {
		if object.Namespace == namespace {
			s.EndpointList = append(s.EndpointList, object)
		}
	}
	for _, object := range view.EndpointSlices() {
		if object.Namespace == namespace {
			s.EndpointSliceList = append(s.EndpointSliceList, object)
		}
	}
	for _, object := range view.NetworkPolicies() {
		if object.Namespace == namespace {
			s.NetworkPolicyList = append(s.NetworkPolicyList, object)
		}
	}
	for _, object := range view.Events() {
		if object.Namespace == namespace {
			s.EventList = append(s.EventList, object)
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
