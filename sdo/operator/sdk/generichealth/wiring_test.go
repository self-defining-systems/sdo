package generichealth_test

import (
	apps "k8s.io/api/apps/v1"
	core "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sdo.dev/operator/sdk/sdktest"
	"time"
)

func ptr[T any](v T) *T { return &v }
func healthy() sdktest.Snapshot {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := meta.ObjectMeta{Name: "unit-x", Namespace: "tenant-z", CreationTimestamp: meta.NewTime(now.Add(-time.Hour)), Labels: map[string]string{"role": "api"}}
	spec := core.PodSpec{ServiceAccountName: "identity-x", Containers: []core.Container{{Name: "worker", Ports: []core.ContainerPort{{Name: "web", ContainerPort: 8080}}, EnvFrom: []core.EnvFromSource{{ConfigMapRef: &core.ConfigMapEnvSource{LocalObjectReference: core.LocalObjectReference{Name: "settings-x"}}}}}}}
	return sdktest.Snapshot{NamespaceName: m.Namespace, ObservedAtTime: &now, ServiceAccountList: []core.ServiceAccount{{ObjectMeta: meta.ObjectMeta{Name: "identity-x", Namespace: m.Namespace}}}, ConfigMapList: []core.ConfigMap{{ObjectMeta: meta.ObjectMeta{Name: "settings-x", Namespace: m.Namespace}, Data: map[string]string{"config": "yes"}}}, DeploymentList: []apps.Deployment{{ObjectMeta: m, Spec: apps.DeploymentSpec{Replicas: ptr(int32(1)), Template: core.PodTemplateSpec{ObjectMeta: m, Spec: spec}}, Status: apps.DeploymentStatus{UpdatedReplicas: 1, Replicas: 1}}}, PodList: []core.Pod{{ObjectMeta: meta.ObjectMeta{Name: "instance-x", Namespace: m.Namespace, CreationTimestamp: m.CreationTimestamp, Labels: m.Labels, OwnerReferences: []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: m.Name, Controller: ptr(true)}}}, Spec: spec}}, ServiceList: []core.Service{{ObjectMeta: meta.ObjectMeta{Name: "route-x", Namespace: m.Namespace}, Spec: core.ServiceSpec{Selector: m.Labels, Ports: []core.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080)}}}}}}
}

type wiringCase struct {
	name, rule     string
	change, excuse func(*sdktest.Snapshot)
}

func wiringCases() []wiringCase {
	return []wiringCase{
		{"selector", "service-selector-empty", func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.Selector = map[string]string{"wrong": "label"} }, func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.Selector = nil }},
		{"owners", "service-multiple-workloads", func(s *sdktest.Snapshot) {
			p := *s.PodList[0].DeepCopy()
			p.Name = "instance-y"
			p.OwnerReferences[0].Name = "unit-y"
			s.PodList = append(s.PodList, p)
		}, func(s *sdktest.Snapshot) { s.PodList[1].OwnerReferences[0].Name = "unit-x" }},
		{"numeric port", "service-target-port", func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(9090) }, func(s *sdktest.Snapshot) { s.DeploymentList[0].Generation = 2 }},
		{"named port", "service-target-port", func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromString("absent") }, func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromString("web") }},
		{"protocol", "service-target-port", func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.Ports[0].Protocol = core.ProtocolUDP }, func(s *sdktest.Snapshot) { s.PodList[0].Spec.Containers[0].Ports[0].Protocol = core.ProtocolUDP }},
		{"configmap", "template-missing-reference", func(s *sdktest.Snapshot) { s.ConfigMapList = nil }, func(s *sdktest.Snapshot) {
			s.DeploymentList[0].Spec.Template.Spec.Containers[0].EnvFrom[0].ConfigMapRef.Optional = ptr(true)
		}},
		{"configmap key", "template-missing-reference", func(s *sdktest.Snapshot) {
			s.DeploymentList[0].Spec.Template.Spec.Containers[0].Env = []core.EnvVar{{Name: "X", ValueFrom: &core.EnvVarSource{ConfigMapKeyRef: &core.ConfigMapKeySelector{LocalObjectReference: core.LocalObjectReference{Name: "settings-x"}, Key: "missing"}}}}
		}, func(s *sdktest.Snapshot) { s.ConfigMapList[0].Data["missing"] = "present" }},
		{"secret", "template-missing-reference", func(s *sdktest.Snapshot) {
			s.DeploymentList[0].Spec.Template.Spec.ImagePullSecrets = []core.LocalObjectReference{{Name: "registry-x"}}
		}, func(s *sdktest.Snapshot) { s.DeploymentList[0].Spec.Template.Spec.ImagePullSecrets = nil }},
		{"account", "template-missing-reference", func(s *sdktest.Snapshot) { s.ServiceAccountList = nil }, func(s *sdktest.Snapshot) { s.DeploymentList[0].Spec.Paused = false; s.DeploymentList[0].Generation = 2 }},
		{"claim missing", "template-missing-reference", func(s *sdktest.Snapshot) {
			s.DeploymentList[0].Spec.Template.Spec.Volumes = []core.Volume{{Name: "storage", VolumeSource: core.VolumeSource{PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{ClaimName: "claim-x"}}}}
		}, func(s *sdktest.Snapshot) {
			s.PVCList = []core.PersistentVolumeClaim{{ObjectMeta: meta.ObjectMeta{Name: "claim-x", Namespace: s.NamespaceName}, Status: core.PersistentVolumeClaimStatus{Phase: core.ClaimBound}}}
		}},
		{"claim binding", "pvc-not-bound", func(s *sdktest.Snapshot) {
			s.PVCList = []core.PersistentVolumeClaim{{ObjectMeta: s.DeploymentList[0].ObjectMeta, Status: core.PersistentVolumeClaimStatus{Phase: core.ClaimPending}}}
		}, func(s *sdktest.Snapshot) { s.PVCList[0].CreationTimestamp = meta.NewTime(*s.ObservedAtTime) }},
		{"alias", "externalname-missing-service", func(s *sdktest.Snapshot) {
			s.ServiceList[0].Spec.Type = core.ServiceTypeExternalName
			s.ServiceList[0].Spec.ExternalName = "absent.tenant-z.svc.cluster.local"
		}, func(s *sdktest.Snapshot) { s.ServiceList[0].Spec.ExternalName = "outside.example.org" }},
		{"ingress missing", "ingress-service-missing", func(s *sdktest.Snapshot) {
			s.IngressList = []netv1.Ingress{{ObjectMeta: s.DeploymentList[0].ObjectMeta, Spec: netv1.IngressSpec{DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "missing", Port: netv1.ServiceBackendPort{Number: 80}}}}}}
		}, func(s *sdktest.Snapshot) { s.IngressList[0].Spec.DefaultBackend.Service.Name = "route-x" }},
		{"ingress port", "ingress-service-port", func(s *sdktest.Snapshot) {
			s.IngressList = []netv1.Ingress{{ObjectMeta: s.DeploymentList[0].ObjectMeta, Spec: netv1.IngressSpec{DefaultBackend: &netv1.IngressBackend{Service: &netv1.IngressServiceBackend{Name: "route-x", Port: netv1.ServiceBackendPort{Number: 999}}}}}}
		}, func(s *sdktest.Snapshot) { s.IngressList[0].Spec.DefaultBackend.Service.Port.Number = 80 }},
		{"deny ingress", "network-deny-all-ingress", func(s *sdktest.Snapshot) {
			s.NetworkPolicyList = []netv1.NetworkPolicy{{ObjectMeta: s.DeploymentList[0].ObjectMeta, Spec: netv1.NetworkPolicySpec{PolicyTypes: []netv1.PolicyType{netv1.PolicyTypeIngress}}}}
		}, func(s *sdktest.Snapshot) {
			p := *s.NetworkPolicyList[0].DeepCopy()
			p.Name = "allow-x"
			p.Spec.Ingress = []netv1.NetworkPolicyIngressRule{{}}
			s.NetworkPolicyList = append(s.NetworkPolicyList, p)
		}},
		{"deny egress", "network-deny-all-egress", func(s *sdktest.Snapshot) {
			s.NetworkPolicyList = []netv1.NetworkPolicy{{ObjectMeta: s.DeploymentList[0].ObjectMeta, Spec: netv1.NetworkPolicySpec{PolicyTypes: []netv1.PolicyType{netv1.PolicyTypeEgress}}}}
		}, func(s *sdktest.Snapshot) { s.NetworkPolicyList[0].Spec.Egress = []netv1.NetworkPolicyEgressRule{{}} }},
	}
}
