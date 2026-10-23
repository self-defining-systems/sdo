package observe_test

import (
	"context"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/observe"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestCandidateDetector(t *testing.T) {
	meta := metav1.ObjectMeta{Namespace: "sample", Name: "backend"}
	deployment := appsv1.Deployment{ObjectMeta: meta}
	deployment.Spec.Template.Spec.Containers = []corev1.Container{{Name: "worker", Env: []corev1.EnvVar{{Name: "SETTING", ValueFrom: &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "required"}}}}}}
	snapshot := sdktest.Snapshot{NamespaceName: "sample", DeploymentList: []appsv1.Deployment{deployment}, ServiceList: []corev1.Service{{ObjectMeta: meta, Spec: corev1.ServiceSpec{Selector: map[string]string{"role": "backend"}}}}, NetworkPolicyList: []networkingv1.NetworkPolicy{{ObjectMeta: meta}}}
	spec := sdk.DetectorSpec{ID: "candidate", Playbooks: []string{"investigate"}, Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder}
	for _, kind := range []string{"Deployment", "Service", "ConfigMap", "NetworkPolicy"} {
		t.Run(kind, func(t *testing.T) {
			detector := observe.NewDetector(spec, kind)
			if !reflect.DeepEqual(detector.Spec(), spec) {
				t.Fatal("caller policy changed")
			}
			findings, err := detector.Detect(context.Background(), snapshot)
			if err != nil || len(findings) != 1 {
				t.Fatalf("findings=%+v err=%v", findings, err)
			}
			f := findings[0]
			if f.PrimaryResource.Kind != kind || f.DetectorID != spec.ID || !reflect.DeepEqual(f.Playbooks, spec.Playbooks) || f.ParameterBindings["target"] != f.PrimaryResource {
				t.Fatalf("candidate contract: %+v", f)
			}
		})
	}
	snapshot.ConfigMapList = []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Namespace: "sample", Name: "settings"}, BinaryData: map[string][]byte{"required": {}}}}
	findings, _ := observe.NewDetector(spec, "ConfigMap").Detect(context.Background(), snapshot)
	if len(findings) != 0 {
		t.Fatalf("existing binary key reported missing: %+v", findings)
	}
	snapshot.ConfigMapList = nil
	optional := true
	snapshot.DeploymentList[0].Spec.Template.Spec.Containers[0].Env[0].ValueFrom.ConfigMapKeyRef.Optional = &optional
	findings, _ = observe.NewDetector(spec, "ConfigMap").Detect(context.Background(), snapshot)
	if len(findings) != 0 {
		t.Fatal("optional reference reported missing")
	}
	snapshot.ServiceList[0].Spec.Type = corev1.ServiceTypeExternalName
	findings, _ = observe.NewDetector(spec, "Service").Detect(context.Background(), snapshot)
	if len(findings) != 0 {
		t.Fatal("DNS alias reported missing endpoints")
	}
	findings, _ = observe.NewDetector(spec, "Unsupported").Detect(context.Background(), snapshot)
	if len(findings) != 0 {
		t.Fatal("unsupported kind produced candidates")
	}
}
