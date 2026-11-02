package runtime

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// flagSourceView compares the live ConfigMap value with accepted source. A
// projected capture keeps key names only, so comparing it reads as drift.
type flagSourceView struct {
	program.ReadView
	snapshot sdk.DetectionContext
}

func (v flagSourceView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	for _, cm := range v.snapshot.ConfigMaps() {
		if cm.Name == ref.Name {
			return program.Diff{Changed: cm.Data["flag"] != "off"}, nil
		}
	}
	return program.Diff{Changed: true}, nil
}

func (v flagSourceView) AdmissionWebhooksFor(sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	ready := 0
	if v.flag() == "off" {
		ready = 1
	}
	return []program.AdmissionWebhook{{Webhook: "policy.example", ReadyEndpoints: ready}}, nil
}

func (v flagSourceView) flag() string {
	for _, cm := range v.snapshot.ConfigMaps() {
		return cm.Data["flag"]
	}
	return ""
}

func flagSnapshot(value string) sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "demo", ConfigMapList: []corev1.ConfigMap{{ObjectMeta: metav1.ObjectMeta{Name: "flags", Namespace: "demo"}, Data: map[string]string{"flag": value}}}}
}
