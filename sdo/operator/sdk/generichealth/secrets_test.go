package generichealth

import (
	"context"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

var secretNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(minutes int) meta.Time {
	return meta.NewTime(secretNow.Add(time.Duration(minutes) * time.Minute))
}

// rotatedSecret is a Secret created long ago whose data a manager rewrote at
// changed, while another manager later touched only its labels.
func rotatedSecret(changed meta.Time, value string) core.Secret {
	later := at(-1)
	return core.Secret{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: "shared-token", CreationTimestamp: at(-600), ManagedFields: []meta.ManagedFieldsEntry{
			{Manager: "creator", Operation: meta.ManagedFieldsOperationUpdate, Time: ptr(at(-600)), FieldsV1: &meta.FieldsV1{Raw: []byte(`{"f:data":{".":{},"f:token":{}},"f:type":{}}`)}},
			{Manager: "rotator", Operation: meta.ManagedFieldsOperationUpdate, Time: &changed, FieldsV1: &meta.FieldsV1{Raw: []byte(`{"f:data":{"f:token":{}}}`)}},
			{Manager: "labeler", Operation: meta.ManagedFieldsOperationUpdate, Time: &later, FieldsV1: &meta.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{"f:tier":{}}}}`)}},
		}},
		Data: map[string][]byte{"token": []byte(value)},
	}
}

func ptr[T any](v T) *T { return &v }

func readerPod(name, owner string, started meta.Time, env bool) core.Pod {
	spec := core.PodSpec{Containers: []core.Container{{Name: "main"}}, ImagePullSecrets: []core.LocalObjectReference{{Name: "pull"}}}
	if env {
		spec.Containers[0].Env = []core.EnvVar{{Name: "TOKEN", ValueFrom: &core.EnvVarSource{SecretKeyRef: &core.SecretKeySelector{LocalObjectReference: core.LocalObjectReference{Name: "shared-token"}, Key: "token"}}}}
	} else {
		spec.Volumes = []core.Volume{{Name: "keys", VolumeSource: core.VolumeSource{Projected: &core.ProjectedVolumeSource{Sources: []core.VolumeProjection{{Secret: &core.SecretProjection{LocalObjectReference: core.LocalObjectReference{Name: "shared-token"}}}}}}}}
	}
	return core.Pod{
		ObjectMeta: meta.ObjectMeta{Namespace: "ns", Name: name, OwnerReferences: []meta.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: owner, Controller: ptr(true)}}},
		Spec:       spec,
		Status: core.PodStatus{Phase: core.PodRunning, StartTime: &started, ContainerStatuses: []core.ContainerStatus{{
			Name: "main", State: core.ContainerState{Running: &core.ContainerStateRunning{StartedAt: started}},
		}}},
	}
}

func probeWindow(failing bool) map[string]traffic.Window {
	outcome := traffic.OutcomeOK
	if failing {
		outcome = traffic.OutcomeError
	}
	var samples []traffic.Sample
	for i := 5; i > 0; i-- {
		samples = append(samples, traffic.Sample{At: secretNow.Add(-time.Duration(i) * time.Second), Outcome: outcome, Status: 500})
	}
	return map[string]traffic.Window{"probe": {
		Workload:   traffic.Workload{Name: "probe", Purpose: traffic.PurposeHealthProbe, Scenarios: []traffic.WorkloadScenario{{ID: "sign-in"}}},
		ObservedAt: secretNow,
		Scenarios:  []traffic.ScenarioObservations{{Scenario: traffic.Descriptor{ID: "sign-in"}, Samples: samples, Qualified: true}},
	}}
}

func secretSnapshot(secret core.Secret, userFailing bool, pods ...core.Pod) sdktest.Snapshot {
	now := secretNow
	return sdktest.Snapshot{
		NamespaceName:  "ns",
		ObservedAtTime: &now,
		SecretList:     []sdk.SecretMetadata{sdk.SecretMetadataFrom(secret)},
		PodList:        pods,
		Traffic:        probeWindow(userFailing),
	}
}

func secretFindings(t *testing.T, s sdktest.Snapshot) []sdk.Finding {
	t.Helper()
	findings, err := NewWiring(sdk.DetectorSpec{ID: "cluster-wiring"}).Detect(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	var out []sdk.Finding
	for _, f := range findings {
		if f.RuleID == RuleSecretReaderSkew || f.RuleID == RuleSecretReadersPredateChange {
			out = append(out, f)
		}
	}
	return out
}

func TestSecretReapplyCannotDiagnoseReaderSkew(t *testing.T) {
	secret := rotatedSecret(at(-10), "same-value")
	pods := []core.Pod{
		readerPod("new", "web", at(-5), true),
		readerPod("old", "worker", at(-90), true),
	}
	if got := secretFindings(t, secretSnapshot(secret, true, pods...)); len(got) != 0 {
		t.Fatalf("metadata-only reapply diagnosed reader skew: %+v", got)
	}
}
