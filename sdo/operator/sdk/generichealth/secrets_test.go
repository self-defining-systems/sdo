package generichealth

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

var secretNow = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(minutes int) meta.Time { return meta.NewTime(secretNow.Add(time.Duration(minutes) * time.Minute)) }

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

// Readers that started on both sides of a Secret's last data change may hold
// different versions of it. The finding names the Secret, the stale readers
// and the fresh readers, and fires only while a user-facing probe fails.
func TestSecretReaderSkewNamesStaleAndFreshReaders(t *testing.T) {
	secret := rotatedSecret(at(-10), "v2")
	pods := []core.Pod{
		readerPod("store-0", "store", at(-60), false),
		readerPod("web-0", "web", at(-5), true),
		readerPod("worker-0", "worker", at(-90), true),
	}
	if got := secretFindings(t, secretSnapshot(secret, false, pods...)); len(got) != 0 {
		t.Fatalf("no user-facing failure must suppress the rule, got %+v", got)
	}
	got := secretFindings(t, secretSnapshot(secret, true, pods...))
	if len(got) != 1 || got[0].RuleID != RuleSecretReaderSkew {
		t.Fatalf("findings = %+v, want one %s", got, RuleSecretReaderSkew)
	}
	f := got[0]
	if f.PrimaryResource != (sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "ns", Name: "shared-token"}) {
		t.Fatalf("primary = %+v", f.PrimaryResource)
	}
	stale, fresh, _ := strings.Cut(strings.SplitN(f.Evidence, "stale readers (started before): ", 2)[1], "fresh readers (started after): ")
	for _, want := range []string{"StatefulSet/store pod=store-0", "via=volume", "StatefulSet/worker pod=worker-0", "via=env"} {
		if !strings.Contains(stale, want) {
			t.Errorf("stale readers %q lack %q", stale, want)
		}
	}
	if !strings.Contains(fresh, "StatefulSet/web pod=web-0") || strings.Contains(fresh, "store-0") {
		t.Errorf("fresh readers = %q", fresh)
	}
	if !strings.Contains(f.Evidence, at(-10).UTC().Format(time.RFC3339)) {
		t.Errorf("evidence %q lacks the data change time (the label-only manager must not move it)", f.Evidence)
	}
	if len(f.RelatedResources) != 2 || f.RelatedResources[0].Name != "store" || f.RelatedResources[1].Name != "worker" {
		t.Errorf("related = %+v, want the stale reader workloads", f.RelatedResources)
	}
}

func TestSecretReadersPredateChangeAndFreshReadersAreQuiet(t *testing.T) {
	secret := rotatedSecret(at(-10), "v2")
	got := secretFindings(t, secretSnapshot(secret, true, readerPod("web-0", "web", at(-30), true), readerPod("worker-0", "worker", at(-40), false)))
	if len(got) != 1 || got[0].RuleID != RuleSecretReadersPredateChange {
		t.Fatalf("findings = %+v, want one %s", got, RuleSecretReadersPredateChange)
	}
	if got := secretFindings(t, secretSnapshot(secret, true, readerPod("web-0", "web", at(-5), true))); len(got) != 0 {
		t.Fatalf("readers started after the change must not fire, got %+v", got)
	}
}

// The rule is value-blind by construction: detectors receive SecretMetadata,
// which has no field that can hold a value, and two Secrets that differ only
// in data yield identical metadata and identical findings.
func TestSecretReaderRulesNeverConsultSecretData(t *testing.T) {
	typ := reflect.TypeOf(sdk.SecretMetadata{})
	for i := range typ.NumField() {
		field := typ.Field(i)
		if field.Type == reflect.TypeOf([]byte(nil)) || field.Type == reflect.TypeOf(map[string][]byte(nil)) || field.Type == reflect.TypeOf(map[string]string(nil)) && field.Name != "Labels" {
			t.Fatalf("SecretMetadata.%s could carry Secret values", field.Name)
		}
	}
	a, b := rotatedSecret(at(-10), "old-value"), rotatedSecret(at(-10), "new-value")
	b.StringData = map[string]string{"token": "other"}
	if !reflect.DeepEqual(sdk.SecretMetadataFrom(a), sdk.SecretMetadataFrom(b)) {
		t.Fatalf("metadata depends on values: %+v vs %+v", sdk.SecretMetadataFrom(a), sdk.SecretMetadataFrom(b))
	}
	pods := []core.Pod{readerPod("web-0", "web", at(-5), true), readerPod("worker-0", "worker", at(-90), true)}
	fa, fb := secretFindings(t, secretSnapshot(a, true, pods...)), secretFindings(t, secretSnapshot(b, true, pods...))
	if len(fa) != 1 || !reflect.DeepEqual(fa, fb) {
		t.Fatalf("findings depend on Secret values: %+v vs %+v", fa, fb)
	}
	for _, f := range fa {
		if strings.Contains(f.Evidence, "old-value") || strings.Contains(f.Evidence, "new-value") {
			t.Fatalf("evidence leaks a value: %q", f.Evidence)
		}
	}
}
