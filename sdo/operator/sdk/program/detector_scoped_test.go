package program_test

import (
	"context"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
)

func TestProgramDetectorPreservesScopedWebhookSnapshot(t *testing.T) {
	target := sdk.ObservationTarget{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "policy.example"}
	snapshot := sdktest.Snapshot{NamespaceName: "demo", ScopedSnapshot: sdk.ScopedSnapshot{Version: 1, Generation: "test", Scope: sdk.ObservationScope{Version: 1, Targets: []sdk.ObservationTarget{target}}, Statuses: []sdk.ObservationStatus{{Target: target, State: sdk.ObservationObserved}}, Mutating: []admissionv1.MutatingWebhookConfiguration{{ObjectMeta: metav1.ObjectMeta{Name: target.Name}, Webhooks: []admissionv1.MutatingWebhook{{Name: "admit.example"}}}}}}
	p := diagnostic{fn: func(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
		object, err := sdk.ScopedSource(v).MutatingWebhookConfiguration(target.Name)
		if err != nil {
			return program.Verdict{}, err
		}
		if object != nil && len(object.Webhooks) == 1 && object.Webhooks[0].Name == "admit.example" {
			return program.Verdict{Result: program.Match}, nil
		}
		return program.Verdict{Result: program.NoMatch}, nil
	}}
	locate := func(sdk.DetectionContext) []program.Bindings {
		return []program.Bindings{{Objects: program.ObjectBindings{"service": serviceRef}}}
	}
	d := program.DetectorFromProgram(p, locate, sdk.DetectorSpec{ID: "incident-webhook", Playbooks: []string{"webhook"}})
	findings, err := d.Detect(context.Background(), snapshot)
	if err != nil || len(findings) != 1 {
		t.Fatalf("scoped snapshot lost: findings=%v error=%v", findings, err)
	}
	findings, err = d.Detect(context.Background(), sdktest.Snapshot{NamespaceName: "demo"})
	if err != nil || len(findings) != 0 {
		t.Fatalf("unconfigured scope acquired evidence: %v %v", findings, err)
	}
}
