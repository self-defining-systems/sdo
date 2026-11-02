//go:build integration

package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestCompiledMitigatorBindsWebhookReferencesBeforePlanValidation(t *testing.T) {
	body := `target:=program.WebhookCA{};target.Ref.APIVersion="admissionregistration.k8s.io/v1";target.Ref.Kind="MutatingWebhookConfiguration";target.Ref.Name="pod-policy";target.Webhook="pod-policy.validation.k8s.io";return program.Plan{Actions:[]program.Action{program.SyncWebhookCA{Target:target,Reason:"synthetic relation-bound CA repair"}}},nil`
	p := compiledTerminalPlanProgram(t, body)
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), Client: admissionCluster()}
	bindings := program.Bindings{Objects: program.ObjectBindings{"service": {APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
	plan, err := BuildMitigationPlan(context.Background(), p, view, bindings, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got := plan.Actions[0].(program.SyncWebhookCA).Target
	if got.SecretNamespace != "platform" || got.SecretName != "pod-policy-tls" || got.SecretKey != "tls.crt" {
		t.Fatalf("compiled relation was not bound: %+v", got)
	}
}

