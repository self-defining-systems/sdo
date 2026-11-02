package program_test

import (
	"context"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"testing"
	"time"
)

type ignoredWebhookCARead struct{ target program.WebhookCA }

func (p ignoredWebhookCARead) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "ignored-ca"}
}
func (p ignoredWebhookCARead) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	_, _ = v.WebhookCAMatchesSecret(p.target)
	return program.Verdict{Result: program.NoMatch}, nil
}
func TestWebhookCAUnavailableCannotClearDiagnosis(t *testing.T) {
	target := program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "policy"}, Webhook: "policy.example", SecretNamespace: "other", SecretName: "serving", SecretKey: "ca.crt"}
	verdict := program.RunDiagnose(context.Background(), ignoredWebhookCARead{target}, program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), program.Bindings{}, time.Second)
	if verdict.Result != program.Unknown {
		t.Fatal("missing CA evidence cleared diagnosis")
	}
}
func TestWebhookCATargetValidation(t *testing.T) {
	good := program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "policy"}, Webhook: "policy.example", SecretNamespace: "other", SecretName: "serving", SecretKey: "tls.crt"}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*program.WebhookCA){
		func(v *program.WebhookCA) { v.Ref.Kind = "Secret" },
		func(v *program.WebhookCA) { v.Ref.Namespace = "app" },
		func(v *program.WebhookCA) { v.Ref.APIVersion = "v1" },
		func(v *program.WebhookCA) { v.Ref.Name = "" },
		func(v *program.WebhookCA) { v.Webhook = "" },
		func(v *program.WebhookCA) { v.SecretNamespace = "" },
		func(v *program.WebhookCA) { v.SecretName = "../serving" },
		func(v *program.WebhookCA) { v.SecretKey = "../ca.crt" },
		func(v *program.WebhookCA) { v.SecretKey = "" },
	} {
		bad := good
		change(&bad)
		if (program.Plan{Actions: []program.Action{program.SyncWebhookCA{Target: bad}}}).Validate() == nil {
			t.Fatal("invalid CA target accepted")
		}
	}
	var absent *program.SyncWebhookCA
	if (program.Plan{Actions: []program.Action{absent}}).Validate() == nil {
		t.Fatal("nil action accepted")
	}
}
