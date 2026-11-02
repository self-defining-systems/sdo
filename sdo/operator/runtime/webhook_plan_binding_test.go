package runtime

import (
	"context"
	"testing"
	"time"

	kruntime "k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func incompleteWebhookMitigator(target program.WebhookCA) program.Mitigator {
	return funcMitigator{id: "observed-ca-repair", plan: func(program.ReadView, program.Bindings) (program.Plan, error) {
		return program.Plan{Actions: []program.Action{program.SyncWebhookCA{Target: target, Reason: "restore observed serving certificate trust"}}}, nil
	}}
}

// The saved social repair omitted Secret reference parameters. A unique real
// Namespace -> webhook -> Service -> Pod mount relation can supply references
// without any Secret read or guessed field/parameter names.
func TestMitigationBindsMissingWebhookCAReferencesFromLiveRelation(t *testing.T) {
	client := admissionCluster()
	secretReads := 0
	client.PrependReactor("*", "secrets", func(ktesting.Action) (bool, kruntime.Object, error) { secretReads++; return false, nil, nil })
	view := LiveReadView{Carrier: inputs.New(sdktest.Snapshot{NamespaceName: "app"}), Client: client}
	want := program.WebhookCA{Ref: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: "pod-policy"}, Webhook: "pod-policy.validation.k8s.io", SecretNamespace: "platform", SecretName: "pod-policy-tls", SecretKey: "tls.crt"}
	for _, mask := range []int{1, 2, 3, 4, 5, 6, 7} {
		target := want
		if mask&1 != 0 {
			target.SecretNamespace = ""
		}
		if mask&2 != 0 {
			target.SecretName = ""
		}
		if mask&4 != 0 {
			target.SecretKey = ""
		}
		plan, err := BuildMitigationPlan(context.Background(), incompleteWebhookMitigator(target), view, program.Bindings{}, time.Second)
		if err != nil {
			t.Fatalf("missing reference fields mask=%d: %v", mask, err)
		}
		got, ok := plan.Actions[0].(program.SyncWebhookCA)
		if !ok || got.Target != want {
			t.Fatalf("mask=%d: wrong bound target: %+v", mask, plan)
		}
	}
	if secretReads != 0 {
		t.Fatalf("relation binding read %d Secrets", secretReads)
	}
}

func TestMitigationDoesNotGuessAmbiguousOrUnobservedWebhookReferences(t *testing.T) {
	ref := sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: "synthetic-policy"}
	base := program.AdmissionWebhook{Configuration: ref, Webhook: "synthetic.example", ServingSecrets: []program.WebhookSecretKey{{Namespace: "platform", Name: "serving", Key: "tls.crt"}}}
	for _, name := range []string{"different kind", "different entry", "multiple certificates", "whole mount without key", "conflicting supplied reference"} {
		t.Run(name, func(t *testing.T) {
			entry := base
			target := program.WebhookCA{Ref: ref, Webhook: base.Webhook}
			switch name {
			case "different kind":
				target.Ref.Kind = "MutatingWebhookConfiguration"
			case "different entry":
				target.Webhook = "other.example"
			case "multiple certificates":
				entry.ServingSecrets = append(append([]program.WebhookSecretKey(nil), entry.ServingSecrets...), program.WebhookSecretKey{Namespace: "platform", Name: "other", Key: "tls.crt"})
			case "whole mount without key":
				entry.ServingSecrets = []program.WebhookSecretKey{{Namespace: "platform", Name: "serving"}}
			case "conflicting supplied reference":
				target.SecretNamespace = "unrelated"
			}
			view := webhookBindingView{ReadView: &sdktest.ReplayReadView{Snapshot: sdktest.Snapshot{NamespaceName: "app"}}, entries: []program.AdmissionWebhook{entry}}
			_, err := BuildMitigationPlan(context.Background(), incompleteWebhookMitigator(target), view, program.Bindings{}, time.Second)
			if err == nil {
				t.Fatal("incomplete or conflicting relation supplied write authority")
			}
		})
	}
}

type webhookBindingView struct {
	program.ReadView
	entries []program.AdmissionWebhook
}

func (v webhookBindingView) AdmissionWebhooksFor(sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	return v.entries, nil
}
