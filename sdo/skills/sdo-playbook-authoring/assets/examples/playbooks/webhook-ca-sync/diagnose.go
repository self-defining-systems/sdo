// Package webhookcasync restores an admission webhook's caBundle from the Secret its server mounts.
package webhookcasync

import (
	"context"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "webhook-ca-sync", Params: []program.ParamSpec{{Name: "configuration", Kind: "MutatingWebhookConfiguration", Required: true}}}
}

// Diagnose decides from what every capture holds: the frozen Pod admission
// dry run. Mitigate reads the webhook relation live, when it applies.
func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	admission, observed := v.PodAdmission()
	if !observed || admission.Error != "" {
		return program.Verdict{Result: program.Unknown}, nil
	}
	rejection := admission.Rejection
	if admission.Admitted || !strings.Contains(rejection, "failed calling webhook") || !(strings.Contains(rejection, "x509") || strings.Contains(rejection, "unknown authority")) {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: b.Objects["configuration"], Fact: "Pod admission fails webhook TLS verification", Observed: rejection, Expected: "admitted"}}}, nil
}

// staleCA finds the bound configuration's webhook entries whose caBundle differs from
// the serving Secret; entries are read by relation, never by name.
func staleCA(v program.ReadView, b program.Bindings) ([]program.WebhookCA, error) {
	configuration := b.Objects["configuration"]
	webhooks, err := v.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: v.Namespace()})
	if err != nil {
		return nil, err
	}
	var stale []program.WebhookCA
	for _, webhook := range webhooks {
		if webhook.Configuration.Kind != configuration.Kind || webhook.Configuration.Name != configuration.Name || len(webhook.ServingSecrets) != 1 {
			continue
		}
		target := webhook.CATarget(webhook.ServingSecrets[0])
		matches, err := v.WebhookCAMatchesSecret(target)
		if err != nil {
			return nil, err
		}
		if !matches {
			stale = append(stale, target)
		}
	}
	return stale, nil
}
