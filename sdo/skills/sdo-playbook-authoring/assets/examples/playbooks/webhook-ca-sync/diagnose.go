// Package webhookcasync restores an admission webhook's caBundle from the Secret its server mounts.
package webhookcasync

import (
	"context"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type Program struct{}

func (Program) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "webhook-ca-sync", Params: []program.ParamSpec{{Name: "configuration", Kind: "MutatingWebhookConfiguration", Required: true}}}
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

func (Program) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	stale, err := staleCA(v, b)
	if err != nil {
		return program.Verdict{}, err
	}
	if len(stale) == 0 {
		return program.Verdict{Result: program.NoMatch}, nil
	}
	return program.Verdict{Result: program.Match, Evidence: []program.Evidence{{Resource: b.Objects["configuration"], Fact: "webhook caBundle differs from its serving Secret", Observed: stale[0].Webhook, Expected: stale[0].SecretName}}}, nil
}
