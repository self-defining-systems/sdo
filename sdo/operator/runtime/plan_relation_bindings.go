package runtime

import (
	"fmt"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// bindPlanRelations completes reference-only inputs from exact observed
// relations before executable-plan validation. It never reads Secret values,
// invents a key, changes an explicit input or retargets another configuration.
func bindPlanRelations(plan program.Plan, view program.ReadView) (program.Plan, error) {
	plan.Actions = append([]program.Action(nil), plan.Actions...)
	for index, action := range plan.Actions {
		var ca program.SyncWebhookCA
		switch value := action.(type) {
		case program.SyncWebhookCA:
			ca = value
		case *program.SyncWebhookCA:
			if value == nil {
				continue
			}
			ca = *value
		default:
			continue
		}
		target := ca.Target
		if target.SecretNamespace != "" && target.SecretName != "" && target.SecretKey != "" {
			continue
		}
		// Structural identity errors still belong to ordinary plan validation.
		if target.Ref.APIVersion != "admissionregistration.k8s.io/v1" || target.Ref.Namespace != "" || target.Ref.Name == "" || target.Webhook == "" || (target.Ref.Kind != "MutatingWebhookConfiguration" && target.Ref.Kind != "ValidatingWebhookConfiguration") {
			continue
		}
		entries, err := view.AdmissionWebhooksFor(sdk.ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: view.Namespace()})
		if err != nil {
			return program.Plan{}, fmt.Errorf("bind SyncWebhookCA serving reference: %w", err)
		}
		candidates := map[program.WebhookCA]bool{}
		for _, entry := range entries {
			if entry.Configuration != target.Ref || entry.Webhook != target.Webhook {
				continue
			}
			for _, secret := range entry.ServingSecrets {
				if target.SecretNamespace != "" && target.SecretNamespace != secret.Namespace || target.SecretName != "" && target.SecretName != secret.Name || target.SecretKey != "" && secret.Key != "" && target.SecretKey != secret.Key {
					continue
				}
				bound := target
				if bound.SecretNamespace == "" {
					bound.SecretNamespace = secret.Namespace
				}
				if bound.SecretName == "" {
					bound.SecretName = secret.Name
				}
				if bound.SecretKey == "" {
					bound.SecretKey = secret.Key
				}
				if bound.Validate() == nil {
					candidates[bound] = true
				}
			}
		}
		if len(candidates) != 1 {
			return program.Plan{}, fmt.Errorf("bind SyncWebhookCA %s/%s entry %s: require one observed serving Secret namespace, name and key; found %d candidates; supply missing or ambiguous references explicitly", target.Ref.Kind, target.Ref.Name, target.Webhook, len(candidates))
		}
		for bound := range candidates {
			ca.Target = bound
		}
		plan.Actions[index] = ca
	}
	return plan, nil
}
