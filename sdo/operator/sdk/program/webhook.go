package program

import (
	"fmt"
	"k8s.io/apimachinery/pkg/util/validation"
	"sdo.dev/operator/sdk"
	"strings"
)

// WebhookCA identifies one webhook and a Secret key. Secret contents remain
// inside the runtime; programs receive only an equality observation.
type WebhookCA struct {
	Ref             sdk.ObjectRef
	Webhook         string
	SecretNamespace string
	SecretName      string
	SecretKey       string
}

func (t WebhookCA) Validate() error {
	if t.Ref.APIVersion != "admissionregistration.k8s.io/v1" || (t.Ref.Kind != "ValidatingWebhookConfiguration" && t.Ref.Kind != "MutatingWebhookConfiguration") || t.Ref.Namespace != "" || len(validation.IsDNS1123Subdomain(t.Ref.Name)) != 0 || t.Ref.Name == "" || t.Webhook == "" {
		return fmt.Errorf("webhook CA requires a named v1 admission configuration and webhook")
	}
	if t.SecretNamespace == "" || len(validation.IsDNS1123Label(t.SecretNamespace)) != 0 || t.SecretName == "" || len(validation.IsDNS1123Subdomain(t.SecretName)) != 0 || t.SecretKey == "" || len(validation.IsConfigMapKey(t.SecretKey)) != 0 || strings.Contains(t.SecretKey, "..") {
		return fmt.Errorf("webhook CA requires a named Secret namespace, name and key")
	}
	return nil
}

// SyncWebhookCA copies one Secret key into the named webhook's caBundle inside
// the runtime. Receipts contain references and a SHA256 digest, never CA bytes.
// This operation is irreversible; no Secret-bearing preimage is retained.
type SyncWebhookCA struct {
	Target WebhookCA
	Reason string
}

func (SyncWebhookCA) action()           {}
func (a SyncWebhookCA) Validate() error { return a.Target.Validate() }
func (v snapshotReadView) WebhookCAMatchesSecret(t WebhookCA) (bool, error) {
	if err := t.Validate(); err != nil {
		return false, err
	}
	if source, ok := v.DetectionContext.(interface{ WebhookCAMatchesSecret(WebhookCA) (bool, error) }); ok {
		return source.WebhookCAMatchesSecret(t)
	}
	return false, fmt.Errorf("webhook CA observation unavailable")
}
func (v *checkedReadView) WebhookCAMatchesSecret(t WebhookCA) (bool, error) {
	r, err := v.ReadView.WebhookCAMatchesSecret(t)
	v.check(err)
	return r, err
}
