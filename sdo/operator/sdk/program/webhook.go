package program

import (
	"fmt"
	"sdo.dev/operator/sdk"
)

// WebhookCA identifies one webhook and a Secret key (sdk.WebhookCA).
type WebhookCA = sdk.WebhookCA

// MaxAdmissionWebhooks bounds one AdmissionWebhooksFor answer.
const MaxAdmissionWebhooks = sdk.MaxAdmissionWebhooks

// WebhookSecretKey names a Secret by reference only (sdk.WebhookSecretKey).
type WebhookSecretKey = sdk.WebhookSecretKey

// AdmissionWebhook is one webhook entry admitting Pods (sdk.AdmissionWebhook).
type AdmissionWebhook = sdk.AdmissionWebhook

// Webhook repairs automatically requeue affected admission-blocked ReplicaSets.
// RetryAdmission also exposes this recovery after a raw repair.
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

func (v snapshotReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]AdmissionWebhook, error) {
	if source, ok := v.DetectionContext.(interface {
		AdmissionWebhooksFor(sdk.ObjectRef) ([]AdmissionWebhook, error)
	}); ok {
		return source.AdmissionWebhooksFor(subject)
	}
	return nil, fmt.Errorf("admission webhook observation unavailable")
}
func (v *checkedReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]AdmissionWebhook, error) {
	r, err := v.ReadView.AdmissionWebhooksFor(subject)
	v.check(err)
	return r, err
}
