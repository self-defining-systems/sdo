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

// MaxAdmissionWebhooks bounds one AdmissionWebhooksFor answer.
const MaxAdmissionWebhooks = 16

// WebhookSecretKey names a Secret (and key, when the serving Pod mounts one) by
// reference only.
type WebhookSecretKey struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
}

// AdmissionWebhook is one webhook entry that admits Pod creation in the asked
// namespace, with its backend: the Service, its ready endpoint count and the
// Secrets its Pods mount. CABundle is a digest, never certificate bytes.
// To repair a broken admission webhook, bind the failing Namespace or workload (not the webhook configuration or Secret), read it with ReadView.AdmissionWebhooksFor, and repair with SyncWebhookCA for the CA, PatchObject for failurePolicy or the service reference, or DeleteObject for an orphaned entry.
type AdmissionWebhook struct {
	Configuration  sdk.ObjectRef      `json:"configuration"`
	Webhook        string             `json:"webhook"`
	FailurePolicy  string             `json:"failure_policy"`
	Service        *sdk.ObjectRef     `json:"service,omitempty"`
	Port           int32              `json:"port,omitempty"`
	ReadyEndpoints int                `json:"ready_endpoints"`
	ServingSecrets []WebhookSecretKey `json:"serving_secrets,omitempty"`
	CABundle       string             `json:"ca_bundle,omitempty"`
}

// CATarget names this entry and one serving Secret key for SyncWebhookCA and
// WebhookCAMatchesSecret. An empty secret.Key means the Pod mounts the whole
// Secret; set the key the serving certificate uses.
func (w AdmissionWebhook) CATarget(secret WebhookSecretKey) WebhookCA {
	return WebhookCA{Ref: w.Configuration, Webhook: w.Webhook, SecretNamespace: secret.Namespace, SecretName: secret.Name, SecretKey: secret.Key}
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
