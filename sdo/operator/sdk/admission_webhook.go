package sdk

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// WebhookCA identifies one webhook and a Secret key. Secret contents remain
// inside the runtime; programs receive only an equality observation.
type WebhookCA struct {
	Ref             ObjectRef
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

// MaxAdmissionWebhooks bounds one admission webhook answer.
const MaxAdmissionWebhooks = 16

// DefaultWebhookTimeoutSeconds is the API server's timeout for a webhook entry
// that declares none.
const DefaultWebhookTimeoutSeconds = 10

// WebhookSecretKey names a Secret (and key, when the serving Pod mounts one) by
// reference only.
type WebhookSecretKey struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
}

// AdmissionWebhook is one webhook entry admitting Pods in the namespace.
// Its backend is the Service, its ready endpoint count and the Secrets its
// Pods mount, or, for a webhook served outside the cluster, the URL's scheme
// and host. CABundle is a digest, never certificate bytes. TimeoutSeconds is
// how long the API server waits for it (DefaultWebhookTimeoutSeconds when the
// entry declares none); ObjectSelector, when set, limits it to Pods whose
// labels match.
// To repair a broken admission webhook, bind the failing Namespace or workload (not the webhook configuration or Secret), read it with ReadView.AdmissionWebhooksFor, and repair with SyncWebhookCA for the CA, PatchObject for failurePolicy or the service reference, or DeleteObject for an orphaned entry.
type AdmissionWebhook struct {
	Configuration  ObjectRef             `json:"configuration"`
	Webhook        string                `json:"webhook"`
	FailurePolicy  string                `json:"failure_policy"`
	TimeoutSeconds int32                 `json:"timeout_seconds,omitempty"`
	ObjectSelector *metav1.LabelSelector `json:"object_selector,omitempty"`
	Service        *ObjectRef            `json:"service,omitempty"`
	Port           int32                 `json:"port,omitempty"`
	URL            string                `json:"url,omitempty"`
	ReadyEndpoints int                   `json:"ready_endpoints"`
	ServingSecrets []WebhookSecretKey    `json:"serving_secrets,omitempty"`
	CABundle       string                `json:"ca_bundle,omitempty"`
}

// Mutating reports whether the entry belongs to a MutatingWebhookConfiguration.
func (w AdmissionWebhook) Mutating() bool {
	return w.Configuration.Kind == "MutatingWebhookConfiguration"
}

// CATarget names this entry and one serving Secret key for SyncWebhookCA and
// WebhookCAMatchesSecret. An empty secret.Key means the Pod mounts the whole
// Secret; set the key the serving certificate uses.
func (w AdmissionWebhook) CATarget(secret WebhookSecretKey) WebhookCA {
	return WebhookCA{Ref: w.Configuration, Webhook: w.Webhook, SecretNamespace: secret.Namespace, SecretName: secret.Name, SecretKey: secret.Key}
}
