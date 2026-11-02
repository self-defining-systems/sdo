package kubereads

import (
	"bytes"
	"context"
	"fmt"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk/program"
)

// WebhookCAState is one webhook entry's identity and current CA bundle.
type WebhookCAState struct {
	Bundle       []byte
	Index        int
	UID, Version string
}

// selectedWebhook never returns an API error body: admission errors can contain
// the private CA. Both admission kinds share selection and write preconditions.
func SelectedWebhook(ctx context.Context, client kubernetes.Interface, t program.WebhookCA) (WebhookCAState, error) {
	var configs []admissionv1.WebhookClientConfig
	var names []string
	var state WebhookCAState
	if t.Ref.Kind == "ValidatingWebhookConfiguration" {
		object, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, t.Ref.Name, metav1.GetOptions{})
		if err != nil {
			return state, fmt.Errorf("webhook CA configuration read unavailable")
		}
		state.UID, state.Version = string(object.UID), object.ResourceVersion
		for _, w := range object.Webhooks {
			configs = append(configs, w.ClientConfig)
			names = append(names, w.Name)
		}
	} else {
		object, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, t.Ref.Name, metav1.GetOptions{})
		if err != nil {
			return state, fmt.Errorf("webhook CA configuration read unavailable")
		}
		state.UID, state.Version = string(object.UID), object.ResourceVersion
		for _, w := range object.Webhooks {
			configs = append(configs, w.ClientConfig)
			names = append(names, w.Name)
		}
	}
	count := 0
	for i, name := range names {
		if name == t.Webhook {
			state.Index = i
			count++
		}
	}
	if count != 1 {
		return state, fmt.Errorf("webhook name must match exactly once")
	}
	state.Bundle = configs[state.Index].CABundle
	return state, nil
}

// webhookCA confines the selected Secret key to runtime memory.
func WebhookCA(ctx context.Context, client kubernetes.Interface, protectedNamespace string, t program.WebhookCA) ([]byte, WebhookCAState, error) {
	if err := t.Validate(); err != nil {
		return nil, WebhookCAState{}, err
	}
	if protectedNamespace != "" && t.SecretNamespace == protectedNamespace {
		return nil, WebhookCAState{}, apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, t.SecretNamespace, fmt.Errorf("generic plans cannot access the SDO control namespace"))
	}
	if client == nil {
		return nil, WebhookCAState{}, fmt.Errorf("webhook CA transport unavailable")
	}
	state, err := SelectedWebhook(ctx, client, t)
	if err != nil {
		return nil, state, err
	}
	secret, err := client.CoreV1().Secrets(t.SecretNamespace).Get(ctx, t.SecretName, metav1.GetOptions{})
	if err != nil {
		return nil, state, fmt.Errorf("webhook CA Secret read unavailable")
	}
	ca := secret.Data[t.SecretKey]
	if len(ca) == 0 {
		return nil, state, fmt.Errorf("webhook CA Secret key is empty or absent")
	}
	return ca, state, nil
}

// WebhookCAMatchesSecret reports whether the webhook's caBundle equals the
// Secret key without exposing either value.
func WebhookCAMatchesSecret(ctx context.Context, client kubernetes.Interface, protectedNamespace string, t program.WebhookCA) (bool, error) {
	ca, state, err := WebhookCA(ctx, client, protectedNamespace, t)
	if err != nil {
		return false, err
	}
	return bytes.Equal(state.Bundle, ca), nil
}
