package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk/program"
	"time"
)

var errWebhookCAWriteUnknown = errors.New("webhook CA write unconfirmed")

// PlanWebhookCAClient is a private execution boundary: no CA bytes cross it.
type PlanWebhookCAClient interface {
	SyncWebhookCA(context.Context, program.WebhookCA, bool) (string, error)
}

type webhookCAState struct {
	bundle       []byte
	index        int
	uid, version string
}

// selectedWebhook never returns an API error body: admission errors can contain
// the private CA. Both admission kinds share selection and write preconditions.
func (c TypedPlanClient) selectedWebhook(ctx context.Context, t program.WebhookCA) (webhookCAState, error) {
	var configs []admissionv1.WebhookClientConfig
	var names []string
	var state webhookCAState
	if t.Ref.Kind == "ValidatingWebhookConfiguration" {
		object, err := c.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, t.Ref.Name, metav1.GetOptions{})
		if err != nil {
			return state, fmt.Errorf("webhook CA configuration read unavailable")
		}
		state.uid, state.version = string(object.UID), object.ResourceVersion
		for _, w := range object.Webhooks {
			configs = append(configs, w.ClientConfig)
			names = append(names, w.Name)
		}
	} else {
		object, err := c.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, t.Ref.Name, metav1.GetOptions{})
		if err != nil {
			return state, fmt.Errorf("webhook CA configuration read unavailable")
		}
		state.uid, state.version = string(object.UID), object.ResourceVersion
		for _, w := range object.Webhooks {
			configs = append(configs, w.ClientConfig)
			names = append(names, w.Name)
		}
	}
	count := 0
	for i, name := range names {
		if name == t.Webhook {
			state.index = i
			count++
		}
	}
	if count != 1 {
		return state, fmt.Errorf("webhook name must match exactly once")
	}
	state.bundle = configs[state.index].CABundle
	return state, nil
}

// webhookCA confines the selected Secret key to runtime memory.
func (c TypedPlanClient) webhookCA(ctx context.Context, t program.WebhookCA) ([]byte, webhookCAState, error) {
	if err := t.Validate(); err != nil {
		return nil, webhookCAState{}, err
	}
	if err := c.checkPlanNamespace(t.SecretNamespace); err != nil {
		return nil, webhookCAState{}, err
	}
	if c.Client == nil {
		return nil, webhookCAState{}, fmt.Errorf("webhook CA transport unavailable")
	}
	state, err := c.selectedWebhook(ctx, t)
	if err != nil {
		return nil, state, err
	}
	secret, err := c.Client.CoreV1().Secrets(t.SecretNamespace).Get(ctx, t.SecretName, metav1.GetOptions{})
	if err != nil {
		return nil, state, fmt.Errorf("webhook CA Secret read unavailable")
	}
	ca := secret.Data[t.SecretKey]
	if len(ca) == 0 {
		return nil, state, fmt.Errorf("webhook CA Secret key is empty or absent")
	}
	return ca, state, nil
}

func (v LiveReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	c := TypedPlanClient{Client: v.Client, ProtectedNamespace: v.ProtectedNamespace}
	ca, state, err := c.webhookCA(ctx, t)
	if err != nil {
		return false, err
	}
	return bytes.Equal(state.bundle, ca), nil
}
func (v *cachedReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	var result bool
	err := v.read("WebhookCAMatchesSecret", []any{t}, &result, func() (any, error) { return v.ReadView.WebhookCAMatchesSecret(t) })
	return result, err
}
func (c TypedPlanClient) SyncWebhookCA(ctx context.Context, t program.WebhookCA, dry bool) (string, error) {
	ca, state, err := c.webhookCA(ctx, t)
	if err != nil {
		return "", err
	}
	if state.uid == "" || state.version == "" {
		return "", fmt.Errorf("webhook CA configuration identity unavailable")
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(ca))
	if dry || bytes.Equal(state.bundle, ca) {
		return digest, nil
	}
	patch, _ := json.Marshal([]any{
		map[string]any{"op": "test", "path": "/metadata/uid", "value": state.uid},
		map[string]any{"op": "test", "path": "/metadata/resourceVersion", "value": state.version},
		map[string]any{"op": "test", "path": fmt.Sprintf("/webhooks/%d/name", state.index), "value": t.Webhook},
		map[string]any{"op": "add", "path": fmt.Sprintf("/webhooks/%d/clientConfig/caBundle", state.index), "value": base64.StdEncoding.EncodeToString(ca)},
	})
	// Discard the response rather than publishing an object or private patch.
	if t.Ref.Kind == "ValidatingWebhookConfiguration" {
		_, err = c.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Patch(ctx, t.Ref.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	} else {
		_, err = c.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().Patch(ctx, t.Ref.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
	}
	if err != nil {
		return "", errWebhookCAWriteUnknown
	}
	// Read back the exact written bytes and incarnation, independent of health.
	observed, err := c.selectedWebhook(ctx, t)
	if err != nil || observed.uid != state.uid || !bytes.Equal(observed.bundle, ca) {
		return "", fmt.Errorf("%w: readback mismatch or unavailable", errWebhookCAWriteUnknown)
	}
	return digest, nil
}
func (e PlanExecutor) webhookCAStep(ctx context.Context, a program.SyncWebhookCA, dry bool) (PlanApplicationResult, error) {
	r := PlanApplicationResult{Plan: program.Plan{Actions: []program.Action{a}}, DryRun: dry}
	r.ActionReports = newPlanReports(r.Plan)
	c, ok := e.Client.(PlanWebhookCAClient)
	if !ok {
		return r, fmt.Errorf("webhook CA transport unavailable")
	}
	digest, err := c.SyncWebhookCA(ctx, a.Target, dry)
	if !dry && (err == nil || errors.Is(err, errWebhookCAWriteUnknown)) {
		r.Applied = true
		r.ActionsApplied = []program.Action{a}
		r.Patches = []PlanPatch{{Ref: a.Target.Ref, Operation: "sync_webhook_ca", Irreversible: true}}
		r.ActionReports[0].Status = "unknown"
	}
	if err != nil {
		return r, err
	}
	r.ActionReports[0].WebhookCADigest = digest
	if !dry {
		r.ActionReports[0].Status = "applied"
	}
	return r, nil
}
