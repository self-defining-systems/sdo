package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk/program"
	"time"
)

var errWebhookCAWriteUnknown = errors.New("webhook CA write unconfirmed")

// PlanWebhookCAClient is a private execution boundary: no CA bytes cross it.
type PlanWebhookCAClient interface {
	SyncWebhookCA(context.Context, program.WebhookCA, bool) (string, error)
}

func (v LiveReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	return kubereads.WebhookCAMatchesSecret(ctx, v.Client, v.ProtectedNamespace, t)
}
func (v *cachedReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	var result bool
	err := v.read("WebhookCAMatchesSecret", []any{t}, &result, func() (any, error) { return v.ReadView.WebhookCAMatchesSecret(t) })
	return result, err
}
func (c TypedPlanClient) SyncWebhookCA(ctx context.Context, t program.WebhookCA, dry bool) (string, error) {
	ca, state, err := kubereads.WebhookCA(ctx, c.Client, c.ProtectedNamespace, t)
	if err != nil {
		return "", err
	}
	if state.UID == "" || state.Version == "" {
		return "", fmt.Errorf("webhook CA configuration identity unavailable")
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(ca))
	if dry || bytes.Equal(state.Bundle, ca) {
		return digest, nil
	}
	patch, _ := json.Marshal([]any{
		map[string]any{"op": "test", "path": "/metadata/uid", "value": state.UID},
		map[string]any{"op": "test", "path": "/metadata/resourceVersion", "value": state.Version},
		map[string]any{"op": "test", "path": fmt.Sprintf("/webhooks/%d/name", state.Index), "value": t.Webhook},
		map[string]any{"op": "add", "path": fmt.Sprintf("/webhooks/%d/clientConfig/caBundle", state.Index), "value": base64.StdEncoding.EncodeToString(ca)},
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
	observed, err := kubereads.SelectedWebhook(ctx, c.Client, t)
	if err != nil || observed.UID != state.UID || !bytes.Equal(observed.Bundle, ca) {
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
