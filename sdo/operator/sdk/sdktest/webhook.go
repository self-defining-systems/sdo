package sdktest

import (
	"fmt"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func (v *RecordingReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	if err := t.Validate(); err != nil {
		return false, err
	}
	r, err := v.ReadView.WebhookCAMatchesSecret(t)
	if err != nil {
		err = fmt.Errorf("webhook CA observation unavailable")
	}
	v.record("WebhookCAMatchesSecret", []any{t}, r, err)
	return r, err
}
func (v *ReplayReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	if err := t.Validate(); err != nil {
		return false, err
	}
	var r bool
	err := v.replay("WebhookCAMatchesSecret", []any{t}, &r)
	return r, err
}
func (s Snapshot) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).WebhookCAMatchesSecret(t)
}

func (v *RecordingReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	r, err := v.ReadView.AdmissionWebhooksFor(subject)
	if err != nil {
		err = fmt.Errorf("admission webhook observation unavailable")
	}
	v.record("AdmissionWebhooksFor", []any{subject}, r, err)
	return r, err
}
func (v *ReplayReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	var r []program.AdmissionWebhook
	err := v.replay("AdmissionWebhooksFor", []any{subject}, &r)
	return r, err
}
func (s Snapshot) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	return (&ReplayReadView{Snapshot: s, Calls: s.Readings}).AdmissionWebhooksFor(subject)
}
