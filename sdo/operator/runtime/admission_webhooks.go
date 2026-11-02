package runtime

import (
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// AdmissionWebhooksFor resolves, with the runtime's own grant, which webhook
// entries admit Pod creation in the subject's namespace and what backs them.
func (v LiveReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	return kubereads.AdmissionWebhooksFor(v.context(), v.Client, v.ProtectedNamespace, subject)
}

var failurePolicy = kubereads.FailurePolicy

func (v *cachedReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	var result []program.AdmissionWebhook
	err := v.read("AdmissionWebhooksFor", []any{subject}, &result, func() (any, error) { return v.ReadView.AdmissionWebhooksFor(subject) })
	return result, err
}
