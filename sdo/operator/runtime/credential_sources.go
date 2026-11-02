package runtime

import (
	"context"

	corev1 "k8s.io/api/core/v1"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk/program"
)

// CredentialReferenceError identifies an unavailable reference without exposing
// its contents or an upstream API response. Reason is a fixed diagnostic code.
type CredentialReferenceError = kubereads.CredentialReferenceError

var assignmentCredential = kubereads.AssignmentCredential

func (c TypedPlanClient) resolveCredential(ctx context.Context, namespace string, ref program.CredentialRef) (string, error) {
	return kubereads.ResolveCredential(ctx, c.Client, c.ProtectedNamespace, namespace, ref)
}

func (c TypedPlanClient) credentialConfigMap(ctx context.Context, namespace, name string) (*corev1.ConfigMap, error) {
	return kubereads.CredentialConfigMap(ctx, c.Client, namespace, name)
}
