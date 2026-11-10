package runtime

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk/storestate"
)

// Credential check Pod bounds.
const (
	credentialCheckDeadline = 60 * time.Second
	credentialCheckWait     = 90 * time.Second
	credentialCheckLogBytes = 4096
)

// CredentialCheckLabel marks the short-lived Pods of credential checks.
const CredentialCheckLabel = "sdo.dev/credential-check"

// StoreCredentialCheck runs one credential check Pod: the store container's
// image on the store Pod's node, with the client container's env and envFrom
// references copied as references (the kubelet resolves them; the controller
// never reads a value), attempting one authentication against the store Pod's
// IP. It returns the Pod's output and always deletes the Pod.
func (c TypedPlanClient) StoreCredentialCheck(ctx context.Context, probe StoreCredentialProbe) (string, error) {
	if c.Client == nil {
		return "", fmt.Errorf("no Kubernetes client")
	}
	if c.ProtectedNamespace != "" && probe.Namespace == c.ProtectedNamespace {
		return "", fmt.Errorf("the controller namespace is refused")
	}
	pods := c.Client.CoreV1().Pods(probe.Namespace)
	store, err := pods.Get(ctx, probe.StorePod, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("store Pod %s unreadable: %w", probe.StorePod, err)
	}
	client, err := pods.Get(ctx, probe.ClientPod, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("client Pod %s unreadable: %w", probe.ClientPod, err)
	}
	spec, err := credentialCheckPod(store, client, probe)
	if err != nil {
		return "", err
	}
	created, err := pods.Create(ctx, spec, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("check Pod not created: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_ = pods.Delete(cleanup, created.Name, metav1.DeleteOptions{GracePeriodSeconds: ptr.To[int64](0)})
	}()
	deadline := lifecycleio.Now().Add(credentialCheckWait)
	ticker := lifecycleio.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		current, err := pods.Get(ctx, created.Name, metav1.GetOptions{})
		if err == nil && (current.Status.Phase == corev1.PodSucceeded || current.Status.Phase == corev1.PodFailed) {
			break
		}
		if lifecycleio.Now().After(deadline) {
			return "", fmt.Errorf("check Pod did not finish within %s", credentialCheckWait)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
		}
	}
	logs, err := pods.GetLogs(created.Name, &corev1.PodLogOptions{Container: "check", LimitBytes: ptr.To[int64](credentialCheckLogBytes)}).DoRaw(ctx)
	if err != nil {
		return "", fmt.Errorf("check Pod output unreadable: %w", err)
	}
	return string(logs), nil
}

// credentialCheckPod builds the check Pod from the live store and client Pods.
func credentialCheckPod(store, client *corev1.Pod, probe StoreCredentialProbe) (*corev1.Pod, error) {
	var image string
	for _, container := range store.Spec.Containers {
		if container.Name == probe.StoreContainer {
			image = container.Image
		}
	}
	if image == "" {
		return nil, fmt.Errorf("store Pod %s has no container %s", store.Name, probe.StoreContainer)
	}
	if store.Status.PodIP == "" {
		return nil, fmt.Errorf("store Pod %s has no IP", store.Name)
	}
	var source *corev1.Container
	for i := range client.Spec.Containers {
		if client.Spec.Containers[i].Name == probe.ClientContainer {
			source = &client.Spec.Containers[i]
		}
	}
	if source == nil {
		return nil, fmt.Errorf("client Pod %s has no container %s", client.Name, probe.ClientContainer)
	}
	argv, err := storestate.CredentialCheckCommand(probe.Engine, store.Status.PodIP, probe.Port, probe.Inputs)
	if err != nil {
		return nil, err
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "sdo-credential-check-", Namespace: probe.Namespace,
			Labels: map[string]string{CredentialCheckLabel: "true"}},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:         ptr.To(int64(credentialCheckDeadline / time.Second)),
			AutomountServiceAccountToken:  ptr.To(false),
			TerminationGracePeriodSeconds: ptr.To[int64](0),
			NodeName:                      store.Spec.NodeName,
			ImagePullSecrets:              store.Spec.ImagePullSecrets,
			Containers: []corev1.Container{{
				Name: "check", Image: image, ImagePullPolicy: corev1.PullIfNotPresent,
				Command: argv[:1], Args: argv[1:],
				Env: source.Env, EnvFrom: source.EnvFrom,
			}},
		},
	}, nil
}
