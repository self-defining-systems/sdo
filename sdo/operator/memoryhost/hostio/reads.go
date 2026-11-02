package hostio

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The reads below mirror the controller's live view so a saved program behaves
// the same in the host. They are confined to the observed namespace, refuse the
// control namespace, and have no write path: every transport is a read call or a
// fixed read command. A read that cannot be served returns a named error.

const readTimeout = 5 * time.Second

func (v *LiveReadView) readContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(v.ctx, readTimeout)
}

func (v *LiveReadView) refuseControl(namespace string) error {
	if p := v.backends.ProtectedNamespace; p != "" && namespace == p {
		return fmt.Errorf("%w: the SDO control namespace %q is refused", kubereads.ErrOutOfScope, p)
	}
	return nil
}

func (v *LiveReadView) getPod(ctx context.Context, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return kubereads.GetPod(ctx, v.client, v.backends.ProtectedNamespace, ref)
}

func (v *LiveReadView) KubernetesRead(r program.KubernetesRequest) (json.RawMessage, error) {
	if err := program.ValidateKubernetesRead(r); err != nil {
		return nil, err
	}
	if err := kubereads.ScopeRead(r, v.Namespace(), v.backends.ProtectedNamespace); err != nil {
		return nil, err
	}
	transport := v.backends.APITransport
	if transport == nil {
		transport = func(r program.KubernetesRequest) (kubereads.APIDoer, *url.URL, error) {
			return kubereads.APITransport(v.backends.Client, v.backends.Config, r)
		}
	}
	doer, base, err := transport(r)
	if err != nil {
		return nil, err
	}
	ctx, cancel := v.readContext()
	defer cancel()
	raw, err := kubereads.APIRequest(ctx, doer, base, r)
	if err != nil {
		return nil, err
	}
	projected, projectionErr := sdk.ProjectWebhookCAJSON(raw)
	if projectionErr == nil {
		return projected, nil
	}
	if strings.Contains(r.Path, "webhookconfigurations") {
		return nil, fmt.Errorf("webhook API response projection unavailable")
	}
	return raw, nil
}

func (v *LiveReadView) Observe(o program.ExternalOperation) (int32, error) {
	if err := o.Validate(); err != nil {
		return 0, err
	}
	pod := o.Pod()
	if pod.Namespace != v.Namespace() {
		return 0, fmt.Errorf("%w: command Pod namespace %q is not %q", kubereads.ErrOutOfScope, pod.Namespace, v.Namespace())
	}
	if err := v.refuseControl(pod.Namespace); err != nil {
		return 0, err
	}
	captured, err := v.Object(pod)
	if err != nil || captured == nil || captured.GetUID() == "" {
		return 0, fmt.Errorf("command Pod absent from snapshot")
	}
	ctx, cancel := v.readContext()
	defer cancel()
	return kubereads.Observe(ctx, v.getPod, v.backends.exec(), v.backends.ProtectedNamespace, captured, o)
}

func (v *LiveReadView) DatabaseUsers(t program.DatabaseTarget) ([]program.DatabaseUser, error) {
	if err := t.Validate(); err != nil {
		return nil, err
	}
	if t.Pod.Namespace != v.Namespace() {
		return nil, fmt.Errorf("database catalog outside snapshot namespace")
	}
	if err := v.refuseControl(t.Pod.Namespace); err != nil {
		return nil, err
	}
	captured, err := v.Object(t.Pod)
	if err != nil || captured == nil || captured.GetUID() == "" {
		return nil, fmt.Errorf("database Pod absent from snapshot")
	}
	ctx, cancel := v.readContext()
	defer cancel()
	reader := kubereads.DatabaseReader{Get: v.getPod, Exec: v.backends.exec(), Client: v.client, ProtectedNamespace: v.backends.ProtectedNamespace}
	current, err := v.getPod(ctx, t.Pod)
	if err != nil || current == nil || current.GetUID() != captured.GetUID() {
		return nil, fmt.Errorf("database Pod identity differs from snapshot")
	}
	users, err := reader.Users(ctx, t)
	if err != nil {
		return nil, err
	}
	current, err = v.getPod(ctx, t.Pod)
	if err != nil || current == nil || current.GetUID() != captured.GetUID() {
		return nil, fmt.Errorf("database Pod identity differs from snapshot")
	}
	return users, nil
}

func (v *LiveReadView) PodUsage(ref sdk.ObjectRef) (*metricsv1beta1.PodMetrics, error) {
	if err := kubereads.ValidUsageRef(ref, "Pod", v.Namespace()); err != nil {
		return nil, err
	}
	client, err := program.Kubernetes(v)
	if err != nil {
		return nil, err
	}
	return kubereads.PodUsage(v.ctx, client, ref)
}

func (v *LiveReadView) NodeUsage(ref sdk.ObjectRef) (*metricsv1beta1.NodeMetrics, error) {
	if err := kubereads.ValidUsageRef(ref, "Node", v.Namespace()); err != nil {
		return nil, err
	}
	client, err := program.Kubernetes(v)
	if err != nil {
		return nil, err
	}
	return kubereads.NodeUsage(v.ctx, client, ref)
}

func (v *LiveReadView) WebhookCAMatchesSecret(t program.WebhookCA) (bool, error) {
	ctx, cancel := v.readContext()
	defer cancel()
	return kubereads.WebhookCAMatchesSecret(ctx, v.client, v.backends.ProtectedNamespace, t)
}

func (v *LiveReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	namespace, err := kubereads.AdmissionSubjectNamespace(subject)
	if err != nil {
		return nil, err
	}
	if namespace != v.Namespace() {
		return nil, fmt.Errorf("%w: admission subject namespace %q is not %q", kubereads.ErrOutOfScope, namespace, v.Namespace())
	}
	if err := v.refuseControl(namespace); err != nil {
		return nil, err
	}
	ctx, cancel := v.readContext()
	defer cancel()
	return kubereads.AdmissionWebhooksFor(ctx, v.client, v.backends.ProtectedNamespace, subject)
}
