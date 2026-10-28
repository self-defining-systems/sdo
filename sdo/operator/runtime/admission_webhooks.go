package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const maxServingSecrets = 8

// admissionSubjectNamespace resolves the namespace whose Pod creation an
// admission failure blocks: the Namespace itself, or the namespace of the
// workload or Pod that could not be created.
func admissionSubjectNamespace(subject sdk.ObjectRef) (string, error) {
	switch subject.Kind {
	case "Namespace":
		if subject.Name != "" {
			return subject.Name, nil
		}
	case "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet", "ReplicationController", "Job", "CronJob", "Pod":
		if subject.Namespace != "" {
			return subject.Namespace, nil
		}
	}
	return "", fmt.Errorf("admission webhooks are resolved from a Namespace or a workload, not %s", subject.Kind)
}

func podCreateRule(rules []admissionv1.RuleWithOperations) bool {
	has := func(values []string, want string) bool {
		for _, value := range values {
			if value == want || value == "*" {
				return true
			}
		}
		return false
	}
	for _, rule := range rules {
		ops := false
		for _, op := range rule.Operations {
			ops = ops || op == admissionv1.Create || op == admissionv1.OperationAll
		}
		scoped := rule.Scope == nil || *rule.Scope != admissionv1.ClusterScope
		resource := false
		for _, r := range rule.Resources {
			resource = resource || r == "pods" || r == "*" || r == "*/*"
		}
		if ops && scoped && resource && has(rule.APIGroups, "") {
			return true
		}
	}
	return false
}

// AdmissionWebhooksFor resolves, with the runtime's own grant, which webhook
// entries admit Pod creation in the subject's namespace and what backs them. It
// returns references, counts and a CA digest only: Secret objects are never read.
func (v LiveReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	namespace, err := admissionSubjectNamespace(subject)
	if err != nil {
		return nil, err
	}
	if v.Client == nil {
		return nil, fmt.Errorf("admission webhook observation unavailable")
	}
	ctx, cancel := context.WithTimeout(v.context(), 5*time.Second)
	defer cancel()
	ns, err := v.Client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("admission webhook namespace read unavailable")
	}
	type entry struct {
		kind, config, name, failure string
		rules                       []admissionv1.RuleWithOperations
		selector                    *metav1.LabelSelector
		client                      admissionv1.WebhookClientConfig
	}
	var entries []entry
	validating, err := v.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("admission webhook configuration read unavailable")
	}
	for _, config := range validating.Items {
		for _, w := range config.Webhooks {
			entries = append(entries, entry{"ValidatingWebhookConfiguration", config.Name, w.Name, failurePolicy(w.FailurePolicy), w.Rules, w.NamespaceSelector, w.ClientConfig})
		}
	}
	mutating, err := v.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("admission webhook configuration read unavailable")
	}
	for _, config := range mutating.Items {
		for _, w := range config.Webhooks {
			entries = append(entries, entry{"MutatingWebhookConfiguration", config.Name, w.Name, failurePolicy(w.FailurePolicy), w.Rules, w.NamespaceSelector, w.ClientConfig})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].kind+"/"+entries[i].config+"/"+entries[i].name < entries[j].kind+"/"+entries[j].config+"/"+entries[j].name
	})
	var result []program.AdmissionWebhook
	for _, e := range entries {
		if !podCreateRule(e.rules) {
			continue
		}
		selector := labels.Everything()
		if e.selector != nil {
			if selector, err = metav1.LabelSelectorAsSelector(e.selector); err != nil {
				continue
			}
		}
		if !selector.Matches(labels.Set(ns.Labels)) {
			continue
		}
		if len(result) == program.MaxAdmissionWebhooks {
			break
		}
		w := program.AdmissionWebhook{
			Configuration: sdk.ObjectRef{APIVersion: "admissionregistration.k8s.io/v1", Kind: e.kind, Name: e.config},
			Webhook:       e.name, FailurePolicy: e.failure, CABundle: string(sdk.ProjectCABundle(e.client.CABundle)),
		}
		if service := e.client.Service; service != nil {
			w.Service = &sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name}
			w.Port = 443
			if service.Port != nil {
				w.Port = *service.Port
			}
			w.ReadyEndpoints, w.ServingSecrets = v.webhookBackend(ctx, service.Namespace, service.Name)
		}
		result = append(result, w)
	}
	return result, nil
}

func failurePolicy(p *admissionv1.FailurePolicyType) string {
	if p == nil {
		return string(admissionv1.Fail)
	}
	return string(*p)
}

// webhookBackend counts ready endpoints and names the Secrets the Service's Pods
// mount. A lookup failure reports no backend evidence; it never fails the relation.
func (v LiveReadView) webhookBackend(ctx context.Context, namespace, name string) (int, []program.WebhookSecretKey) {
	ready := 0
	if endpoints, err := v.Client.CoreV1().Endpoints(namespace).Get(ctx, name, metav1.GetOptions{}); err == nil {
		for _, subset := range endpoints.Subsets {
			ready += len(subset.Addresses)
		}
	}
	service, err := v.Client.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || len(service.Spec.Selector) == 0 {
		return ready, nil
	}
	pods, err := v.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.SelectorFromSet(service.Spec.Selector).String(), Limit: 20})
	if err != nil {
		return ready, nil
	}
	seen := map[program.WebhookSecretKey]bool{}
	var secrets []program.WebhookSecretKey
	add := func(secret string, items []corev1.KeyToPath) {
		if v.ProtectedNamespace != "" && namespace == v.ProtectedNamespace {
			return
		}
		keys := []string{""}
		if len(items) > 0 {
			keys = keys[:0]
			for _, item := range items {
				keys = append(keys, item.Key)
			}
		}
		for _, key := range keys {
			ref := program.WebhookSecretKey{Namespace: namespace, Name: secret, Key: key}
			if !seen[ref] && len(secrets) < maxServingSecrets {
				seen[ref] = true
				secrets = append(secrets, ref)
			}
		}
	}
	for _, pod := range pods.Items {
		for _, volume := range pod.Spec.Volumes {
			if volume.Secret != nil {
				add(volume.Secret.SecretName, volume.Secret.Items)
			}
			if volume.Projected != nil {
				for _, source := range volume.Projected.Sources {
					if source.Secret != nil {
						add(source.Secret.Name, source.Secret.Items)
					}
				}
			}
		}
	}
	sort.Slice(secrets, func(i, j int) bool {
		a, b := secrets[i], secrets[j]
		return a.Name+"/"+a.Key < b.Name+"/"+b.Key
	})
	return ready, secrets
}

func (v *cachedReadView) AdmissionWebhooksFor(subject sdk.ObjectRef) ([]program.AdmissionWebhook, error) {
	var result []program.AdmissionWebhook
	err := v.read("AdmissionWebhooksFor", []any{subject}, &result, func() (any, error) { return v.ReadView.AdmissionWebhooksFor(subject) })
	return result, err
}
