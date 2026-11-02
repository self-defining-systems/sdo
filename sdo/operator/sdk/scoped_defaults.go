package sdk

import (
	admissionv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// Normalize only fields retained by the safe webhook projection, matching
// Kubernetes v1.30 admissionregistration/v1 defaulting. This is deterministic
// comparison, never a dry-run write against a foreign resource.
// https://github.com/kubernetes/kubernetes/blob/v1.30.3/pkg/apis/admissionregistration/v1/defaults.go
func defaultWebhook(failure **admissionv1.FailurePolicyType, match **admissionv1.MatchPolicyType, namespace, object **metav1.LabelSelector, timeout **int32, rules []admissionv1.RuleWithOperations) {
	if *failure == nil {
		v := admissionv1.Fail
		*failure = &v
	}
	if *match == nil {
		v := admissionv1.Equivalent
		*match = &v
	}
	if *namespace == nil {
		*namespace = &metav1.LabelSelector{}
	}
	if *object == nil {
		*object = &metav1.LabelSelector{}
	}
	if *timeout == nil {
		v := int32(10)
		*timeout = &v
	}
	for i := range rules {
		if rules[i].Scope == nil {
			v := admissionv1.AllScopes
			rules[i].Scope = &v
		}
	}
}
func defaultMutatingWebhook(w *admissionv1.MutatingWebhook) {
	defaultWebhook(&w.FailurePolicy, &w.MatchPolicy, &w.NamespaceSelector, &w.ObjectSelector, &w.TimeoutSeconds, w.Rules)
	if w.ReinvocationPolicy == nil {
		v := admissionv1.NeverReinvocationPolicy
		w.ReinvocationPolicy = &v
	}
}
func defaultValidatingWebhook(w *admissionv1.ValidatingWebhook) {
	defaultWebhook(&w.FailurePolicy, &w.MatchPolicy, &w.NamespaceSelector, &w.ObjectSelector, &w.TimeoutSeconds, w.Rules)
}

func defaultServicePort(port *corev1.ServicePort) {
	if port.Protocol == "" {
		port.Protocol = corev1.ProtocolTCP
	}
	if port.TargetPort == intstr.FromInt32(0) || port.TargetPort == intstr.FromString("") {
		port.TargetPort = intstr.FromInt32(port.Port)
	}
}
