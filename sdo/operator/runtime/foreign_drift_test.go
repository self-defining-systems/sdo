package runtime

import (
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk/program"
)

func foreignPolicy(name string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: networkingv1.NetworkPolicySpec{
		PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "api"}}, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
	}}
}

var _ program.ReadView = LiveReadView{}
