package program

import (
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// RetryAdmission requeues one ReplicaSet that still needs Pods after admission
// FailedCreate. It changes only a controller annotation, preserving replicas
// and the Pod template. The executor refuses targets outside the incident
// namespace and checks live failure evidence before writing. A retry is not
// proof of admission or application recovery; independent verification remains
// required. Webhook repairs automatically include retries for affected sets.
type RetryAdmission struct{ Ref sdk.ObjectRef }

func (RetryAdmission) action() {}

// AdmissionRejection identifies admission failures, excluding quota, scheduling
// and image failures. Both rollout handoff and admission retry use this rule.
func AdmissionRejection(message string) bool {
	lower := strings.ToLower(message)
	return strings.Contains(lower, "webhook") && (strings.Contains(lower, "denied") || strings.Contains(lower, "failed calling") || strings.Contains(lower, "failed to call") || strings.Contains(lower, "x509") || strings.Contains(lower, "tls")) || strings.Contains(lower, "denied by policy") || strings.Contains(lower, "admission policy") && strings.Contains(lower, "denied")
}

// AdmissionFailedCreate returns current admission failure messages only while
// a nonterminating ReplicaSet still lacks desired Pods. Historical events alone
// cannot authorize a retry of a healthy, scaled-down or replaced workload.
func AdmissionFailedCreate(set appsv1.ReplicaSet) []string {
	desired := int32(1)
	if set.Spec.Replicas != nil {
		desired = *set.Spec.Replicas
	}
	if set.DeletionTimestamp != nil || desired <= set.Status.Replicas {
		return nil
	}
	var messages []string
	for _, condition := range set.Status.Conditions {
		if condition.Type == appsv1.ReplicaSetReplicaFailure && condition.Status == corev1.ConditionTrue && condition.Reason == "FailedCreate" && AdmissionRejection(condition.Message) {
			messages = append(messages, condition.Message)
		}
	}
	return messages
}
