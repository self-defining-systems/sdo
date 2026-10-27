package runtime

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// AdmissionBlockedError means a workload's writes landed, but admission rejects
// the Pods they require. Waiting cannot repair this prerequisite. Pending is
// evidence only: it grants neither rollback nor independent repair credit.
type AdmissionBlockedError struct{ Pending *contracts.RolloutPending }

func (e *AdmissionBlockedError) Error() string { return "admission_blocked: " + e.Pending.GetReason() }

var admissionWebhookName = regexp.MustCompile(`(?i)webhook\s+["']([^"']+)["']`)

func admissionRejection(message string) bool {
	lower := strings.ToLower(message)
	// Scope comes from the workload ownership join; do not classify image pulls,
	// scheduling failures or unrelated warning events as admission prerequisites.
	return strings.Contains(lower, "webhook") && (strings.Contains(lower, "denied") || strings.Contains(lower, "failed calling") || strings.Contains(lower, "failed to call") || strings.Contains(lower, "x509") || strings.Contains(lower, "tls")) ||
		strings.Contains(lower, "denied by policy") || strings.Contains(lower, "admission policy") && strings.Contains(lower, "denied")
}

func (w KubernetesPlanRollouts) admissionBlocked(ctx context.Context, blocker *contracts.RolloutBlocker, messages []string) *AdmissionBlockedError {
	if len(messages) == 0 {
		return nil
	}
	sort.Strings(messages)
	pending := &contracts.RolloutPending{ReasonCode: "admission_blocked", Reason: boundedRolloutText(strings.Join(messages, "; "), 4096), Blockers: []*contracts.RolloutBlocker{blocker}}
	names := map[string]bool{}
	for _, message := range messages {
		for _, match := range admissionWebhookName.FindAllStringSubmatch(message, -1) {
			names[match[1]] = true
		}
	}
	for name := range names {
		pending.AdmissionWebhooks = append(pending.AdmissionWebhooks, name)
	}
	sort.Strings(pending.AdmissionWebhooks)
	// Events name a webhook entry, not its containing configuration. Resolve that
	// identity without reading credentials or modifying the admission control.
	if len(names) > 0 {
		lookup, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		validating, err := w.Client.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(lookup, metav1.ListOptions{})
		if err == nil {
			for _, config := range validating.Items {
				for _, webhook := range config.Webhooks {
					if names[webhook.Name] {
						pending.AdmissionConfigurations = append(pending.AdmissionConfigurations, &contracts.ObjectRef{ApiVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration", Name: config.Name})
						break
					}
				}
			}
		}
		mutating, err := w.Client.AdmissionregistrationV1().MutatingWebhookConfigurations().List(lookup, metav1.ListOptions{})
		if err == nil {
			for _, config := range mutating.Items {
				for _, webhook := range config.Webhooks {
					if names[webhook.Name] {
						pending.AdmissionConfigurations = append(pending.AdmissionConfigurations, &contracts.ObjectRef{ApiVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration", Name: config.Name})
						break
					}
				}
			}
		}
		sort.Slice(pending.AdmissionConfigurations, func(i, j int) bool {
			a, b := pending.AdmissionConfigurations[i], pending.AdmissionConfigurations[j]
			return a.Kind+"/"+a.Name < b.Kind+"/"+b.Name
		})
		for _, ref := range pending.AdmissionConfigurations {
			pending.Reason = fmt.Sprintf("%s/%s: %s", ref.Kind, ref.Name, pending.Reason)
		}
	}
	return &AdmissionBlockedError{Pending: pending}
}
