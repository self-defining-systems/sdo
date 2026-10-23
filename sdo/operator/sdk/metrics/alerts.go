package metrics

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
)

// AlertRulePrefix prefixes the rule ID of each alert finding ("alert.<alertname>").
const AlertRulePrefix = "alert."

// AlertFiringState is the only alert state that reports; pending alerts have
// not yet held for their rule's duration.
const AlertFiringState = "firing"

const maxAnnotationBytes = 512

type alertsDetector struct {
	spec    sdk.DetectorSpec
	mapping LabelMapping
}

// NewAlertsDetector returns a detector that reports every firing alert whose
// namespace label (mapping.Namespace, default "namespace") names the
// application namespace, one finding per alert. It filters on nothing else:
// the alert rules, not this detector, decide what is worth firing. Pending
// alerts are ignored, and alerts without a namespace label are ignored but
// counted in each finding's metadata. Alerts that could not be read are a
// detector error, never a clear.
func NewAlertsDetector(spec sdk.DetectorSpec, mapping LabelMapping) sdk.Detector {
	return alertsDetector{spec: spec, mapping: mapping.withDefaults()}
}

func (d alertsDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d alertsDetector) ConsumesAlerts() bool { return true }

func (d alertsDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	frozen, ok := AlertsFrom(ctx)
	if !ok {
		return nil, errors.New("alerts unavailable: detection context carries no alerts")
	}
	if frozen.Error != "" {
		return nil, fmt.Errorf("alerts unavailable: %s", frozen.Error)
	}
	namespace := ctx.Namespace()
	unlabelled := 0
	findings := make([]sdk.Finding, 0)
	for _, alert := range frozen.Alerts {
		if alert.State != AlertFiringState {
			continue
		}
		alertNamespace, ok := d.mapping.namespaceOf(alert.Labels)
		if !ok {
			unlabelled++
			continue
		}
		if alertNamespace != namespace {
			continue
		}
		findings = append(findings, d.finding(namespace, frozen, alert))
	}
	for index := range findings {
		findings[index].Metadata["alerts_without_namespace_label"] = unlabelled
	}
	sort.Slice(findings, func(left int, right int) bool {
		if findings[left].RuleID != findings[right].RuleID {
			return findings[left].RuleID < findings[right].RuleID
		}
		return findings[left].Fingerprint < findings[right].Fingerprint
	})
	return findings, nil
}

func (d alertsDetector) finding(namespace string, frozen AlertsResult, alert Alert) sdk.Finding {
	name := alert.Name()
	if name == "" {
		name = "unnamed"
	}
	ruleID := AlertRulePrefix + name
	primary, related := d.mapping.resources(namespace, alert.Labels)
	severity := sdk.SeverityWarning
	if alert.Labels["severity"] == "critical" {
		severity = sdk.SeverityCritical
	}
	annotations := make(map[string]string, len(alert.Annotations))
	annotationNames := make([]string, 0, len(alert.Annotations))
	for key, value := range alert.Annotations {
		if len(value) > maxAnnotationBytes {
			value = value[:maxAnnotationBytes] + "…"
		}
		annotations[key] = value
		annotationNames = append(annotationNames, key)
	}
	sort.Strings(annotationNames)
	details := make([]string, 0, len(annotationNames))
	for _, key := range annotationNames {
		details = append(details, fmt.Sprintf("%s=%q", key, annotations[key]))
	}
	activeAt := ""
	if !alert.ActiveAt.IsZero() {
		activeAt = alert.ActiveAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	}
	observed := frozen.ObservedAt.UTC().Format("2006-01-02T15:04:05Z07:00")
	evidence := fmt.Sprintf("alert %s is %s since %s (observed %s) with labels %s", name, alert.State, activeAt, observed, LabelString(alert.Labels))
	if alert.Value != "" {
		evidence += ", value " + alert.Value
	}
	if len(details) > 0 {
		evidence += "; annotations: " + strings.Join(details, ", ")
	}
	return sdk.Finding{
		DetectorID:       d.spec.ID,
		RuleID:           ruleID,
		Status:           sdk.FindingActive,
		Severity:         severity,
		Summary:          fmt.Sprintf("alert %s is firing for %s %s", name, primary.Kind, primary.Name),
		Evidence:         evidence,
		PrimaryResource:  primary,
		RelatedResources: related,
		Playbooks:        append([]string(nil), d.spec.Playbooks...),
		Fingerprint:      fingerprint(d.spec.ID, ruleID, alert.Labels),
		Metadata: map[string]any{
			"alert_name":  name,
			"state":       alert.State,
			"active_at":   activeAt,
			"observed_at": observed,
			"value":       alert.Value,
			"labels":      copyLabels(alert.Labels),
			"annotations": annotations,
		},
	}
}
