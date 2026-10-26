package core

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// logicalPlaybookID is the grammar of a playbook reference in a detector spec
// or finding: a logical ID, never a repository path.
var logicalPlaybookID = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

func ValidateDetectors(detectors []sdk.Detector) error {
	seen := make(map[string]struct{}, len(detectors))
	for index, detector := range detectors {
		if detector == nil {
			return fmt.Errorf("detector %d is nil", index)
		}
		spec := detector.Spec()
		id := strings.TrimSpace(spec.ID)
		if id == "" {
			return fmt.Errorf("detector %d has empty id", index)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate detector id %q", id)
		}
		if err := validateDetectorSpec(spec); err != nil {
			return fmt.Errorf("detector %q: %w", id, err)
		}
		seen[id] = struct{}{}
	}
	return nil
}

var supportedWatches = map[string]struct{}{
	"admissionregistration.k8s.io/v1/MutatingWebhookConfiguration":   {},
	"admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration": {},
	"apps/v1/Deployment":                 {},
	"apps/v1/ReplicaSet":                 {},
	"discovery.k8s.io/v1/EndpointSlice":  {},
	"networking.k8s.io/v1/NetworkPolicy": {},
	"v1/ConfigMap":                       {},
	"v1/Endpoints":                       {},
	"v1/Event":                           {},
	"v1/Pod":                             {},
	"v1/Service":                         {},
	// Emitted by the controller runtime's synthetic-traffic prober rather
	// than by a Kubernetes informer.
	traffic.Watch.APIVersion + "/" + traffic.Watch.Kind: {},
	// Emitted by the controller runtime's Service reachability observer.
	servicehealth.ReachabilityWatch.APIVersion + "/" + servicehealth.ReachabilityWatch.Kind: {},
	// Declared by metrics detectors, which run on their interval; no
	// informer backs it.
	metrics.Watch.APIVersion + "/" + metrics.Watch.Kind: {},
}

// SupportedWatches lists, sorted, the "apiVersion/Kind" resources a detector
// may watch.
func SupportedWatches() []string {
	keys := make([]string, 0, len(supportedWatches))
	for key := range supportedWatches {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func validateDetectorSpec(spec sdk.DetectorSpec) error {
	if spec.Interval <= 0 {
		return fmt.Errorf("interval must be greater than zero")
	}
	if spec.Class != "" || spec.Owner != "" {
		if spec.Class != sdk.DetectorClassHealth && spec.Class != sdk.DetectorClassIncident {
			return fmt.Errorf("unsupported detector class %q", spec.Class)
		}
		expectedOwner := sdk.DetectorOwnerResponder
		if spec.Class == sdk.DetectorClassHealth {
			expectedOwner = sdk.DetectorOwnerHealthJudge
		}
		if spec.Owner != expectedOwner {
			return fmt.Errorf("class %q must be owned by %q", spec.Class, expectedOwner)
		}
		if spec.Persistence.Firing < 1 || spec.Persistence.Clearing < 1 {
			return fmt.Errorf("persistence firing and clearing counts must be positive")
		}
		if spec.Persistence.MinDuration < 0 {
			return fmt.Errorf("persistence min duration must not be negative")
		}
		if spec.Batching.Severity != sdk.SeverityInfo && spec.Batching.Severity != sdk.SeverityWarning &&
			spec.Batching.Severity != sdk.SeverityCritical {
			return fmt.Errorf("unsupported batching severity %q", spec.Batching.Severity)
		}
		if spec.Batching.Debounce < 0 {
			return fmt.Errorf("batching debounce must not be negative")
		}
		if strings.TrimSpace(spec.OriginatingCommit) == "" {
			return fmt.Errorf("originating commit is required")
		}
		if spec.Class == sdk.DetectorClassIncident && strings.TrimSpace(spec.OriginatingIncident) == "" {
			return fmt.Errorf("incident detector originating incident is required")
		}
	}
	seenWatches := make(map[string]struct{}, len(spec.Watches))
	for index, watch := range spec.Watches {
		if watch.APIVersion != strings.TrimSpace(watch.APIVersion) || watch.Kind != strings.TrimSpace(watch.Kind) ||
			watch.Namespace != strings.TrimSpace(watch.Namespace) {
			return fmt.Errorf("watch %d fields must not contain surrounding whitespace", index)
		}
		resourceKey := watch.APIVersion + "/" + watch.Kind
		if _, ok := supportedWatches[resourceKey]; !ok {
			return fmt.Errorf("watch %d uses unsupported resource %q; the controller watches only %s", index, resourceKey, strings.Join(SupportedWatches(), ", "))
		}
		key := resourceKey + "/" + watch.Namespace
		if _, ok := seenWatches[key]; ok {
			return fmt.Errorf("duplicate watch %q", key)
		}
		seenWatches[key] = struct{}{}
	}
	seenPlaybooks := make(map[string]struct{}, len(spec.Playbooks))
	for index, playbook := range spec.Playbooks {
		if !logicalPlaybookID.MatchString(playbook) {
			return fmt.Errorf("playbook %d must be a logical playbook ID, got %q", index, playbook)
		}
		if _, ok := seenPlaybooks[playbook]; ok {
			return fmt.Errorf("duplicate declared playbook %q", playbook)
		}
		seenPlaybooks[playbook] = struct{}{}
	}
	return nil
}

func ValidateFinding(spec sdk.DetectorSpec, finding sdk.Finding) error {
	if strings.TrimSpace(finding.DetectorID) == "" {
		return fmt.Errorf("detector id is required")
	}
	if finding.DetectorID != spec.ID {
		return fmt.Errorf("detector id %q does not match %q", finding.DetectorID, spec.ID)
	}
	if strings.TrimSpace(finding.RuleID) == "" {
		return fmt.Errorf("rule id is required")
	}
	if finding.Status != sdk.FindingActive && finding.Status != sdk.FindingResolved {
		return fmt.Errorf("unsupported status %q", finding.Status)
	}
	if finding.Severity != sdk.SeverityInfo && finding.Severity != sdk.SeverityWarning && finding.Severity != sdk.SeverityCritical {
		return fmt.Errorf("unsupported severity %q", finding.Severity)
	}
	if strings.TrimSpace(finding.Summary) == "" {
		return fmt.Errorf("summary is required")
	}
	if strings.TrimSpace(finding.Evidence) == "" {
		return fmt.Errorf("evidence is required")
	}
	if err := validateObjectRef(finding.PrimaryResource); err != nil {
		return fmt.Errorf("primary resource: %w", err)
	}
	for index, resource := range finding.RelatedResources {
		if err := validateObjectRef(resource); err != nil {
			return fmt.Errorf("related resource %d: %w", index, err)
		}
	}
	for parameter, resource := range finding.ParameterBindings {
		if strings.TrimSpace(parameter) == "" {
			return fmt.Errorf("parameter binding name is required")
		}
		if err := validateObjectRef(resource); err != nil {
			return fmt.Errorf("parameter binding %q: %w", parameter, err)
		}
	}
	declaredPlaybooks := make(map[string]struct{}, len(spec.Playbooks))
	for _, playbook := range spec.Playbooks {
		declaredPlaybooks[playbook] = struct{}{}
	}
	seenPlaybooks := make(map[string]struct{}, len(finding.Playbooks))
	for _, playbook := range finding.Playbooks {
		if _, ok := declaredPlaybooks[playbook]; !ok {
			return fmt.Errorf("playbook %q was not declared by detector", playbook)
		}
		if _, ok := seenPlaybooks[playbook]; ok {
			return fmt.Errorf("duplicate playbook %q", playbook)
		}
		seenPlaybooks[playbook] = struct{}{}
	}
	if _, err := json.Marshal(finding.Metadata); err != nil {
		return fmt.Errorf("metadata must be JSON serializable: %w", err)
	}
	return nil
}

func validateObjectRef(resource sdk.ObjectRef) error {
	if strings.TrimSpace(resource.Kind) == "" {
		return fmt.Errorf("kind is required")
	}
	if strings.TrimSpace(resource.Name) == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}
