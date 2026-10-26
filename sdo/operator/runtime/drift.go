package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const maxDriftFields = 12

type DriftClassifier struct{}

type DriftControllerConfig struct {
	Inventory bool

	Events interface{ TakeObjectChanges() []ObjectChange }
	Source interface {
		AcceptedManifests(context.Context, string, string) ([]*unstructured.Unstructured, error)
	}
	Client      kubernetes.Interface
	View        func(sdk.DetectionContext) program.ReadView
	BatchWindow time.Duration
}

func (c *Controller) ConfigureDrift(config DriftControllerConfig) {
	c.drift = &config
	c.adoptionNeeded = config.Source != nil && config.Client != nil
}

func (c *Controller) processDrift(ctx context.Context, now time.Time, snapshot sdk.DetectionContext, changes []ObjectChange) {
	if c.drift == nil || c.drift.View == nil {
		return
	}
	view := c.drift.View(snapshot)
	classifier := DriftClassifier{}
	for _, change := range changes {
		object := change.New
		if object == nil {
			object = change.Old
		}
		if object != nil {
			ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
			if ref.Namespace != view.Namespace() && !driftRefAllowed(view, ref) {
				if scopedRefDeclared(view, ref) {
					c.retryDrift(change)
				}
				continue
			}
		}
		event, err := classifier.Classify(ctx, change, view, now)
		if err != nil {
			c.retryDrift(change)
			if c.OnError != nil {
				c.OnError(err)
			}
		}
		if event != nil && c.drift.Inventory && sourceInventoryKind(object) && event.Source == "absent from accepted source" {
			continue
		}
		if event == nil {
			continue
		}
		c.mu.Lock()
		c.driftEvents = append(c.driftEvents, *event)
		if len(c.driftEvents) > 40 {
			c.driftEvents = append([]sdk.DriftEvent(nil), c.driftEvents[len(c.driftEvents)-40:]...)
		}
		c.mu.Unlock()
		if event.Classification == sdk.DriftUnexplained {
			c.queueUnexplainedFinding(now, *event)
		}
	}

}

func safeDriftText(value string) string {
	words := strings.Fields(value)
	for index, word := range words {
		path := strings.Trim(word, `"'(),;`)
		if filepath.IsAbs(path) {
			words[index] = "[path]"
		}
	}
	return bounded(strings.Join(words, " "))
}

// Classify admits changed declared SDO fields and reviewed source-absent kinds.
// Neither admission path judges whether unexplained drift is a fault.
func (c DriftClassifier) Classify(ctx context.Context, change ObjectChange, view program.ReadView, now time.Time) (*sdk.DriftEvent, error) {
	object := change.New
	if object == nil {
		object = change.Old
	}
	if object == nil || change.Initial || view == nil {
		return nil, nil
	}
	ref := sdk.ObjectRef{APIVersion: object.GetAPIVersion(), Kind: object.GetKind(), Namespace: object.GetNamespace(), Name: object.GetName()}
	if !driftRefAllowed(view, ref) || ref.Name == "" {
		return nil, nil
	}
	// Foreign admission is independent of field-manager attribution. Restrict
	// it to reviewed effect-bearing kinds and suppress controller churn.
	if foreignDriftCandidate(change) {
		diff, err := view.DiffAgainstSource(ref)
		if err != nil {
			return nil, fmt.Errorf("compare %s/%s with accepted source: %w", ref.Kind, ref.Name, err)
		}
		if diff.SourceAbsent {
			if !diff.Changed && change.New != nil {
				return nil, nil
			}
			paths := make([]string, 0)
			for path := range flattenDriftObject(change.New) {
				paths = append(paths, path)
			}
			sort.Strings(paths)
			manager, operation, managerAt := changedDriftManager(change, paths, "unknown")
			event := &sdk.DriftEvent{Ref: ref, Source: "absent from accepted source", Manager: manager, Operation: operation, ManagerAt: managerAt,
				ObservedAt: now.UTC(), Classification: sdk.DriftUnexplained,
				Fields: []sdk.DriftField{{Path: "spec", Old: foreignDriftSpec(change.Old), New: foreignDriftSpec(change.New)}},
			}
			return event, nil
		}

		if (sdk.ObservationTarget{Kind: ref.Kind}).Cluster() && diff.Changed {
			return &sdk.DriftEvent{Ref: ref, Source: "accepted scoped configuration", ObservedAt: now.UTC(), Classification: sdk.DriftUnexplained, Fields: scopedDriftFields(change)}, nil
		}
	}
	owner := change.Old
	if owner == nil {
		owner = change.New
	}
	owned := ownedDriftPaths(owner, "sdo-deployer")
	if change.Adoption {
		owned = flattenDriftPaths(change.Old)
	}
	if len(owned) == 0 {
		return nil, nil
	}
	before := flattenDriftObject(change.Old)
	after := flattenDriftObject(change.New)
	paths := make([]string, 0)
	for path := range before {
		if !reflect.DeepEqual(before[path], after[path]) && ownsDriftPath(owned, path) {
			paths = append(paths, path)
		}
	}
	for path := range after {
		if _, exists := before[path]; !exists && ownsDriftPath(owned, path) {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return nil, nil
	}
	sort.Strings(paths)
	diff, err := view.DiffAgainstSource(ref)
	if err != nil {
		return nil, fmt.Errorf("compare %s/%s with accepted source: %w", ref.Kind, ref.Name, err)
	}
	if !diff.Changed || diff.SourceAbsent {
		return nil, nil
	}
	sourceValues := map[string]any{}
	if strings.HasPrefix(diff.Text, "accepted: ") {
		line := strings.SplitN(strings.TrimPrefix(diff.Text, "accepted: "), "\nobserved: ", 2)[0]
		var sourceObject map[string]any
		if json.Unmarshal([]byte(line), &sourceObject) == nil {
			sourceValues = flattenDriftObject(&unstructured.Unstructured{Object: sourceObject})
		}
	}
	manager, operation, managerAt := changedDriftManager(change, paths, "sdo-deployer")
	event := &sdk.DriftEvent{Ref: ref, Fields: make([]sdk.DriftField, 0, min(len(paths), maxDriftFields)), Source: "differs from accepted manifest", Manager: manager, Operation: operation, ManagerAt: managerAt, ObservedAt: now.UTC(), Classification: sdk.DriftUnexplained}
	for _, path := range paths[:min(len(paths), maxDriftFields)] {
		event.Fields = append(event.Fields, sdk.DriftField{Path: path, Old: renderDriftValue(path, before[path]), Source: renderDriftValue(path, sourceValues[path]), New: renderDriftValue(path, after[path])})
	}
	if change.Adoption {
		return event, nil
	}
	return event, nil
}

// The allowlist grows only with an impact argument and controller-churn tests.
func foreignDriftCandidate(change ObjectChange) bool {
	object := change.New
	if object == nil {
		if change.Old == nil || !(sdk.ObservationTarget{Kind: change.Old.GetKind()}).Cluster() {
			return false
		}
		object = change.Old
	}
	if !foreignDriftKind(object) {
		return false
	}
	if metav1.GetControllerOf(object) != nil {
		return false
	}
	return change.Old == nil || change.New == nil || foreignDriftSpec(change.Old) != foreignDriftSpec(change.New)
}

func foreignDriftKind(object *unstructured.Unstructured) bool {
	return object != nil && (object.GetAPIVersion() == "networking.k8s.io/v1" && object.GetKind() == "NetworkPolicy" || object.GetAPIVersion() == "admissionregistration.k8s.io/v1" && (object.GetKind() == "MutatingWebhookConfiguration" || object.GetKind() == "ValidatingWebhookConfiguration"))
}

func foreignDriftSpec(object *unstructured.Unstructured) string {
	if object == nil {
		return ""
	}
	value := object.Object["spec"]
	if (sdk.ObservationTarget{Kind: object.GetKind()}).Cluster() {
		value = object.Object["webhooks"]
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(encoded)
}

func flattenDriftObject(object *unstructured.Unstructured) map[string]any {
	result := map[string]any{}
	if object == nil {
		return result
	}
	var walk func(string, any)
	walk = func(path string, value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				next := key
				if path != "" {
					next = path + "." + key
				}
				walk(next, child)
			}
		case []any:
			for index, child := range typed {
				walk(fmt.Sprintf("%s[%d]", path, index), child)
			}
		default:
			result[path] = typed
		}
	}
	for key, value := range comparableFields(object.Object) {
		walk(key, value)
	}
	return result
}

func flattenDriftPaths(object *unstructured.Unstructured) map[string]bool {
	paths := map[string]bool{}
	for path := range flattenDriftObject(object) {
		paths[path] = true
	}
	return paths
}

func renderDriftValue(path string, value any) string {
	if value == nil {
		return ""
	}
	if strings.HasPrefix(path, "data.") || strings.HasPrefix(path, "binaryData.") {
		return digestJSON(value)
	}
	if value, ok := value.(string); ok {
		return safeDriftText(value)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return bounded(string(encoded))
}

func ownedDriftPaths(object *unstructured.Unstructured, manager string) map[string]bool {
	owned := map[string]bool{}
	if object == nil {
		return owned
	}
	for _, entry := range object.GetManagedFields() {
		if entry.Manager != manager || entry.Operation != metav1.ManagedFieldsOperationApply || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(entry.FieldsV1.Raw, &fields) == nil {
			walkOwnedFields(fields, object.Object, "", owned)
		}
	}
	return owned
}

func walkOwnedFields(fields map[string]any, object any, path string, owned map[string]bool) {
	if len(fields) == 0 && path != "" {
		owned[path] = true
		return
	}
	for key, raw := range fields {
		child, _ := raw.(map[string]any)
		switch {
		case strings.HasPrefix(key, "f:"):
			name := strings.TrimPrefix(key, "f:")
			next := name
			if path != "" {
				next = path + "." + name
			}
			current, _ := object.(map[string]any)
			walkOwnedFields(child, current[name], next, owned)
		case strings.HasPrefix(key, "k:"):
			var selector map[string]any
			if json.Unmarshal([]byte(strings.TrimPrefix(key, "k:")), &selector) != nil {
				continue
			}
			items, _ := object.([]any)
			for index, item := range items {
				candidate, _ := item.(map[string]any)
				matches := true
				for name, expected := range selector {
					if fmt.Sprint(candidate[name]) != fmt.Sprint(expected) {
						matches = false
					}
				}
				if matches {
					walkOwnedFields(child, item, fmt.Sprintf("%s[%d]", path, index), owned)
				}
			}
		}
	}
}

func ownsDriftPath(owned map[string]bool, path string) bool {
	for candidate := range owned {
		if path == candidate || strings.HasPrefix(path, candidate+".") || strings.HasPrefix(path, candidate+"[") {
			return true
		}
	}
	return false
}

func changedDriftManager(change ObjectChange, paths []string, fallback string) (string, string, *time.Time) {
	if change.New == nil {
		return "unknown", "delete", nil
	}
	old := map[string]metav1.ManagedFieldsEntry{}
	if change.Old != nil {
		for _, entry := range change.Old.GetManagedFields() {
			if entry.FieldsV1 != nil {
				old[entry.Manager+"/"+string(entry.Operation)] = entry
			}
		}
	}
	for _, entry := range change.New.GetManagedFields() {
		prior := old[entry.Manager+"/"+string(entry.Operation)]
		if entry.FieldsV1 == nil || entry.Manager == "sdo-deployer" || prior.FieldsV1 != nil && string(prior.FieldsV1.Raw) == string(entry.FieldsV1.Raw) && reflect.DeepEqual(prior.Time, entry.Time) {
			continue
		}
		owned := ownedDriftPathsForEntry(change.New, entry)
		for _, path := range paths {
			if ownsDriftPath(owned, path) {
				if entry.Time != nil {
					at := entry.Time.Time.UTC()
					return entry.Manager, string(entry.Operation), &at
				}
				return entry.Manager, string(entry.Operation), nil
			}
		}
	}
	for _, entry := range change.New.GetManagedFields() {
		if entry.Manager == "sdo-deployer" && entry.Time != nil {
			at := entry.Time.Time.UTC()
			return entry.Manager, string(entry.Operation), &at
		}
	}
	if fallback == "unknown" {
		return "unknown", "", nil
	}
	return fallback, string(metav1.ManagedFieldsOperationApply), nil
}

func ownedDriftPathsForEntry(object *unstructured.Unstructured, entry metav1.ManagedFieldsEntry) map[string]bool {
	owned := map[string]bool{}
	var fields map[string]any
	if entry.FieldsV1 != nil && json.Unmarshal(entry.FieldsV1.Raw, &fields) == nil {
		walkOwnedFields(fields, object.Object, "", owned)
	}
	return owned
}

// attachPendingDrift gives queued drift the same incident ownership as drift
// arriving after launch. Individual batch deadlines must not strand evidence
// across verification and acknowledgment, including after a controller restart.
// Run before evaluating drift so clearance cannot retract the queued evidence
// before it is retained in this incident's outcome.
func (c *Controller) attachPendingDrift() {
	c.mu.Lock()
	open := c.incidentOpen && c.currentIncidentRequest != nil
	c.mu.Unlock()
	if !open {
		return
	}
	for _, finding := range c.batcher.DrainDetectorPrefix("drift.") {
		c.attachDrift(finding)
	}
}

// attachDrift retains evidence arriving after launch without changing the frozen
// worker request. Tools read the moving controller view on each call.
func (c *Controller) attachDrift(finding sdk.Finding) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.incidentOpen || c.currentIncidentRequest == nil {
		return false
	}
	request := c.currentIncidentRequest
	key := FindingStateKey(finding.DetectorID, FindingFingerprint(finding))
	found := false
	for i, old := range request.Findings {
		if FindingStateKey(old.DetectorID, FindingFingerprint(old)) == key {
			request.Findings[i] = finding
			found = true
			break
		}
	}
	if !found {
		request.Findings = append(request.Findings, finding)
	}
	sortFindings(request.Findings)
	request.DriftEvents = driftEventsForFindings(c.driftEvents, request.Findings)
	c.incidentFindingKeys = findingKeys(request.Findings)
	if c.incidentDispatchedAt.IsZero() {
		request.PreparedAgentTask = ""
	}
	if c.incidentDispatchedAt.IsZero() {
		c.noteBatchedLocked(request.IncidentID, []sdk.Finding{finding})
	}
	return true
}

func (c *Controller) queueUnexplainedFinding(now time.Time, event sdk.DriftEvent) {
	encoded, _ := json.Marshal(event)
	finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, Severity: sdk.SeverityLatent,
		Summary: "unexplained drift; in the app namespace restore accepted-source differences unless positive evidence shows intended drift, then verify", Evidence: bounded(string(encoded)), PrimaryResource: event.Ref, ParameterBindings: map[string]sdk.ObjectRef{"resource": event.Ref}, Metadata: map[string]any{"drift_event": event}}
	finding.Fingerprint = fmt.Sprintf("%s/%x", FindingFingerprint(finding), sha256.Sum256([]byte(driftEventKey(event))))
	c.observeUnexplainedDrift(now, finding)
	if c.attachDrift(finding) {
		return
	}
	window := c.drift.BatchWindow
	if window <= 0 {
		window = 20 * time.Second
	}
	c.batcher.AddAtWithDebounce(finding, now, window)
}

// Record activation before attaching to the moving incident so the existing
// late-findings transport sees an after-dispatch activation exactly once.
func (c *Controller) observeUnexplainedDrift(now time.Time, finding sdk.Finding) {
	findings := []sdk.Finding{finding}
	for _, state := range c.tracker.Snapshot() {
		if state.DetectorID == finding.DetectorID && state.Active && FindingFingerprint(state.Finding) != FindingFingerprint(finding) {
			findings = append(findings, state.Finding)
		}
	}
	c.tracker.SetPolicy(finding.DetectorID, 1, 1, 0)
	changes := c.tracker.Observe(now, finding.DetectorID, findings)
	c.recordEvaluation(finding.DetectorID, now, findings)
	c.recordFiring(sdk.DetectorSpec{ID: finding.DetectorID, Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder}, now, findings, changes)
}

func driftRefAllowed(view program.ReadView, ref sdk.ObjectRef) bool {
	if ref.Namespace == view.Namespace() {
		return true
	}
	scoped, ok := view.(interface{ ScopedRefAllowed(sdk.ObjectRef) bool })
	return ok && scoped.ScopedRefAllowed(ref)
}

func scopedDriftFields(change ObjectChange) []sdk.DriftField {
	before, after := flattenDriftObject(change.Old), flattenDriftObject(change.New)
	keys := map[string]bool{}
	for k := range before {
		keys[k] = true
	}
	for k := range after {
		keys[k] = true
	}
	ordered := []string{}
	for k := range keys {
		if !reflect.DeepEqual(before[k], after[k]) {
			ordered = append(ordered, k)
		}
	}
	sort.Strings(ordered)
	fields := []sdk.DriftField{}
	for _, k := range ordered {
		if len(fields) == maxDriftFields {
			break
		}
		a, _ := json.Marshal(before[k])
		b, _ := json.Marshal(after[k])
		fields = append(fields, sdk.DriftField{Path: k, Old: bounded(string(a)), New: bounded(string(b))})
	}
	return fields
}

func (c *Controller) retryDrift(change ObjectChange) {
	if queue, ok := c.drift.Events.(interface{ RetryObjectChange(ObjectChange) }); ok {
		queue.RetryObjectChange(change)
	}
}
func scopedRefDeclared(view any, ref sdk.ObjectRef) bool {
	t := sdk.ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
	if t.Cluster() {
		t.Name = ref.Name
	}
	for _, status := range sdk.ScopedSource(view).ObservationStatuses() {
		if status.Target == t {
			return true
		}
	}
	return false
}
