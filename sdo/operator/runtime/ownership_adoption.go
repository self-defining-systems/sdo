package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"log/slog"
	"reflect"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// OwnershipAdoptionStatus records the transfer of accepted manifest fields to
// the deployer's field manager for one accepted source commit.
type OwnershipAdoptionStatus struct {
	SourceCommit       string    `json:"source_commit,omitempty"`
	AdoptedObjects     int       `json:"adopted_objects,omitempty"`
	ConflictsTakenOver int       `json:"conflicts_taken_over,omitempty"`
	FromManagers       []string  `json:"from_managers,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
	SkippedReason      string    `json:"skipped_reason,omitempty"`
	RetryAt            time.Time `json:"retry_at,omitempty"`
	Failures           int       `json:"failures,omitempty"`
	// AcceptedObjects lists the objects SDO has recorded as accepted, kept
	// across source commits (see acceptedObjectKey).
	AcceptedObjects []string `json:"accepted_objects,omitempty"`
	// UnadoptedObjects lists declared objects adoption left alone because it
	// cannot handle their kind, each with the reason (see inventoryKind).
	UnadoptedObjects []string `json:"unadopted_objects,omitempty"`
}

func acceptedObjectKey(ref sdk.ObjectRef) string {
	return ref.APIVersion + "/" + ref.Kind + "/" + ref.Namespace + "/" + ref.Name
}

func (c *Controller) recordAdoptionError(now time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.ownershipAdoption
	if status.SourceCommit != c.config.SourceCommit {
		status = OwnershipAdoptionStatus{SourceCommit: c.config.SourceCommit, AcceptedObjects: status.AcceptedObjects}
	}
	switch {
	case errors.Is(err, rendered.ErrSourceUnconfigured):
		status.SkippedReason = err.Error()
		status.LastError = ""
		status.RetryAt = time.Time{}
		c.adoptionNeeded = false
	case errors.Is(err, rendered.ErrSourceInvalid) && !errors.Is(err, rendered.ErrSourceUnavailable):
		// Permanent for this source commit, but visible: no retry until the
		// commit changes.
		status.SkippedReason = ""
		status.LastError = err.Error()
		status.RetryAt = time.Time{}
		c.adoptionNeeded = false
	default:
		status.LastError = err.Error()
		status.Failures++
		delay := 2 * time.Second
		for step := 1; step < status.Failures && delay < time.Minute; step++ {
			delay *= 2
		}
		if delay > time.Minute {
			delay = time.Minute
		}
		status.RetryAt = now.Add(delay)
	}
	c.ownershipAdoption = status
}

// acceptanceSettleRetry spaces adoption passes that wait for an object to
// settle before its acceptance record is taken. The spacing doubles while
// the wait continues, up to acceptanceSettleRetryMax: an object may never
// settle (a declared bare Pod the scheduler cannot place), and every pass
// dry-runs all accepted objects.
const (
	acceptanceSettleRetry    = 5 * time.Second
	acceptanceSettleRetryMax = 2 * time.Minute
)

func (c *Controller) adoptOwnership(ctx context.Context, now time.Time, snapshot sdk.DetectionContext) error {
	if c.CanAct != nil && !c.CanAct() {
		return nil
	}
	if c.drift == nil || c.drift.Source == nil || c.drift.Client == nil {
		c.adoptionNeeded = false
		return nil
	}
	if c.drift.View == nil {
		return fmt.Errorf("drift read view is required for ownership adoption")
	}
	actionCtx, cancel, err := c.actionContext(ctx)
	if err != nil {
		return err
	}
	defer cancel()
	objects, renderErr := c.drift.Source.AcceptedManifests(actionCtx, c.config.SourceCommit, c.config.Namespace)
	if renderErr != nil && len(objects) == 0 {
		return fmt.Errorf("render accepted manifests for ownership adoption: %w", renderErr)
	}
	c.mu.Lock()
	status := c.ownershipAdoption
	c.mu.Unlock()
	status.FromManagers = append([]string(nil), status.FromManagers...)
	record := status.SourceCommit != c.config.SourceCommit || status.AdoptedObjects == 0
	if status.SourceCommit != c.config.SourceCommit {
		status = OwnershipAdoptionStatus{SourceCommit: c.config.SourceCommit, AcceptedObjects: status.AcceptedObjects}
	}
	// A retry pass follows a failed one. Objects it already adopted and
	// recorded are not dry-run again: one permanently refused object would
	// otherwise cost a sweep of every accepted object per retry.
	retrying := status.Failures > 0
	accepted := map[string]bool{}
	for _, key := range status.AcceptedObjects {
		accepted[key] = true
	}
	managerSet := map[string]bool{}
	for _, manager := range status.FromManagers {
		managerSet[manager] = true
	}
	type adoptionChange struct {
		ref    sdk.ObjectRef
		change ObjectChange
	}
	changes := make([]adoptionChange, 0)
	type candidate struct {
		ref        sdk.ObjectRef
		resource   acceptedResource
		source     *unstructured.Unstructured
		normalized *unstructured.Unstructured
		conflicts  int
		managers   []string
		record     string
	}
	pending := make([]candidate, 0)
	unsettled := false
	// An object the API server refuses (or that cannot be adopted) leaves
	// only that object without an acceptance point; the pass continues and
	// the failure is retried with the adoption backoff.
	objectFailures := []error{}
	unadopted := []string{}
	objectFailed := func(ref sdk.ObjectRef, err error) {
		objectFailures = append(objectFailures, &ObjectAdoptionError{Ref: ref, Err: err})
	}
	for _, source := range objects {
		ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
		// Identity-only declarations have no configuration fields to adopt.
		// Presence remains an inventory/source obligation without requiring an
		// otherwise unnecessary API write (for example a bare ServiceAccount).
		if len(flattenDriftPaths(source)) == 0 {
			continue
		}
		// Secret values are never read, so adoption cannot compare or claim
		// them; a declared Secret remains an inventory/source obligation.
		if isSecretRef(ref) {
			continue
		}
		if reason := adoptionNotAdopted(ref); reason != "" {
			unadopted = append(unadopted, acceptedObjectKey(ref)+": "+reason)
			continue
		}
		resource, err := resolveAcceptedResource(c.drift.Client, ref)
		if err != nil {
			objectFailed(ref, fmt.Errorf("unsupported accepted manifest %s/%s/%s: %w", ref.Kind, ref.Namespace, ref.Name, err))
			continue
		}
		if resource.namespaced && ref.Namespace != c.config.Namespace {
			objectFailed(ref, fmt.Errorf("accepted manifest %s/%s/%s is outside namespace %s", ref.Kind, ref.Namespace, ref.Name, c.config.Namespace))
			continue
		}
		// Shared cluster objects (CRDs, StorageClasses, webhooks, ...) are not
		// the app's to own: another tenant's edit must not become its drift.
		// Only the app's own RBAC grants are adopted at cluster scope.
		if !resource.namespaced && !inventoryClusterKind(ref.Kind) {
			continue
		}
		live, err := resource.get(actionCtx, ref)
		if err != nil && !apierrors.IsNotFound(err) {
			objectFailed(ref, fmt.Errorf("read live %s/%s for adoption: %w", ref.Kind, ref.Name, err))
			continue
		}
		if retrying && live != nil && adoptedAndRecorded(live, source) {
			accepted[acceptedObjectKey(ref)] = true
			continue
		}
		normalized, err := resource.apply(actionCtx, ref, source, true)
		if err != nil {
			if jobTemplateImmutable(ref, err) {
				// The deployer recreates a Job whose template changed; adoption
				// follows once the new Job exists, so this is not a failure.
				unadopted = append(unadopted, acceptedObjectKey(ref)+": Job template changed; the deployer recreates it")
				continue
			}
			objectFailed(ref, fmt.Errorf("dry run adoption of %s/%s: %w", ref.Kind, ref.Name, err))
			continue
		}
		declared := flattenDriftPaths(source)
		if live == nil || declaredFieldDifference(normalized, live, declared) {
			changes = append(changes, adoptionChange{ref: ref, change: ObjectChange{Old: normalized, New: live, Adoption: true}})
			continue
		}
		owned := ownedDriftPaths(live, "sdo-deployer")
		allOwned := true
		for path := range declared {
			if !ownsDriftPath(owned, path) {
				allOwned = false
				break
			}
		}
		if acceptanceRecordCurrent(live, source) {
			accepted[acceptedObjectKey(ref)] = true
		}
		recorded := acceptanceRecordBound(live, source)
		if allOwned && recorded {
			continue
		}
		// The record waits until controllers have caught up with the
		// object; until then its source comparison fails closed.
		record := ""
		lost := !recorded && recordLost(live, accepted[acceptedObjectKey(ref)])
		if !recorded {
			if !acceptanceSettled(live) {
				unsettled = true
			} else if encoded, fits, err := buildAcceptanceRecord(source, normalized, live, lostRecordDefaults(actionCtx, resource, ref, source, live, lost), lost); err != nil {
				objectFailed(ref, fmt.Errorf("record accepted paths of %s/%s: %w", ref.Kind, ref.Name, err))
				continue
			} else if fits {
				record = encoded
			}
		}
		if allOwned {
			if record != "" {
				pending = append(pending, candidate{ref: ref, resource: resource, normalized: normalized, record: record})
			}
			continue
		}
		conflicts, managers := adoptionConflicts(live, declared)
		if live.GetUID() == "" || live.GetResourceVersion() == "" {
			objectFailed(ref, fmt.Errorf("live %s/%s lacks UID or resourceVersion for conditional adoption", ref.Kind, ref.Name))
			continue
		}
		conditional := source.DeepCopy()
		conditional.SetUID(live.GetUID())
		conditional.SetResourceVersion(live.GetResourceVersion())
		pending = append(pending, candidate{ref: ref, resource: resource, source: conditional, normalized: normalized, conflicts: conflicts, managers: managers, record: record})
	}
	// Classifying or persisting one object's pre-adoption difference fails
	// only that object: classification re-runs its source comparison, which
	// can fail like the scan's dry run.
	persistFailures := []error{}
	persistDifferences := func(changes []adoptionChange, view program.ReadView) {
		classifier := DriftClassifier{}
		events := make([]sdk.DriftEvent, 0, len(changes))
		for _, item := range changes {
			event, err := classifier.Classify(actionCtx, item.change, view, now)
			if err != nil {
				objectFailed(item.ref, fmt.Errorf("classify pre-adoption difference: %w", err))
				continue
			}
			if event == nil {
				objectFailed(item.ref, fmt.Errorf("pre-adoption difference was not classified"))
				continue
			}
			events = append(events, *event)
		}
		if len(events) == 0 {
			return
		}
		for _, event := range events {
			c.mu.Lock()
			c.driftEvents = append(c.driftEvents, event)
			if len(c.driftEvents) > 40 {
				c.driftEvents = append([]sdk.DriftEvent(nil), c.driftEvents[len(c.driftEvents)-40:]...)
			}
			c.mu.Unlock()
			if event.Classification == sdk.DriftUnexplained {
				c.queueUnexplainedFinding(now, event)
			}
		}
		if err := c.PersistState(actionCtx); err != nil {
			persistFailures = append(persistFailures, fmt.Errorf("persist pre-adoption drift: %w", err))
		}
	}
	persistDifferences(changes, c.drift.View(snapshot))
	// A failed record write leaves only that object without an acceptance
	// point (its comparison fails closed); the pass continues and the
	// failure is retried with the adoption backoff.
	recordFailures := []error{}
	writeRecord := func(item candidate) {
		if err := item.resource.recordAcceptance(actionCtx, item.ref, item.record); err != nil {
			recordFailures = append(recordFailures, &AcceptanceRecordError{Ref: item.ref, Err: err})
			return
		}
		accepted[acceptedObjectKey(item.ref)] = true
	}
	for _, item := range pending {
		if item.source == nil {
			writeRecord(item)
			continue
		}
		if _, err := applyAdoption(actionCtx, item.resource, item.ref, item.source, item.normalized); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				live, readErr := item.resource.get(actionCtx, item.ref)
				if readErr != nil && !apierrors.IsNotFound(readErr) {
					objectFailed(item.ref, fmt.Errorf("re-read %s/%s after adoption conflict: %w", item.ref.Kind, item.ref.Name, readErr))
					continue
				}
				if live == nil || declaredFieldDifference(item.normalized, live, flattenDriftPaths(item.source)) {
					// The informer snapshot may predate the conflicting write.
					// Use the accepted projection and the direct API read for
					// admission, without attempting another write.
					expected, marshalErr := json.Marshal(comparableFields(item.normalized.Object))
					if marshalErr != nil {
						objectFailed(item.ref, fmt.Errorf("encode accepted projection of %s/%s: %w", item.ref.Kind, item.ref.Name, marshalErr))
						continue
					}
					view := adoptionConflictView{ReadView: c.drift.View(snapshot), ref: item.ref, diff: program.Diff{Changed: true, Text: "accepted: " + string(expected)}}
					persistDifferences([]adoptionChange{{ref: item.ref, change: ObjectChange{Old: item.normalized, New: live, Adoption: true}}}, view)
				}
			}
			objectFailed(item.ref, fmt.Errorf("adopt %s/%s: %w", item.ref.Kind, item.ref.Name, err))
			continue
		}
		if item.record != "" {
			writeRecord(item)
		}
		if record {
			status.AdoptedObjects++
			status.ConflictsTakenOver += item.conflicts
			for _, manager := range item.managers {
				managerSet[manager] = true
			}
		}
	}
	status.FromManagers = status.FromManagers[:0]
	for manager := range managerSet {
		status.FromManagers = append(status.FromManagers, manager)
	}
	sort.Strings(status.FromManagers)
	status.AcceptedObjects = status.AcceptedObjects[:0]
	for key := range accepted {
		status.AcceptedObjects = append(status.AcceptedObjects, key)
	}
	sort.Strings(status.AcceptedObjects)
	recordErr := errors.Join(append(append(objectFailures, recordFailures...), persistFailures...)...)
	if recordErr == nil {
		status.LastError = ""
		status.Failures = 0
	}
	status.SkippedReason = ""
	status.RetryAt = time.Time{}
	sort.Strings(unadopted)
	status.UnadoptedObjects = unadopted
	c.mu.Lock()
	c.ownershipAdoption = status
	c.mu.Unlock()
	if renderErr != nil {
		return errors.Join(fmt.Errorf("render accepted manifests for ownership adoption (partial coverage): %w", renderErr), recordErr)
	}
	if recordErr != nil {
		return recordErr
	}
	if unsettled {
		delay := acceptanceSettleRetry
		for step := 0; step < c.acceptanceSettleWaits && delay < acceptanceSettleRetryMax; step++ {
			delay *= 2
		}
		c.acceptanceSettleWaits++
		c.mu.Lock()
		c.ownershipAdoption.RetryAt = now.Add(min(delay, acceptanceSettleRetryMax))
		c.mu.Unlock()
		return nil
	}
	c.acceptanceSettleWaits = 0
	c.adoptionNeeded = false
	return nil
}

// adoptedAndRecorded reports whether live already has every declared field
// owned by the deployer and a current acceptance record bound to its UID:
// adoption has nothing left to do for it.
// adoptionConflictRetries bounds how often one conditional adoption write is
// remade on a fresh resourceVersion within a pass.
const adoptionConflictRetries = 5

// applyAdoption applies the conditional source. A conflict usually means only
// that another controller wrote the object (status, observed generation) since
// the scan read it, so the write is remade on the fresh resourceVersion for as
// long as the declared fields still match the accepted projection. Leaving it
// to the adoption backoff left the object without an acceptance record, and a
// repeat incident in that window met "acceptance record is missing". A
// conflict over a declared field is a real change: it is returned, to be
// classified as drift and never overwritten.
func applyAdoption(ctx context.Context, resource acceptedResource, ref sdk.ObjectRef, source, normalized *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	declared := flattenDriftPaths(source)
	// The deployer's own apply stamped the live object and its pod template.
	// The accepted source is unstamped, and a server-side apply under the same
	// manager that omits an owned field deletes it: without the carried stamp
	// every adopted workload would roll once for a change to nothing but
	// SDO's bookkeeping.
	if live, err := resource.get(ctx, ref); err == nil {
		source = carryOwnedOmissions(carrySourceTreeStamp(source, live), live)
		// Adoption claims ownership; it never changes what the object is. The
		// server's dry run says whether this apply would move
		// metadata.generation (a spec field removed or changed: fields inside
		// list items are not carried above). Such a write is skipped and the
		// object left as the deployer applied it.
		if preview, err := resource.apply(ctx, ref, source, true); err == nil && live.GetGeneration() > 0 && preview.GetGeneration() > live.GetGeneration() {
			slog.Warn("adoption_skipped_spec_change", "kind", ref.Kind, "name", ref.Name, "live_generation", live.GetGeneration(), "would_be_generation", preview.GetGeneration())
			return live, nil
		}
	}
	for attempt := 1; ; attempt++ {
		applied, err := resource.apply(ctx, ref, source, false)
		if err == nil || !apierrors.IsConflict(err) || attempt >= adoptionConflictRetries {
			return applied, err
		}
		live, readErr := resource.get(ctx, ref)
		if readErr != nil || live == nil || declaredFieldDifference(normalized, live, declared) ||
			live.GetUID() == "" || live.GetUID() != source.GetUID() {
			return nil, err
		}
		source = carryOwnedOmissions(carrySourceTreeStamp(source, live), live)
		source.SetResourceVersion(live.GetResourceVersion())
	}
}

// sourceTreeAnnotation is the deployer's effective-tree stamp.
const sourceTreeAnnotation = "sdo.dev/source-tree"

// carrySourceTreeStamp returns a copy of source that keeps live's
// sdo.dev/source-tree stamp on the object and on its pod template, so applying
// it as the deployer leaves the stamp, and with it the pod template, unchanged.
func carrySourceTreeStamp(source, live *unstructured.Unstructured) *unstructured.Unstructured {
	out := source.DeepCopy()
	for _, fields := range [][]string{{"metadata", "annotations"}, {"spec", "template", "metadata", "annotations"}} {
		stamp, found, _ := unstructured.NestedString(live.Object, append(append([]string(nil), fields...), sourceTreeAnnotation)...)
		if !found || stamp == "" {
			continue
		}
		if _, declared, _ := unstructured.NestedString(out.Object, append(append([]string(nil), fields...), sourceTreeAnnotation)...); declared {
			continue
		}
		_ = unstructured.SetNestedField(out.Object, stamp, append(append([]string(nil), fields...), sourceTreeAnnotation)...)
	}
	return out
}

// carryOwnedOmissions returns a copy of source that also holds every plain
// (map-path, no list item) field the deployer owns on live and source omits,
// with live's value. A server-side apply under the same manager that omits an
// owned field deletes it, so a non-default value the deployer applied (and the
// accepted render does not carry) would change the spec: adoption would bump
// metadata.generation for an accepted state that did not change. Fields inside
// list items are left to the apply (their position is not stable between the
// two renders).
func carryOwnedOmissions(source, live *unstructured.Unstructured) *unstructured.Unstructured {
	out := source.DeepCopy()
	for _, entry := range live.GetManagedFields() {
		if entry.Manager != "sdo-deployer" || entry.Operation != metav1.ManagedFieldsOperationApply || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(entry.FieldsV1.Raw, &fields) != nil {
			continue
		}
		carryOwnedFields(fields, nil, out.Object, live.Object)
	}
	return out
}

func carryOwnedFields(fields map[string]any, path []string, into, from map[string]any) {
	for key, raw := range fields {
		name, plain := strings.CutPrefix(key, "f:")
		if !plain {
			continue
		}
		child, _ := raw.(map[string]any)
		next := append(append([]string(nil), path...), name)
		if len(path) == 0 && name != "spec" && name != "metadata" {
			continue
		}
		if len(path) == 1 && path[0] == "metadata" && name != "annotations" && name != "labels" {
			continue
		}
		if hasItemKeys(child) {
			continue
		}
		if len(child) > 0 {
			carryOwnedFields(child, next, into, from)
			continue
		}
		if _, found, _ := unstructured.NestedFieldNoCopy(into, next...); found {
			continue
		}
		if value, found, _ := unstructured.NestedFieldCopy(from, next...); found {
			_ = unstructured.SetNestedField(into, value, next...)
		}
	}
}

// hasItemKeys reports whether a managed-fields node selects list items.
func hasItemKeys(node map[string]any) bool {
	for key := range node {
		if strings.HasPrefix(key, "k:") || strings.HasPrefix(key, "v:") || strings.HasPrefix(key, "i:") {
			return true
		}
	}
	return false
}

func adoptedAndRecorded(live, source *unstructured.Unstructured) bool {
	if !acceptanceRecordBound(live, source) {
		return false
	}
	owned := ownedDriftPaths(live, "sdo-deployer")
	for path := range flattenDriftPaths(source) {
		if !ownsDriftPath(owned, path) {
			return false
		}
	}
	return true
}

// recordLost reports whether live, which needs a new record, lost the one
// SDO took earlier: it carries a record taken on another object instance, or
// it carries none although SDO accepted it before. A record of this instance
// (taken against older source, or before UID binding) is not lost: its
// additions carry forward.
func recordLost(live *unstructured.Unstructured, acceptedBefore bool) bool {
	if record, ok := readAcceptanceRecord(live); ok {
		return record.UID != "" && record.UID != string(live.GetUID())
	}
	return acceptedBefore
}

// lostRecordDefaults renders accepted source's server defaults for an object
// that lost its record with no earlier copy to carry. Without them every
// default the recreating writer owns would stay an addition forever.
func lostRecordDefaults(ctx context.Context, resource acceptedResource, ref sdk.ObjectRef, source, live *unstructured.Unstructured, lost bool) *unstructured.Unstructured {
	if _, carried := readAcceptanceRecord(live); !lost || carried {
		return nil
	}
	rendered, err := resource.renderDefaults(ctx, ref, source)
	if err != nil {
		return nil
	}
	return rendered
}

func declaredFieldDifference(expected, observed *unstructured.Unstructured, declared map[string]bool) bool {
	left := flattenDriftObject(expected)
	right := flattenDriftObject(observed)
	for path := range declared {
		if !reflect.DeepEqual(left[path], right[path]) {
			return true
		}
	}
	return false
}

func adoptionConflicts(live *unstructured.Unstructured, declared map[string]bool) (int, []string) {
	count := 0
	managers := map[string]bool{}
	for _, entry := range live.GetManagedFields() {
		if entry.Manager == "sdo-deployer" || entry.FieldsV1 == nil {
			continue
		}
		var fields map[string]any
		if json.Unmarshal(entry.FieldsV1.Raw, &fields) != nil {
			continue
		}
		owned := map[string]bool{}
		walkOwnedFields(fields, live.Object, "", owned)
		for path := range declared {
			if ownsDriftPath(owned, path) {
				count++
				managers[entry.Manager] = true
			}
		}
	}
	result := make([]string, 0, len(managers))
	for manager := range managers {
		result = append(result, manager)
	}
	return count, result
}

// adoptionConflictView binds source comparison to the API state already read
// after a conditional write failed, rather than a potentially stale informer.
type adoptionConflictView struct {
	program.ReadView
	ref  sdk.ObjectRef
	diff program.Diff
}

// UnderlyingReadView exposes the wrapped view to the memory host edge.
func (v adoptionConflictView) UnderlyingReadView() program.ReadView { return v.ReadView }

func (v adoptionConflictView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if ref == v.ref {
		return v.diff, nil
	}
	return v.ReadView.DiffAgainstSource(ref)
}

func (v adoptionConflictView) ScopedDeployments(namespace string) ([]appsv1.Deployment, error) {
	return sdk.ScopedSource(v.ReadView).ScopedDeployments(namespace)
}

func (v adoptionConflictView) ScopedServices(namespace string) ([]corev1.Service, error) {
	return sdk.ScopedSource(v.ReadView).ScopedServices(namespace)
}

func (v adoptionConflictView) ScopedEndpoints(namespace string) ([]corev1.Endpoints, error) {
	return sdk.ScopedSource(v.ReadView).ScopedEndpoints(namespace)
}

func (v adoptionConflictView) MutatingWebhookConfigurations() ([]admissionv1.MutatingWebhookConfiguration, error) {
	return sdk.ScopedSource(v.ReadView).MutatingWebhookConfigurations()
}

func (v adoptionConflictView) ValidatingWebhookConfigurations() ([]admissionv1.ValidatingWebhookConfiguration, error) {
	return sdk.ScopedSource(v.ReadView).ValidatingWebhookConfigurations()
}

func (v adoptionConflictView) ObservationStatuses() []sdk.ObservationStatus {
	return sdk.ScopedSource(v.ReadView).ObservationStatuses()
}
func (v adoptionConflictView) ObservationGeneration() string {
	return sdk.ScopedSource(v.ReadView).ObservationGeneration()
}

func (v adoptionConflictView) ObservedAt() time.Time {
	if clock, ok := v.ReadView.(sdk.ObservationClock); ok {
		return clock.ObservedAt()
	}
	return time.Time{}
}

func (v adoptionConflictView) TrafficWindow(name string) (traffic.Window, bool) {
	if source, ok := v.ReadView.(traffic.Source); ok {
		return source.TrafficWindow(name)
	}
	return traffic.Window{}, false
}

func (v adoptionConflictView) ServiceReachability() (servicehealth.ReachabilityWindow, bool) {
	if source, ok := v.ReadView.(servicehealth.ReachabilitySource); ok {
		return source.ServiceReachability()
	}
	return servicehealth.ReachabilityWindow{}, false
}

func (v adoptionConflictView) MutatingWebhookConfiguration(name string) (*admissionv1.MutatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).MutatingWebhookConfiguration(name)
	return result, err
}

func (v adoptionConflictView) ValidatingWebhookConfiguration(name string) (*admissionv1.ValidatingWebhookConfiguration, error) {
	result, err := sdk.ScopedSource(v.ReadView).ValidatingWebhookConfiguration(name)
	return result, err
}

// AcceptanceRecordError reports one object whose acceptance record could not
// be written.
type AcceptanceRecordError struct {
	Ref sdk.ObjectRef
	Err error
}

func (e *AcceptanceRecordError) Error() string {
	return fmt.Sprintf("record acceptance of %s/%s: %v", e.Ref.Kind, e.Ref.Name, e.Err)
}

func (e *AcceptanceRecordError) Unwrap() error { return e.Err }

// ObjectAdoptionError reports one accepted object that a pass could not read,
// dry-run or adopt. The pass continues with the other objects, so one refused
// object never leaves the rest without an acceptance record.
type ObjectAdoptionError struct {
	Ref sdk.ObjectRef
	Err error
}

func (e *ObjectAdoptionError) Error() string { return e.Err.Error() }

func (e *ObjectAdoptionError) Unwrap() error { return e.Err }

// jobTemplateImmutable reports the API server refusing a Job's changed
// spec.template (immutable): the deployer's recreate policy owns that change.
func jobTemplateImmutable(ref sdk.ObjectRef, err error) bool {
	return ref.Kind == "Job" && apierrors.IsInvalid(err) && strings.Contains(err.Error(), "immutable")
}
