package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"reflect"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sort"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"

	"sdo.dev/operator/deployment"
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
}

func (c *Controller) recordAdoptionError(now time.Time, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	status := c.ownershipAdoption
	if status.SourceCommit != c.config.SourceCommit {
		status = OwnershipAdoptionStatus{SourceCommit: c.config.SourceCommit}
	}
	if errors.Is(err, deployment.ErrManifestSelectionUnavailable) {
		status.SkippedReason = err.Error()
		status.LastError = ""
		status.RetryAt = time.Time{}
		c.adoptionNeeded = false
	} else {
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

func supportsAcceptedSource(ref sdk.ObjectRef) bool {
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service", "v1/ConfigMap", "v1/Pod", "apps/v1/Deployment", "apps/v1/ReplicaSet", "v1/Endpoints", "discovery.k8s.io/v1/EndpointSlice", "networking.k8s.io/v1/NetworkPolicy":
		return true
	default:
		return false
	}
}

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
		status = OwnershipAdoptionStatus{SourceCommit: c.config.SourceCommit}
	}
	managerSet := map[string]bool{}
	for _, manager := range status.FromManagers {
		managerSet[manager] = true
	}
	changes := make([]ObjectChange, 0)
	type candidate struct {
		ref        sdk.ObjectRef
		source     *unstructured.Unstructured
		normalized *unstructured.Unstructured
		conflicts  int
		managers   []string
	}
	pending := make([]candidate, 0)
	for _, source := range objects {
		ref := sdk.ObjectRef{APIVersion: source.GetAPIVersion(), Kind: source.GetKind(), Namespace: source.GetNamespace(), Name: source.GetName()}
		if !supportsAcceptedSource(ref) || ref.Namespace != c.config.Namespace {
			return fmt.Errorf("unsupported accepted manifest %s/%s/%s", ref.Kind, ref.Namespace, ref.Name)
		}
		live, err := getAdoptionObject(actionCtx, c.drift.Client, ref)
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("read live %s/%s for adoption: %w", ref.Kind, ref.Name, err)
		}
		normalized, err := dryRunAcceptedSource(actionCtx, c.drift.Client, ref, source)
		if err != nil {
			return fmt.Errorf("dry run adoption of %s/%s: %w", ref.Kind, ref.Name, err)
		}
		declared := flattenDriftPaths(source)
		if live == nil || declaredFieldDifference(normalized, live, declared) {
			changes = append(changes, ObjectChange{Old: normalized, New: live, Adoption: true})
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
		if allOwned {
			continue
		}
		conflicts, managers := adoptionConflicts(live, declared)
		if live.GetUID() == "" || live.GetResourceVersion() == "" {
			return fmt.Errorf("live %s/%s lacks UID or resourceVersion for conditional adoption", ref.Kind, ref.Name)
		}
		conditional := source.DeepCopy()
		conditional.SetUID(live.GetUID())
		conditional.SetResourceVersion(live.GetResourceVersion())
		pending = append(pending, candidate{ref: ref, source: conditional, normalized: normalized, conflicts: conflicts, managers: managers})
	}
	persistDifferences := func(changes []ObjectChange, view program.ReadView) error {
		if len(changes) == 0 {
			return nil
		}
		classifier := DriftClassifier{Programs: c.drift.Programs, Deadline: c.drift.Deadline}
		events := make([]sdk.DriftEvent, 0, len(changes))
		for _, change := range changes {
			event, _, err := classifier.Classify(actionCtx, change, view, now)
			if err != nil {
				return fmt.Errorf("classify pre-adoption difference: %w", err)
			}
			if event == nil {
				return fmt.Errorf("pre-adoption difference was not classified")
			}
			events = append(events, *event)
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
			return fmt.Errorf("persist pre-adoption drift: %w", err)
		}
		return nil
	}
	if err := persistDifferences(changes, c.drift.View(snapshot)); err != nil {
		return err
	}
	for _, item := range pending {
		if _, err := applyAcceptedSource(actionCtx, c.drift.Client, item.ref, item.source, false); err != nil {
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				live, readErr := getAdoptionObject(actionCtx, c.drift.Client, item.ref)
				if readErr != nil && !apierrors.IsNotFound(readErr) {
					return fmt.Errorf("re-read %s/%s after adoption conflict: %w", item.ref.Kind, item.ref.Name, readErr)
				}
				if live == nil || declaredFieldDifference(item.normalized, live, flattenDriftPaths(item.source)) {
					// The informer snapshot may predate the conflicting write.
					// Use the accepted projection and the direct API read for
					// admission, without attempting another write.
					expected, marshalErr := json.Marshal(comparableFields(item.normalized.Object))
					if marshalErr != nil {
						return marshalErr
					}
					view := adoptionConflictView{ReadView: c.drift.View(snapshot), ref: item.ref, diff: program.Diff{Changed: true, Text: "accepted: " + string(expected)}}
					if classifyErr := persistDifferences([]ObjectChange{{Old: item.normalized, New: live, Adoption: true}}, view); classifyErr != nil {
						return classifyErr
					}
				}
			}
			return fmt.Errorf("adopt %s/%s: %w", item.ref.Kind, item.ref.Name, err)
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
	status.LastError = ""
	status.SkippedReason = ""
	status.RetryAt = time.Time{}
	status.Failures = 0
	c.mu.Lock()
	c.ownershipAdoption = status
	c.mu.Unlock()
	if renderErr != nil {
		return fmt.Errorf("render accepted manifests for ownership adoption (partial coverage): %w", renderErr)
	}
	c.adoptionNeeded = false
	return nil
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

func getAdoptionObject(ctx context.Context, client kubernetes.Interface, ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	var object any
	var err error
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/Service":
		object, err = client.CoreV1().Services(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "v1/ConfigMap":
		object, err = client.CoreV1().ConfigMaps(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "v1/Pod":
		object, err = client.CoreV1().Pods(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "apps/v1/Deployment":
		object, err = client.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "apps/v1/ReplicaSet":
		object, err = client.AppsV1().ReplicaSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "v1/Endpoints":
		object, err = client.CoreV1().Endpoints(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "discovery.k8s.io/v1/EndpointSlice":
		object, err = client.DiscoveryV1().EndpointSlices(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	case "networking.k8s.io/v1/NetworkPolicy":
		object, err = client.NetworkingV1().NetworkPolicies(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	default:
		return nil, fmt.Errorf("unsupported adoption object %s/%s", ref.APIVersion, ref.Kind)
	}
	if err != nil {
		return nil, err
	}
	content, err := kruntime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, err
	}
	result := &unstructured.Unstructured{Object: content}
	result.SetAPIVersion(ref.APIVersion)
	result.SetKind(ref.Kind)
	return result, nil
}

// adoptionConflictView binds source comparison to the API state already read
// after a conditional write failed, rather than a potentially stale informer.
type adoptionConflictView struct {
	program.ReadView
	ref  sdk.ObjectRef
	diff program.Diff
}

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
