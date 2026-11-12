package sdktest

import (
	"encoding/json"
	"errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"regexp"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
	"sdo.dev/operator/sdk/vantage"
)

// MaxCapturedSnapshotBytes is the encoded size a captured snapshot may reach.
// It is the smallest limit on a snapshot's path: fastloop checkpoints record
// corpus files up to 4 MiB, the memory validator accepts corpus files up to
// 8 MiB, and the healthy-baseline writer's 8 MiB bounds a whole set. A capture
// that fits here is recordable and publishable. Larger namespaces are trimmed
// to it (trimToBudget), not rejected.
const MaxCapturedSnapshotBytes = 4 << 20

var sensitiveEnvironmentName = regexp.MustCompile(`(?i)SECRET|TOKEN|PASSWORD|KEY`)

// CaptureSnapshot copies the namespace-local detection view for corpus replay.
// ConfigMap data and binaryData retain key names only; live read views are unchanged.
func CaptureSnapshot(view sdk.DetectionContext, namespace string, refs ...sdk.ObjectRef) (Snapshot, error) {
	if view == nil || namespace == "" || view.Namespace() != namespace {
		return Snapshot{}, errors.New("capture requires a matching application namespace")
	}
	s := Snapshot{NamespaceName: namespace, ObjectSnapshotVersion: 1}
	if result, ok := view.PodAdmission(); ok {
		s.Admission = &result
	}
	if recorder, ok := view.(interface{ RecordedReads() []PrimitiveCall }); ok {
		s.captureReadInputs(recorder.RecordedReads())
	}
	if original, ok := view.(Snapshot); ok {
		s.Traffic = original.Traffic
		s.Metrics = original.Metrics
		s.PodReads = original.PodReads
		s.Vantage = original.Vantage
		s.Alerts = original.Alerts
		s.Readings = original.Readings
		s.Reachability = original.Reachability
	} else {
		// A live composed view: keep the observation windows detectors read,
		// so replay of the capture sees what the live evaluation saw.
		if set, ok := view.(traffic.WindowSet); ok {
			s.Traffic = set.TrafficWindows()
		}
		if set, ok := view.(metrics.ResultSet); ok {
			s.Metrics = set.MetricResults()
		}
		if set, ok := view.(podread.ResultSet); ok {
			s.PodReads = set.PodReadResults()
		}
		if set, ok := view.(vantage.ResultSet); ok {
			s.Vantage = set.VantageResults()
		}
		if source, ok := view.(metrics.AlertSource); ok {
			if alerts, ok := source.AlertsResult(); ok {
				s.Alerts = &alerts
			}
		}
		if source, ok := view.(servicehealth.ReachabilitySource); ok {
			if window, ok := source.ServiceReachability(); ok {
				s.Reachability = &window
			}
		}
	}
	if clock, ok := view.(sdk.ObservationClock); ok && !clock.ObservedAt().IsZero() {
		observed := clock.ObservedAt().UTC()
		s.ObservedAtTime = &observed
	}
	if policy, ok := view.(sdk.SelfHealPolicy); ok && policy.SelfHealHold() > 0 {
		hold := policy.SelfHealHold()
		s.SelfHealHoldTime = &hold
	}
	if scoped, ok := view.(sdk.ScopedObservationSource); ok {
		snapshot, scopedRefs, err := CaptureScoped(scoped)
		if err != nil {
			return Snapshot{}, err
		}
		s.ScopedSnapshot = snapshot
		refs = append(refs, scopedRefs...)
	}
	objects := sdk.TypedObjects(view)
	if source, ok := view.(sdk.ObjectCaptureSource); ok {
		var err error
		objects, err = source.CaptureObjects(refs)
		if err != nil {
			return Snapshot{}, err
		}
	}
	seen := map[sdk.ObjectRef]bool{}
	var local []unstructured.Unstructured
	for _, o := range objects {
		if o.GetKind() == "Secret" || seen[sdk.ObjectIdentity(o)] {
			continue
		}
		seen[sdk.ObjectIdentity(o)] = true
		raw, err := json.Marshal(&o)
		if err != nil {
			return Snapshot{}, err
		}
		projected, err := ProjectCapturedObject(raw)
		if err != nil {
			return Snapshot{}, err
		}
		var object unstructured.Unstructured
		if err = json.Unmarshal(projected, &object); err != nil {
			return Snapshot{}, err
		}
		s.Objects = append(s.Objects, object)
		if object.GetNamespace() == namespace {
			local = append(local, object)
		}
	}
	sdk.SortObjects(s.Objects)
	if err := sdk.DecodeTypedObjects(&s, local); err != nil {
		return Snapshot{}, err
	}
	for _, object := range view.Nodes() {
		copy := *object.DeepCopy()
		compactMeta(&copy.ObjectMeta)
		s.NodeList = append(s.NodeList, copy)
	}
	for _, object := range view.Secrets() {
		if object.Namespace == namespace {
			s.SecretList = append(s.SecretList, *object.DeepCopy())
		}
	}
	if err := s.trimToBudget(MaxCapturedSnapshotBytes); err != nil {
		return Snapshot{}, err
	}
	return s, nil
}

func compactMeta(meta *metav1.ObjectMeta) {
	meta.ManagedFields = nil
	meta.ResourceVersion = ""
	meta.UID = ""
	meta.Generation = 0
	delete(meta.Annotations, "kubectl.kubernetes.io/last-applied-configuration")
}

// CaptureScoped freezes a scoped observation source into its credential-free
// replay form and lists the observed targets as object references.
func CaptureScoped(scoped sdk.ScopedObservationSource) (sdk.ScopedSnapshot, []sdk.ObjectRef, error) {
	var s sdk.ScopedSnapshot
	var refs []sdk.ObjectRef
	s.Version = 1
	s.Generation = scoped.ObservationGeneration()
	for _, status := range scoped.ObservationStatuses() {
		s.Scope.Version = 1
		s.Scope.Targets = append(s.Scope.Targets, status.Target)
		s.Statuses = append(s.Statuses, status)
		if status.State != sdk.ObservationObserved {
			continue
		}
		t := status.Target
		refs = append(refs, sdk.ObjectRef{APIVersion: t.APIVersion, Kind: t.Kind, Namespace: t.Namespace, Name: t.Name})
		switch t.Kind {
		case "MutatingWebhookConfiguration":
			object, err := scoped.MutatingWebhookConfiguration(t.Name)
			if err != nil {
				return sdk.ScopedSnapshot{}, nil, err
			}
			if object != nil {
				s.Mutating = append(s.Mutating, *object)
			}
		case "ValidatingWebhookConfiguration":
			object, err := scoped.ValidatingWebhookConfiguration(t.Name)
			if err != nil {
				return sdk.ScopedSnapshot{}, nil, err
			}
			if object != nil {
				s.Validating = append(s.Validating, *object)
			}
		case "Deployment":
			objects, err := scoped.ScopedDeployments(t.Namespace)
			if err != nil {
				return sdk.ScopedSnapshot{}, nil, err
			}
			s.Deployments = append(s.Deployments, objects...)
		case "Service":
			objects, err := scoped.ScopedServices(t.Namespace)
			if err != nil {
				return sdk.ScopedSnapshot{}, nil, err
			}
			s.Services = append(s.Services, objects...)
		case "Endpoints":
			objects, err := scoped.ScopedEndpoints(t.Namespace)
			if err != nil {
				return sdk.ScopedSnapshot{}, nil, err
			}
			s.Endpoints = append(s.Endpoints, objects...)
		}
	}
	return s, refs, nil
}
