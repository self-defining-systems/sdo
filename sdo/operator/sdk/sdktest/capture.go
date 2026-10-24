package sdktest

import (
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"regexp"

	"sdo.dev/operator/sdk"
)

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
		s.Alerts = original.Alerts
		s.Readings = original.Readings
		s.Reachability = original.Reachability
	}
	if clock, ok := view.(sdk.ObservationClock); ok && !clock.ObservedAt().IsZero() {
		observed := clock.ObservedAt().UTC()
		s.ObservedAtTime = &observed
	}
	if scoped, ok := view.(sdk.ScopedObservationSource); ok {
		s.ScopedSnapshot.Version = 1
		s.ScopedSnapshot.Generation = scoped.ObservationGeneration()
		for _, status := range scoped.ObservationStatuses() {
			s.ScopedSnapshot.Scope.Version = 1
			s.ScopedSnapshot.Scope.Targets = append(s.ScopedSnapshot.Scope.Targets, status.Target)
			s.ScopedSnapshot.Statuses = append(s.ScopedSnapshot.Statuses, status)
			if status.State != sdk.ObservationObserved {
				continue
			}
			t := status.Target
			refs = append(refs, sdk.ObjectRef{APIVersion: t.APIVersion, Kind: t.Kind, Namespace: t.Namespace, Name: t.Name})
			switch t.Kind {
			case "MutatingWebhookConfiguration":
				object, err := scoped.MutatingWebhookConfiguration(t.Name)
				if err != nil {
					return Snapshot{}, err
				}
				if object != nil {
					s.ScopedSnapshot.Mutating = append(s.ScopedSnapshot.Mutating, *object)
				}
			case "ValidatingWebhookConfiguration":
				object, err := scoped.ValidatingWebhookConfiguration(t.Name)
				if err != nil {
					return Snapshot{}, err
				}
				if object != nil {
					s.ScopedSnapshot.Validating = append(s.ScopedSnapshot.Validating, *object)
				}
			case "Deployment":
				objects, err := scoped.ScopedDeployments(t.Namespace)
				if err != nil {
					return Snapshot{}, err
				}
				s.ScopedSnapshot.Deployments = append(s.ScopedSnapshot.Deployments, objects...)
			case "Service":
				objects, err := scoped.ScopedServices(t.Namespace)
				if err != nil {
					return Snapshot{}, err
				}
				s.ScopedSnapshot.Services = append(s.ScopedSnapshot.Services, objects...)
			case "Endpoints":
				objects, err := scoped.ScopedEndpoints(t.Namespace)
				if err != nil {
					return Snapshot{}, err
				}
				s.ScopedSnapshot.Endpoints = append(s.ScopedSnapshot.Endpoints, objects...)
			}
		}

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
	data, err := json.Marshal(s)
	if err != nil {
		return Snapshot{}, fmt.Errorf("encode captured snapshot: %w", err)
	}
	if len(data) > MaxCapturedSnapshotBytes {
		return Snapshot{}, fmt.Errorf("captured snapshot exceeds %d bytes", MaxCapturedSnapshotBytes)
	}
	return s, nil
}
