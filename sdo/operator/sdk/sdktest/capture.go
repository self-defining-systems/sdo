package sdktest

import (
	"encoding/json"
	"errors"
	"fmt"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"regexp"

<<<<<<< HEAD
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
=======
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
<<<<<<< HEAD
	for _, object := range view.ConfigMaps() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.ConfigMapList = append(s.ConfigMapList, copy)
		}
	}
	for _, object := range view.Services() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.ServiceList = append(s.ServiceList, copy)
		}
	}
	for _, object := range view.Pods() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		compactMeta(&copy.ObjectMeta)
		redactPodSpec(&copy.Spec)
		s.PodList = append(s.PodList, copy)
	}
	for _, object := range view.Deployments() {
		if object.Namespace != namespace {
			continue
		}
		copy := *object.DeepCopy()
		compactMeta(&copy.ObjectMeta)
		compactMeta(&copy.Spec.Template.ObjectMeta)
		redactPodSpec(&copy.Spec.Template.Spec)
		s.DeploymentList = append(s.DeploymentList, copy)
	}
	for _, object := range view.StatefulSets() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			compactMeta(&copy.Spec.Template.ObjectMeta)
			redactPodSpec(&copy.Spec.Template.Spec)
			s.StatefulSetList = append(s.StatefulSetList, copy)
		}
	}
	for _, object := range view.HorizontalPodAutoscalers() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.HPAList = append(s.HPAList, copy)
=======
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
>>>>>>> origin/vic-lsh/dev/sdo-improve
		}

	}
<<<<<<< HEAD
	for _, object := range view.PodDisruptionBudgets() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.PDBList = append(s.PDBList, copy)
=======
	objects := sdk.TypedObjects(view)
	if source, ok := view.(sdk.ObjectCaptureSource); ok {
		var err error
		objects, err = source.CaptureObjects(refs)
		if err != nil {
			return Snapshot{}, err
>>>>>>> origin/vic-lsh/dev/sdo-improve
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
	for _, object := range view.DaemonSets() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			compactMeta(&copy.Spec.Template.ObjectMeta)
			redactPodSpec(&copy.Spec.Template.Spec)
			s.DaemonSetList = append(s.DaemonSetList, copy)
		}
	}
	for _, object := range view.Jobs() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			compactMeta(&copy.Spec.Template.ObjectMeta)
			redactPodSpec(&copy.Spec.Template.Spec)
			s.JobList = append(s.JobList, copy)
		}
	}
	for _, object := range view.CronJobs() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			compactMeta(&copy.Spec.JobTemplate.ObjectMeta)
			compactMeta(&copy.Spec.JobTemplate.Spec.Template.ObjectMeta)
			redactPodSpec(&copy.Spec.JobTemplate.Spec.Template.Spec)
			s.CronJobList = append(s.CronJobList, copy)
		}
	}
	for _, object := range view.PersistentVolumeClaims() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.PVCList = append(s.PVCList, copy)
		}
	}
	for _, object := range view.ServiceAccounts() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.ServiceAccountList = append(s.ServiceAccountList, copy)
		}
	}
	for _, object := range view.Ingresses() {
		if object.Namespace == namespace {
			copy := *object.DeepCopy()
			compactMeta(&copy.ObjectMeta)
			s.IngressList = append(s.IngressList, copy)
		}
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
	data, err := json.Marshal(s)
	if err != nil {
		return Snapshot{}, fmt.Errorf("encode captured snapshot: %w", err)
	}
	if len(data) > MaxCapturedSnapshotBytes {
		return Snapshot{}, fmt.Errorf("captured snapshot exceeds %d bytes", MaxCapturedSnapshotBytes)
	}
	return s, nil
}
