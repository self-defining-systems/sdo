package memoryhost

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// ObjectSource is the host-owned, discovery-backed Kubernetes reader for kinds
// the mirror does not carry. It reads only; it never writes.
type ObjectSource interface {
	sdk.ObjectReader
	sdk.ObjectCaptureSource
}

// WithObjects installs the generic reader before serving. protectedNamespace,
// when set, is the SDO control namespace that generic reads refuse.
func (s *Server) WithObjects(source ObjectSource, protectedNamespace string) *Server {
	s.generic = source
	s.protected = protectedNamespace
	return s
}

// objectReads gives detectors and programs the object reads the controller's
// own views have: scoped targets, mirrored kinds, then the generic reader.
type objectReads struct {
	base      sdk.ObjectReader
	scoped    sdk.ScopedObservationSource
	capture   sdk.ObjectCaptureSource
	source    ObjectSource
	protected string
	namespace string
}

func (r objectReads) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	if r.protected != "" && ref.Namespace == r.protected {
		return nil, fmt.Errorf("generic reads cannot access the SDO control namespace %q", r.protected)
	}
	if ref.Namespace != r.namespace {
		target := sdk.ObservationTarget{APIVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace}
		if target.Cluster() {
			target.Name = ref.Name
		}
		for _, status := range r.scoped.ObservationStatuses() {
			if status.Target == target {
				return sdk.ScopedObject(r.scoped, ref)
			}
		}
	}
	object, err := r.base.Object(ref)
	if err == nil || !errors.Is(err, sdk.ErrObjectNotCaptured) || r.source == nil {
		return object, err
	}
	object, err = r.source.Object(ref)
	if err == nil && object != nil && (ref.Kind == "ValidatingWebhookConfiguration" || ref.Kind == "MutatingWebhookConfiguration") {
		object = object.DeepCopy()
		sdk.ProjectWebhookCABundles(object.Object)
	}
	return object, err
}

func (r objectReads) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	if r.source != nil {
		return r.source.CaptureObjects(refs)
	}
	return r.capture.CaptureObjects(refs)
}

// detectionView is the frozen snapshot plus the generic object reads.
type detectionView struct {
	sdktest.Snapshot
	reads objectReads
}

func (v detectionView) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return v.reads.Object(ref)
}
func (v detectionView) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	return v.reads.CaptureObjects(refs)
}

func (s *Server) detectionView(snapshot sdktest.Snapshot) sdk.DetectionContext {
	return detectionView{Snapshot: snapshot, reads: s.objectReads(snapshot, snapshot, snapshot, snapshot.Namespace())}
}

func (s *Server) objectReads(base sdk.ObjectReader, scoped sdk.ScopedObservationSource, capture sdk.ObjectCaptureSource, namespace string) objectReads {
	return objectReads{base: base, scoped: scoped, capture: capture, source: s.generic, protected: s.protected, namespace: namespace}
}

// programObjects layers the same reads over a program's read view.
type programObjects struct {
	program.ReadView
	reads objectReads
}

func (v programObjects) Object(ref sdk.ObjectRef) (*unstructured.Unstructured, error) {
	return v.reads.Object(ref)
}
func (v programObjects) CaptureObjects(refs []sdk.ObjectRef) ([]unstructured.Unstructured, error) {
	return v.reads.CaptureObjects(refs)
}

func (s *Server) programObjects(view program.ReadView, snapshot sdktest.Snapshot) program.ReadView {
	return programObjects{ReadView: view, reads: s.objectReads(view, view, view, snapshot.Namespace())}
}
