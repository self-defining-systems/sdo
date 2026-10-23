package runtime

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/core"
	"sdo.dev/operator/sdk"
)

var errCausalObservation = errors.New("causal observation unavailable or superseded")

// CurrentObjectList comes only from a direct most-recent API list, never an
// informer or a resourceVersion=0 read. Incomplete/paginated lists cannot prove
// absence or the complete dependency closure.
type CurrentObjectList struct {
	Items           []unstructured.Unstructured
	ResourceVersion string
	Complete        bool
}
type DirectObservationReader interface {
	ListCurrent(context.Context, sdk.WatchKind) (CurrentObjectList, error)
}

// EffectExpectation contains process-local field images, never durable data.
// UID fences updates and deletes. A create requires independently observed
// prior absence; an existing name cannot be mistaken for that creation.
type EffectExpectation struct {
	Ref                   sdk.ObjectRef
	UID                   string
	Before                *unstructured.Unstructured
	After                 *unstructured.Unstructured
	RequireAbsent         bool
	PriorAbsenceCaptureID string
}
type CausalObservation struct {
	Receipt *contracts.MitigationBarrier
	View    sdk.DetectionContext
}

// CausalBarrier is separate from snapshot/corpus lifecycle hooks. Callers must
// supply the full diagnosis/health scope, a nonsecret read authorization gate,
// and source/scope/policy fencing. Nothing infers freshness from scope identity.
// It is deliberately not wired into the SK2 diagnosis/closure seams here.
type CausalBarrier struct {
	Journal       *selectionJournal
	Reader        DirectObservationReader
	Namespace     string
	Dependencies  []sdk.WatchKind
	AuthorizeRead func(context.Context, sdk.WatchKind) error
	CheckFence    func(context.Context) error
}

func (b *CausalBarrier) Capture(ctx context.Context, attempt string, round uint32, phase string, effects []EffectExpectation) (observation CausalObservation, err error) {
	if b.Journal == nil || b.Reader == nil || b.Namespace == "" || len(b.Dependencies) == 0 || b.AuthorizeRead == nil || b.CheckFence == nil {
		return observation, errCausalObservation
	}
	state := b.Journal.snapshot()
	if !b.Journal.now().Before(state.OriginalDeadline.AsTime()) {
		return observation, context.DeadlineExceeded
	}
	ctx, cancel := context.WithDeadline(ctx, state.OriginalDeadline.AsTime())
	defer cancel()
	if ctx.Err() != nil {
		return observation, ctx.Err()
	}
	dependencies := append([]sdk.WatchKind(nil), b.Dependencies...)
	sort.Slice(dependencies, func(i, k int) bool {
		return dependencies[i].APIVersion+"/"+dependencies[i].Kind < dependencies[k].APIVersion+"/"+dependencies[k].Kind
	})
	seen := map[sdk.WatchKind]bool{}
	for _, kind := range dependencies {
		if kind.Namespace != b.Namespace || !causalSupportedKind(kind) || seen[kind] {
			return observation, errCausalObservation
		}
		seen[kind] = true
		if b.AuthorizeRead(ctx, kind) != nil {
			return observation, errCausalObservation
		}
	}
	if b.CheckFence(ctx) != nil {
		return observation, errCausalObservation
	}
	receipt, err := b.Journal.beginBarrier(ctx, attempt, round, phase)
	if err != nil {
		return observation, err
	}
	observation.Receipt = receipt
	objects := map[sdk.ObjectRef]*unstructured.Unstructured{}
	snapshot := core.DetectionSnapshot{NamespaceName: b.Namespace}
	for _, kind := range dependencies {
		list, readErr := b.Reader.ListCurrent(ctx, kind)
		if readErr != nil || !list.Complete || list.ResourceVersion == "" {
			return observation, errCausalObservation
		}
		receipt.Collections = append(receipt.Collections, &contracts.MitigationCollectionObservation{ApiVersion: kind.APIVersion, Kind: kind.Kind, Namespace: kind.Namespace, ResourceVersion: list.ResourceVersion, ItemCount: uint32(len(list.Items))})
		for _, object := range list.Items {
			// TypeMeta is supplied by the collection's typed API in readers which
			// omit it per item. Never accept a contradictory type or namespace.
			if object.GetAPIVersion() == "" {
				object.SetAPIVersion(kind.APIVersion)
			}
			if object.GetKind() == "" {
				object.SetKind(kind.Kind)
			}
			if object.GetAPIVersion() != kind.APIVersion || object.GetKind() != kind.Kind || object.GetNamespace() != b.Namespace || object.GetName() == "" || object.GetUID() == "" || object.GetResourceVersion() == "" {
				return observation, errCausalObservation
			}
			ref := sdk.ObjectRef{APIVersion: kind.APIVersion, Kind: kind.Kind, Namespace: b.Namespace, Name: object.GetName()}
			if objects[ref] != nil {
				return observation, errCausalObservation
			}
			objects[ref] = object.DeepCopy()
			if causalAppendObject(&snapshot, kind.Kind, object.Object) != nil {
				return observation, errCausalObservation
			}
		}
	}
	for ref, object := range objects {
		receipt.Observations = append(receipt.Observations, &contracts.MitigationObservation{Resource: planRefProto(ref), Uid: string(object.GetUID()), ResourceVersion: object.GetResourceVersion()})
	}
	for _, effect := range effects {
		if !seen[sdk.WatchKind{APIVersion: effect.Ref.APIVersion, Kind: effect.Ref.Kind, Namespace: effect.Ref.Namespace}] {
			return observation, errCausalObservation
		}
		object := objects[effect.Ref]
		if effect.RequireAbsent {
			if object != nil {
				return observation, errCausalObservation
			}
			receipt.Observations = append(receipt.Observations, &contracts.MitigationObservation{Resource: planRefProto(effect.Ref), ConfirmedAbsent: true, EffectConfirmed: true})
			continue
		}
		if object == nil {
			return observation, errCausalObservation
		}
		if effect.PriorAbsenceCaptureID != "" {
			if !causalPriorAbsence(state, attempt, effect.PriorAbsenceCaptureID, effect.Ref) {
				return observation, errCausalObservation
			}
			if effect.Before != nil || effect.UID != "" || effect.After == nil || string(effect.After.GetUID()) == "" || object.GetUID() != effect.After.GetUID() {
				return observation, errCausalObservation
			}
		} else if effect.UID == "" || string(object.GetUID()) != effect.UID {
			return observation, errCausalObservation
		}
		if effect.After != nil {
			for _, field := range []string{"spec", "data", "binaryData"} {
				var before any
				bok := false
				if effect.Before != nil {
					before, bok = effect.Before.Object[field]
				}
				after, aok := effect.After.Object[field]
				current, cok := object.Object[field]
				if !causalFieldsMatch(before, bok, after, aok, current, cok) {
					return observation, errCausalObservation
				}
			}
		} else {
			return observation, errCausalObservation
		}
		for _, proof := range receipt.Observations {
			if proto.Equal(proof.Resource, planRefProto(effect.Ref)) {
				proof.EffectConfirmed = true
			}
		}
	}
	if !b.Journal.now().Before(state.OriginalDeadline.AsTime()) {
		return observation, context.DeadlineExceeded
	}
	if ctx.Err() != nil {
		return observation, ctx.Err()
	}
	if b.CheckFence(ctx) != nil {
		return observation, errCausalObservation
	}
	sort.Slice(receipt.Observations, func(i, k int) bool {
		return receipt.Observations[i].Resource.String() < receipt.Observations[k].Resource.String()
	})
	receipt.ObservedAt = timestamppb.New(b.Journal.now().UTC())
	receipt.Complete = true
	if err := b.Journal.finishBarrier(ctx, attempt, receipt); err != nil {
		receipt.Complete = false
		return observation, err
	}
	snapshot.ObservedAtTime = receipt.ObservedAt.AsTime()
	observation.View = causalDetectionView{DetectionSnapshot: snapshot, listed: seen}
	return observation, nil
}
func causalSupportedKind(kind sdk.WatchKind) bool {
	switch kind.APIVersion + "/" + kind.Kind {
	case "v1/ConfigMap", "v1/Service", "v1/Pod", "v1/Endpoints", "v1/Event", "apps/v1/Deployment", "apps/v1/ReplicaSet", "apps/v1/StatefulSet", "discovery.k8s.io/v1/EndpointSlice", "networking.k8s.io/v1/NetworkPolicy", "autoscaling/v2/HorizontalPodAutoscaler", "policy/v1/PodDisruptionBudget":
		return true
	}
	return false
}

// KubernetesCurrentReader uses only existing client credentials and grants.
// No informer, cached fallback, opaque-version ordering, or Secret API exists.
// CausalBarrier must authorize each collection before invoking this reader.
type KubernetesCurrentReader struct {
	Client      KubernetesPlanClient
	TypedClient kubernetes.Interface
}

func (r KubernetesCurrentReader) ListCurrent(ctx context.Context, kind sdk.WatchKind) (CurrentObjectList, error) {
	if !causalSupportedKind(kind) || kind.Namespace == "" {
		return CurrentObjectList{}, errCausalObservation
	}
	if r.TypedClient != nil {
		return r.listTypedCurrent(ctx, kind)
	}
	resource, err := r.Client.resource(sdk.ObjectRef{APIVersion: kind.APIVersion, Kind: kind.Kind, Namespace: kind.Namespace})
	if err != nil {
		return CurrentObjectList{}, errCausalObservation
	}
	// Empty ResourceVersion asks the API for most-recent data. Never request 0.
	list, err := resource.List(ctx, metav1.ListOptions{Limit: 1001})
	if err != nil {
		return CurrentObjectList{}, errCausalObservation
	}
	complete := list.GetContinue() == "" && (list.GetRemainingItemCount() == nil || *list.GetRemainingItemCount() == 0) && len(list.Items) <= 1000
	return CurrentObjectList{Items: list.Items, ResourceVersion: list.GetResourceVersion(), Complete: complete}, nil
}
func (j *selectionJournal) beginBarrier(ctx context.Context, attempt string, round uint32, phase string) (*contracts.MitigationBarrier, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.frozen || j.state.SelectionLedger.DispatchEpoch != j.epoch {
		return nil, errSelectionFenced
	}
	next := j.next()
	a, err := selectionAttempt(next.SelectionLedger, attempt)
	if err != nil {
		return nil, err
	}
	occurrence := uint32(1)
	for _, capture := range a.Barriers {
		if capture.RoundIndex == round && capture.Phase == phase && capture.Occurrence >= occurrence {
			occurrence = capture.Occurrence + 1
		}
	}
	if len(a.Barriers) >= 128 {
		return nil, errors.New("selection barrier limit exhausted")
	}
	receipt := &contracts.MitigationBarrier{CaptureId: fmt.Sprintf("%s/round-%d/%s/%d-%s", attempt, round, phase, occurrence, rand.Text()), AttemptId: attempt, RoundIndex: round, Phase: phase, Occurrence: occurrence, ObservedAt: timestamppb.New(j.now().UTC())}
	a.Barriers = append(a.Barriers, receipt)
	if err := j.save(ctx, next); err != nil {
		return nil, err
	}
	return proto.Clone(receipt).(*contracts.MitigationBarrier), nil
}
func (j *selectionJournal) finishBarrier(ctx context.Context, attempt string, receipt *contracts.MitigationBarrier) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.frozen || j.state.SelectionLedger.DispatchEpoch != j.epoch {
		return errSelectionFenced
	}
	next := j.next()
	a, err := selectionAttempt(next.SelectionLedger, attempt)
	if err != nil {
		return err
	}
	for index, capture := range a.Barriers {
		if capture.CaptureId == receipt.CaptureId {
			if capture.Complete || capture.AttemptId != receipt.AttemptId || capture.RoundIndex != receipt.RoundIndex || capture.Phase != receipt.Phase || capture.Occurrence != receipt.Occurrence || !receipt.Complete {
				return errors.New("barrier completion identity mismatch")
			}
			a.Barriers[index] = proto.Clone(receipt).(*contracts.MitigationBarrier)
			return j.save(ctx, next)
		}
	}
	return errors.New("barrier completion requires durable capture intent")
}
func causalAppendObject(snapshot *core.DetectionSnapshot, kind string, object map[string]any) error {
	switch kind {
	case "ConfigMap":
		return causalDecodeAppend(&snapshot.ConfigMapList, object)
	case "Service":
		return causalDecodeAppend(&snapshot.ServiceList, object)
	case "Pod":
		return causalDecodeAppend(&snapshot.PodList, object)
	case "Deployment":
		return causalDecodeAppend(&snapshot.DeploymentList, object)
	case "ReplicaSet":
		return causalDecodeAppend(&snapshot.ReplicaSetList, object)
	case "StatefulSet":
		return causalDecodeAppend(&snapshot.StatefulSetList, object)
	case "Endpoints":
		return causalDecodeAppend(&snapshot.EndpointList, object)
	case "EndpointSlice":
		return causalDecodeAppend(&snapshot.EndpointSliceList, object)
	case "NetworkPolicy":
		return causalDecodeAppend(&snapshot.NetworkPolicyList, object)
	case "Event":
		return causalDecodeAppend(&snapshot.EventList, object)
	case "HorizontalPodAutoscaler":
		return causalDecodeAppend(&snapshot.HPAList, object)
	case "PodDisruptionBudget":
		return causalDecodeAppend(&snapshot.PDBList, object)
	}
	return errCausalObservation
}
func causalDecodeAppend[T any](destination *[]T, object map[string]any) error {
	var item T
	if err := kruntime.DefaultUnstructuredConverter.FromUnstructured(object, &item); err != nil {
		return err
	}
	*destination = append(*destination, item)
	return nil
}

// Compare exactly the changed fields, including removal. Compensation's
// server-default tolerance must never turn a missing forward effect into proof.
func causalFieldsMatch(before any, bok bool, after any, aok bool, current any, cok bool) bool {
	if bok == aok && planValuesEqual(before, after) {
		return true
	}
	bm, bmap := before.(map[string]any)
	am, amap := after.(map[string]any)
	cm, cmap := current.(map[string]any)
	if bmap && amap && cmap {
		keys := map[string]bool{}
		for key := range bm {
			keys[key] = true
		}
		for key := range am {
			keys[key] = true
		}
		for key := range keys {
			b, bo := bm[key]
			a, ao := am[key]
			c, co := cm[key]
			if !causalFieldsMatch(b, bo, a, ao, c, co) {
				return false
			}
		}
		return true
	}
	ba, barray := before.([]any)
	aa, aarray := after.([]any)
	ca, carray := current.([]any)
	if barray && aarray && carray && len(ba) == len(aa) && len(aa) == len(ca) {
		for i := range ba {
			if !causalFieldsMatch(ba[i], true, aa[i], true, ca[i], true) {
				return false
			}
		}
		return true
	}
	return aok == cok && planValuesEqual(after, current)
}

func causalPriorAbsence(state *contracts.SelectionLedger, attempt, id string, ref sdk.ObjectRef) bool {
	a, err := selectionAttempt(state, attempt)
	if err != nil {
		return false
	}
	for _, capture := range a.Barriers {
		if capture.CaptureId == id && capture.Complete {
			for _, observation := range capture.Observations {
				if observation.ConfirmedAbsent && proto.Equal(observation.Resource, planRefProto(ref)) {
					return true
				}
			}
		}
	}
	return false
}
