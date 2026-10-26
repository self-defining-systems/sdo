package validate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

// maxOwnerDepth covers Pod -> ReplicaSet -> Deployment and Pod -> Job -> CronJob.
const maxOwnerDepth = 4

// DropReplacedOwnedPods removes from each confirmed cause a Pod that the
// repair replaced and whose controller-owner chain, observed at incident open,
// reaches another resource of the same cause. The replaced Pod has no identity
// left to clear; its owning workload stays in the cause and must still clear
// its own opening finding. Pods still present after the repair, Pods without
// an in-cause owner, and causes without a protected capture are unchanged.
func DropReplacedOwnedPods(outcome *contracts.OutcomeRecord, capture string) {
	if capture == "" || len(outcome.GetConfirmedRootCauses()) == 0 {
		return
	}
	var fixed sdktest.Snapshot
	raw, err := os.ReadFile(filepath.Join(capture, "fixed.json"))
	if err != nil || json.Unmarshal(raw, &fixed) != nil || fixed.ObservedAt().IsZero() {
		return
	}
	var opens []sdktest.Snapshot
	for _, phase := range []string{"open@detect", "open@settle", "open@pre-apply"} {
		var s sdktest.Snapshot
		raw, err := os.ReadFile(filepath.Join(capture, phase+".json"))
		if err == nil && json.Unmarshal(raw, &s) == nil && !s.ObservedAt().IsZero() && !s.ObservedAt().After(fixed.ObservedAt()) {
			opens = append(opens, s)
		}
	}
	for _, cause := range outcome.GetConfirmedRootCauses() {
		kept := make([]*contracts.ObjectRef, 0, len(cause.GetResources()))
		for _, ref := range cause.GetResources() {
			if !replacedOwnedPod(opens, fixed, ref, cause.GetResources()) {
				kept = append(kept, ref)
			}
		}
		cause.Resources = kept
	}
}

func replacedOwnedPod(opens []sdktest.Snapshot, fixed sdktest.Snapshot, ref *contracts.ObjectRef, cause []*contracts.ObjectRef) bool {
	if ref.GetKind() != "Pod" || ref.GetName() == "" {
		return false
	}
	for _, open := range opens {
		pod := snapshotObject(open, "Pod", ref.GetNamespace(), ref.GetName())
		if pod == nil || !ownedByCauseResource(open, pod, ref, cause) {
			continue
		}
		after := snapshotObject(fixed, "Pod", ref.GetNamespace(), ref.GetName())
		if after == nil || (pod.GetUID() != "" && after.GetUID() != "" && pod.GetUID() != after.GetUID()) {
			return true
		}
	}
	return false
}

// ownedByCauseResource follows controller owner references only; a non-
// controlling owner or a name match without the observed chain is not owned.
func ownedByCauseResource(open sdktest.Snapshot, object *unstructured.Unstructured, pod *contracts.ObjectRef, cause []*contracts.ObjectRef) bool {
	return slices.ContainsFunc(observedOwnerChain(open, object), func(owner *contracts.ObjectRef) bool {
		return slices.ContainsFunc(cause, func(other *contracts.ObjectRef) bool {
			return other != pod && other.GetKind() != "Pod" && equalCauseResource(other, owner)
		})
	})
}

// observedOwnerChain is the object's controller owner chain in one snapshot,
// nearest owner first. Each hop's owner comes from the previous object's
// controller reference; the chain stops where an owner is not observed.
func observedOwnerChain(open sdktest.Snapshot, object *unstructured.Unstructured) []*contracts.ObjectRef {
	var chain []*contracts.ObjectRef
	for object != nil && len(chain) < maxOwnerDepth {
		var owner *contracts.ObjectRef
		for _, reference := range object.GetOwnerReferences() {
			if reference.Controller != nil && *reference.Controller {
				owner = &contracts.ObjectRef{ApiVersion: reference.APIVersion, Kind: reference.Kind, Namespace: object.GetNamespace(), Name: reference.Name}
			}
		}
		if owner == nil {
			break
		}
		chain = append(chain, owner)
		object = snapshotObject(open, owner.GetKind(), owner.GetNamespace(), owner.GetName())
	}
	return chain
}

// ObservedRepairOwners records, for each written resource that has no owner
// chain yet, the controller owner chain SDO observed in the incident's open
// capture (latest open phase first). Responder-supplied chains never reach
// the outcome: result ingest drops them. Chains the controller recorded from
// plan pre-images stay. A resource absent from every open phase gets no
// chain, so its write stays unattributed.
func ObservedRepairOwners(outcome *contracts.OutcomeRecord, capture string) {
	if capture == "" || len(outcome.GetRepairActions()) == 0 {
		return
	}
	var opens []sdktest.Snapshot
	for _, phase := range []string{"open@pre-apply", "open@settle", "open@detect"} {
		var s sdktest.Snapshot
		raw, err := os.ReadFile(filepath.Join(capture, phase+".json"))
		if err == nil && json.Unmarshal(raw, &s) == nil && !s.ObservedAt().IsZero() {
			opens = append(opens, s)
		}
	}
	for _, action := range outcome.GetRepairActions() {
		for _, written := range action.GetResources() {
			if slices.ContainsFunc(action.GetOwners(), func(owners *contracts.ResourceOwners) bool { return equalCauseResource(owners.GetResource(), written) }) {
				continue
			}
			for _, open := range opens {
				if chain := observedOwnerChain(open, snapshotObject(open, written.GetKind(), written.GetNamespace(), written.GetName())); len(chain) > 0 {
					action.Owners = append(action.Owners, &contracts.ResourceOwners{Resource: proto.Clone(written).(*contracts.ObjectRef), OwnerChain: chain})
					break
				}
			}
		}
	}
}

func snapshotObject(snapshot sdktest.Snapshot, kind, namespace, name string) *unstructured.Unstructured {
	for _, object := range append(append([]unstructured.Unstructured{}, snapshot.Objects...), sdk.TypedObjects(snapshot)...) {
		identity := sdk.ObjectIdentity(object)
		if identity.Kind == kind && identity.Namespace == namespace && identity.Name == name {
			return &object
		}
	}
	return nil
}
