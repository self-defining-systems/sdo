package runtime

import (
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// recoveryEffects binds observational work to exact incarnations, not names.
// Only complete repairs of an unchanged workload can explain its old children.
// This grants waiting authority only: a live child still blocks until gone.
type recoveryEffect struct {
	Ref sdk.ObjectRef
	UID types.UID
}
type recoveryEffects map[string]recoveryEffect

func repairedLifecycleEffects(snapshot sdk.DetectionContext, applications []RecordedPlanApplication, findings []sdk.Finding) recoveryEffects {
	repaired := map[types.UID]bool{}
	for _, a := range applications {
		if a.Pending || a.Error != "" || a.RolledBack || a.RollbackError != "" || !a.Result.Applied || a.Result.DryRun || a.Result.RolloutPending != nil || !a.Result.VerificationCompleted || !a.Result.VerificationPassed || a.Result.RolledBack || a.Result.RollbackError != "" {
			continue
		}
		for _, effect := range a.Result.observedObjectEffects() {
			if !effect.Confirmed || effect.State != "applied" || effect.Ref.APIVersion != "apps/v1" || effect.Ref.Kind != "Deployment" || effect.UID == "" || effect.Operation == "delete" {
				continue
			}
			for _, d := range snapshot.Deployments() {
				if d.Namespace == effect.Ref.Namespace && d.Name == effect.Ref.Name && string(d.UID) == effect.UID {
					repaired[d.UID] = true
				}
			}
		}
	}
	sets := map[types.UID]appsv1.ReplicaSet{}
	for _, rs := range snapshot.ReplicaSets() {
		owner := metav1.GetControllerOf(&rs)
		if owner == nil || owner.Kind != "Deployment" || owner.APIVersion != "apps/v1" || !repaired[owner.UID] || rs.UID == "" {
			continue
		}
		for _, d := range snapshot.Deployments() {
			if d.UID == owner.UID && d.Name == owner.Name && d.Namespace == rs.Namespace && !sameRolloutTemplate(rs.Spec.Template, d.Spec.Template) {
				sets[rs.UID] = rs
			}
		}
	}
	eligible := map[sdk.ObjectRef]metav1.Object{}
	for _, rs := range sets {
		if rs.Spec.Replicas != nil && *rs.Spec.Replicas == 0 {
			r := rs
			eligible[sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: rs.Namespace, Name: rs.Name}] = &r
		}
	}
	for _, pod := range snapshot.Pods() {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.Kind != "ReplicaSet" || owner.APIVersion != "apps/v1" || pod.UID == "" || pod.DeletionTimestamp == nil {
			continue
		}
		rs, ok := sets[owner.UID]
		if ok && rs.Name == owner.Name && rs.Namespace == pod.Namespace {
			p := pod
			eligible[sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name}] = &p
		}
	}
	effects := recoveryEffects{}
	for _, f := range findings {
		if !inventoryFinding(f) || inventoryEvent(f).Source != "absent from accepted source" {
			continue
		}
		object := eligible[f.PrimaryResource]
		if object == nil {
			continue
		}
		raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			continue
		}
		// The inventory fingerprint binds UID, ownership and configuration. A
		// changed or reused name cannot borrow the old finding's waiting authority.
		current := &unstructured.Unstructured{Object: raw}
		if reflect.DeepEqual(inventoryEvent(f).Fields, inventoryFields(current)) {
			effects[FindingFingerprint(f)] = recoveryEffect{Ref: f.PrimaryResource, UID: object.GetUID()}
		}
	}
	return effects
}

// observe retires absent identities and empty, zero-sized historical sets.
// A replacement at the same name is never disappearance/equivalence.
func (effects recoveryEffects) observe(snapshot sdk.DetectionContext, findings []sdk.Finding) []sdk.Finding {
	present := map[sdk.ObjectRef]bool{}
	for _, p := range snapshot.Pods() {
		present[sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: p.Namespace, Name: p.Name}] = true
	}
	for _, r := range snapshot.ReplicaSets() {
		present[sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: r.Namespace, Name: r.Name}] = true
	}
	remaining := make([]sdk.Finding, 0, len(findings))
	for _, f := range findings {
		effect, owned := effects[FindingFingerprint(f)]
		if !owned {
			remaining = append(remaining, f)
			continue
		}
		if !present[effect.Ref] {
			continue
		}
		equivalent := false
		if effect.Ref.Kind == "ReplicaSet" {
			for _, rs := range snapshot.ReplicaSets() {
				if rs.Namespace != effect.Ref.Namespace || rs.Name != effect.Ref.Name || rs.UID != effect.UID || rs.Spec.Replicas == nil || *rs.Spec.Replicas != 0 || rs.Status.Replicas != 0 {
					continue
				}
				equivalent = true
				for _, pod := range snapshot.Pods() {
					if pod.Namespace == rs.Namespace && rolloutOwnedBy(pod.OwnerReferences, "ReplicaSet", rs.UID) {
						equivalent = false
						break
					}
				}
			}
		}
		if !equivalent {
			remaining = append(remaining, f)
		}
	}
	return remaining
}

// A child already gone at the first post-repair observation needs no retained
// owner chain. Exact accepted-source absence plus observed live absence is the
// proof; read errors and name reuse never count as disappearance.
func retireDisappearedRecoveryEffects(view program.ReadView, findings []sdk.Finding) []sdk.Finding {
	remaining := make([]sdk.Finding, 0, len(findings))
	for _, f := range findings {
		if inventoryFinding(f) && inventoryEvent(f).Source == "absent from accepted source" && (f.PrimaryResource.Kind == "Pod" || f.PrimaryResource.Kind == "ReplicaSet") {
			diff, err := view.DiffAgainstSource(f.PrimaryResource)
			if err == nil && diff.SourceAbsent && !diff.Changed {
				continue
			}
		}
		remaining = append(remaining, f)
	}
	return remaining
}
