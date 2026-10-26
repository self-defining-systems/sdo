package program

import (
	"errors"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
)

// ErrReplicaSetScaleRefused marks a program plan that scales a ReplicaSet it
// may not retire.
var ErrReplicaSetScaleRefused = errors.New("replicaset_scale_refused")

const deploymentRevisionAnnotation = "deployment.kubernetes.io/revision"

// ReplicaSetOwnerView is the observation a ReplicaSet scale is checked against.
type ReplicaSetOwnerView interface {
	Deployments() []appsv1.Deployment
	ReplicaSets() []appsv1.ReplicaSet
}

// CheckReplicaSetScales allows a program to scale a ReplicaSet only when the
// observed ReplicaSet is controlled by a Deployment the program is bound to and
// is not that Deployment's current revision. Retiring an old revision is the
// only repair a program needs on a ReplicaSet; anything else risks an outage.
func CheckReplicaSetScales(plan Plan, view ReplicaSetOwnerView, bindings Bindings) error {
	for _, action := range plan.Actions {
		var scale ScaleWorkload
		switch a := action.(type) {
		case ScaleWorkload:
			scale = a
		case *ScaleWorkload:
			if a == nil {
				continue
			}
			scale = *a
		default:
			continue
		}
		if scale.Ref.Kind != "ReplicaSet" {
			continue
		}
		if err := checkReplicaSetScale(scale, view, bindings); err != nil {
			return fmt.Errorf("%w: ReplicaSet %s/%s: %s", ErrReplicaSetScaleRefused, scale.Ref.Namespace, scale.Ref.Name, err)
		}
	}
	return nil
}

func checkReplicaSetScale(scale ScaleWorkload, view ReplicaSetOwnerView, bindings Bindings) error {
	if view == nil {
		return errors.New("no observation of its owner")
	}
	var set *appsv1.ReplicaSet
	for _, candidate := range view.ReplicaSets() {
		if candidate.Namespace == scale.Ref.Namespace && candidate.Name == scale.Ref.Name {
			set = &candidate
			break
		}
	}
	if set == nil {
		return errors.New("not observed")
	}
	var owner, ownerUID string
	for _, reference := range set.OwnerReferences {
		if reference.Controller != nil && *reference.Controller && reference.Kind == "Deployment" {
			owner, ownerUID = reference.Name, string(reference.UID)
		}
	}
	if owner == "" {
		return errors.New("has no controlling Deployment")
	}
	bound := false
	for _, ref := range bindings.Objects {
		bound = bound || (ref.Kind == "Deployment" && ref.Namespace == set.Namespace && ref.Name == owner)
	}
	if !bound {
		return fmt.Errorf("its owner Deployment %s is not bound to the program", owner)
	}
	for _, deployment := range view.Deployments() {
		if deployment.Namespace != set.Namespace || deployment.Name != owner {
			continue
		}
		if ownerUID != "" && deployment.UID != "" && string(deployment.UID) != ownerUID {
			return fmt.Errorf("its owner reference does not match Deployment %s", owner)
		}
		current, revision := deployment.Annotations[deploymentRevisionAnnotation], set.Annotations[deploymentRevisionAnnotation]
		if current == "" || revision == "" {
			return errors.New("revision is not observed")
		}
		if current == revision {
			return fmt.Errorf("is the current revision %s of Deployment %s", current, owner)
		}
		return nil
	}
	return fmt.Errorf("owner Deployment %s is not observed", owner)
}
