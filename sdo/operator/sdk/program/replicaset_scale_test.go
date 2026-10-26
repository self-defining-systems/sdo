package program_test

import (
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func revisionSet(name, owner, revision string) appsv1.ReplicaSet {
	controller := true
	set := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: name, Annotations: map[string]string{"deployment.kubernetes.io/revision": revision}}}
	if owner != "" {
		set.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: owner, UID: "web-uid", Controller: &controller}}
	}
	return set
}

func TestReplicaSetScaleOnlyRetiresAnOldRevisionOfABoundDeployment(t *testing.T) {
	web := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "demo", Name: "web", UID: "web-uid", Annotations: map[string]string{"deployment.kubernetes.io/revision": "2"}}}
	view := sdktest.Snapshot{NamespaceName: "demo", DeploymentList: []appsv1.Deployment{web}, ReplicaSetList: []appsv1.ReplicaSet{
		revisionSet("web-old", "web", "1"), revisionSet("web-new", "web", "2"), revisionSet("other-old", "other", "1"), revisionSet("bare", "", "1"),
	}}
	bound := program.Bindings{Objects: program.ObjectBindings{"deployment": {APIVersion: "apps/v1", Kind: "Deployment", Namespace: "demo", Name: "web"}}}
	scale := func(name string) program.Plan {
		return program.Plan{Actions: []program.Action{program.ScaleWorkload{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "demo", Name: name}, Replicas: program.FromParam("replicas")}}}
	}
	if err := program.CheckReplicaSetScales(scale("web-old"), view, bound); err != nil {
		t.Fatalf("an old revision of the bound Deployment must be retirable: %v", err)
	}
	for _, name := range []string{"web-new", "other-old", "bare", "missing"} {
		if err := program.CheckReplicaSetScales(scale(name), view, bound); !errors.Is(err, program.ErrReplicaSetScaleRefused) {
			t.Fatalf("scaling %s must be a typed refusal, got %v", name, err)
		}
	}
	if err := program.CheckReplicaSetScales(scale("web-old"), view, program.Bindings{}); !errors.Is(err, program.ErrReplicaSetScaleRefused) {
		t.Fatalf("an unbound owner must be refused, got %v", err)
	}
}
