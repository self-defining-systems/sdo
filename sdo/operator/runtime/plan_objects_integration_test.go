//go:build integration

package runtime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestDeleteObservationPreservesBoundAndRejectsReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint(replacement), func(t *testing.T) {
			ref := sdk.ObjectRef{APIVersion: "batch/v1", Kind: "Job", Namespace: "app", Name: "terminating"}
			before := planObject(ref, nil)
			before.SetUID("original")
			before.SetResourceVersion("1")
			before.SetFinalizers([]string{"example.test/hold"})
			raw := &lifecycleTestClient{planFakeClient: &planFakeClient{objects: map[sdk.ObjectRef]*unstructured.Unstructured{ref: before}}, ignoreDelete: true}
			var client PlanObjectClient = raw
			if replacement {
				client = replacingDeleteClient{raw}
			}
			e := PlanExecutor{Client: client, Rollouts: &planFakeRollout{}, Verifier: planFakeVerifier{true}, RolloutTimeout: 30 * time.Millisecond}
			started := time.Now()
			result, err := e.Apply(context.Background(), IncidentRequest{Namespace: "app"}, program.Plan{Actions: []program.Action{program.DeleteObject{Ref: ref, Reason: "fixture"}}}, false)
			if err == nil || result.VerificationCompleted || result.ObjectEffects[0].Confirmed || raw.creates != 0 || raw.deletes != 1 {
				t.Fatalf("unconfirmed deletion granted credit: %+v %v", result, err)
			}
			var pending *ObjectDeletionPendingError
			if replacement {
				if errors.As(err, &pending) {
					t.Fatal("replacement mistaken for terminating original")
				}
			} else {
				if !errors.As(err, &pending) || pending.UID != "original" || len(pending.Finalizers) != 1 || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost typed terminating evidence: %v", err)
				}
			}
			if time.Since(started) > time.Second {
				t.Fatal("delete exceeded observation budget")
			}
		})
	}
}

