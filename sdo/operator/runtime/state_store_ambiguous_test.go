package runtime

import (
	"context"
	"errors"
	"testing"

	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

func stateAt(iteration int32) *contracts.ControllerState {
	return &contracts.ControllerState{Version: RuntimeStateVersion, EvaluationIteration: iteration}
}

// loseNextReply makes the next create or update of the state ConfigMap land and
// then fail, as a request that times out after the API server committed it.
func loseNextReply(client *fake.Clientset) {
	configMaps := schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	lost := false
	for _, verb := range []string{"create", "update"} {
		verb := verb
		client.PrependReactor(verb, "configmaps", func(action ktesting.Action) (bool, k8sruntime.Object, error) {
			if lost {
				return false, nil, nil
			}
			lost = true
			object := action.(ktesting.CreateAction).GetObject()
			var err error
			if verb == "update" {
				err = client.Tracker().Update(configMaps, object, action.GetNamespace())
			} else {
				err = client.Tracker().Create(configMaps, object, action.GetNamespace())
			}
			if err != nil {
				return true, nil, err
			}
			return true, nil, errors.New("simulated: request timed out after the write landed")
		})
	}
}

// A write whose reply was lost is this controller's own: the next save must
// continue from it, not report another writer forever.
func TestStateStoreContinuesFromItsOwnWriteWhoseReplyWasLost(t *testing.T) {
	for _, verb := range []string{"create", "update"} {
		t.Run(verb, func(t *testing.T) {
			ctx := context.Background()
			client := fake.NewSimpleClientset()
			store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
			revision := ""
			if verb == "update" {
				var err error
				if revision, err = store.Save(ctx, stateAt(1), ""); err != nil {
					t.Fatal(err)
				}
			}
			loseNextReply(client)
			if _, err := store.Save(ctx, stateAt(2), revision); err == nil {
				t.Fatal("lost reply was not reported")
			}
			next, err := store.Save(ctx, stateAt(3), revision)
			if err != nil {
				t.Fatalf("save after a lost reply: %v", err)
			}
			loaded, loadedRevision, err := store.Load(ctx)
			if err != nil || loaded.GetEvaluationIteration() != 3 || loadedRevision != next {
				t.Fatalf("state after recovery: %d at %q (saved %q), %v", loaded.GetEvaluationIteration(), loadedRevision, next, err)
			}
		})
	}
}

// Only the controller's own ambiguous write is adopted: another writer's state at
// the next revision still fences this controller.
func TestStateStoreStillFencesAForeignWriterAfterALostReply(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
	revision, err := store.Save(ctx, stateAt(1), "")
	if err != nil {
		t.Fatal(err)
	}
	// The write fails before landing; a foreign controller then takes the next revision.
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, errors.New("simulated: connection refused")
	})
	if _, err := store.Save(ctx, stateAt(2), revision); err == nil {
		t.Fatal("failed write was not reported")
	}
	client.ReactionChain = client.ReactionChain[1:]
	foreign := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
	if _, _, err := foreign.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.Save(ctx, stateAt(9), revision); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Save(ctx, stateAt(3), revision); !errors.Is(err, lifecycleio.ErrStateConflict) {
		t.Fatalf("foreign writer was adopted: %v", err)
	}
}

// Seed 81's no-restart schedule found an accepted write followed by many
// writes that fail before landing. Recovery must retain the accepted revision.
func TestStateStoreAdoptsLostReplyAcrossRepeatedRefusedWrites(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	store := lifecycleio.NewConfigMapStateStore(client, "demo", "state")
	revision, err := store.Save(ctx, stateAt(1), "")
	if err != nil {
		t.Fatal(err)
	}
	loseNextReply(client)
	if _, err := store.Save(ctx, stateAt(2), revision); err == nil {
		t.Fatal("lost reply not reported")
	}
	remaining := 32
	client.PrependReactor("update", "configmaps", func(ktesting.Action) (bool, k8sruntime.Object, error) {
		if remaining > 0 {
			remaining--
			return true, nil, errors.New("simulated: connection refused")
		}
		return false, nil, nil
	})
	for i := int32(3); i < 35; i++ {
		if _, err := store.Save(ctx, stateAt(i), revision); err == nil {
			t.Fatal("refused write not reported")
		}
	}
	if _, err := store.Save(ctx, stateAt(35), revision); err != nil {
		t.Fatalf("seed 81: recovery after repeated refused writes: %v", err)
	}
}
