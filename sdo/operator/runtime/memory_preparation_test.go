package runtime

import (
	"context"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/inputs"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type mirrorProjectionSnapshot struct {
	sdktest.Snapshot
	projections *atomic.Int32
}

func (s mirrorProjectionSnapshot) Pods() []corev1.Pod { s.projections.Add(1); return s.Snapshot.Pods() }

type gatedDiffView struct {
	program.ReadView
	entered chan struct{}
	release chan struct{}
}

func (v gatedDiffView) DiffAgainstSource(sdk.ObjectRef) (program.Diff, error) {
	v.entered <- struct{}{}
	<-v.release
	return program.Diff{}, nil
}
func TestFrozenPreparationOverlapsBoundedReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}, 16), make(chan struct{})
		view := NewCycleReadView(gatedDiffView{ReadView: program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"}), entered: entered, release: release})
		refs := make([]sdk.ObjectRef, 16)
		for i := range refs {
			refs[i] = sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: fmt.Sprintf("api-%d", i)}
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if _, err := frozenMemoryReads(view, refs); err != nil {
				t.Error(err)
			}
		}()
		synctest.Wait()
		if got := len(entered); got != 8 {
			t.Errorf("bounded independent reads must overlap: entered=%d want 8", got)
		}
		close(release)
		<-done
	})
}

func TestMirrorProjectionSharedByRefsAndCapture(t *testing.T) {
	var projections atomic.Int32
	snapshot := mirrorProjectionSnapshot{Snapshot: sdktest.Snapshot{NamespaceName: "app", PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}}}}, projections: &projections}
	for cycle := int32(1); cycle <= 2; cycle++ {
		view := NewCycleReadView(LiveReadView{Carrier: inputs.New(snapshot)})
		if _, err := snapshotMemoryRefs(view); err != nil {
			t.Fatal(err)
		}
		host := &speedHost{}
		_, _, release, err := host.Capture(context.Background(), view)
		if err != nil {
			t.Fatal(err)
		}
		release()
		if got := projections.Load(); got != cycle {
			t.Fatalf("mirror projected more than once per cycle: got %d want %d", got, cycle)
		}
	}
}
