package runtime

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sdo.dev/operator/install"
)

// A source repair or head deploy pauses the controller from inside the controller process. EKS series: the head
// deployer paused at 07:21:51.54 and the health guard rebuilt the controller 80 ms later; the rebuilt controller
// started paused, polled no head, and nothing resumed it until the pause's 40-minute deadline. A repair pause found
// at start is the previous process's, so the controller lifts it; any other pause (a human's, a harness's) holds.
func TestStartLiftsAnOrphanedRepairPauseOnly(t *testing.T) {
	ctx := context.Background()
	w, _ := pausedWith(t, map[string]string{"state": "paused", "generation": install.RepairMaintenancePrefix + "paused-1795418511543067439"})
	state, err := w.Read(ctx)
	if err != nil || !state.Paused {
		t.Fatalf("read: %+v, %v", state, err)
	}
	lifted, ok, err := w.LiftOrphanedRepairPause(ctx, state)
	if err != nil || !ok || lifted.Paused || !strings.HasPrefix(lifted.Generation, install.RepairMaintenancePrefix+"orphan-resumed-") {
		t.Fatalf("an orphaned repair pause must be lifted: %+v ok=%v err=%v", lifted, ok, err)
	}
	if again, err := w.Read(ctx); err != nil || again.Paused || again.Generation != lifted.Generation {
		t.Fatalf("the ConfigMap must say active with the new generation: %+v, %v", again, err)
	}

	for _, generation := range []string{"human-1", "devswap-paused-7", ""} {
		w, _ := pausedWith(t, map[string]string{"state": "paused", "generation": generation})
		state, _ := w.Read(ctx)
		kept, ok, err := w.LiftOrphanedRepairPause(ctx, state)
		if err != nil || ok || !kept.Paused {
			t.Fatalf("pause %q is not a repair's and must hold: %+v ok=%v err=%v", generation, kept, ok, err)
		}
		cm, _ := w.Client.CoreV1().ConfigMaps(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if cm.Data["state"] != "paused" {
			t.Fatalf("pause %q was rewritten: %v", generation, cm.Data)
		}
	}
}
