package runtime

import (
	"slices"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

func sameSet(t *testing.T, what string, declared, named []string) {
	t.Helper()
	declared, named = slices.Clone(declared), slices.Clone(named)
	slices.Sort(declared)
	slices.Sort(named)
	if !slices.Equal(declared, named) {
		t.Fatalf("%s constants %v differ from the telemetry.proto rule %v", what, named, declared)
	}
}

func TestFiringEventsMatchProto(t *testing.T) {
	sameSet(t, "firing event", wire.StringIn(&contracts.FiringRecord{}, "event"),
		[]string{string(FiringActivated), string(FiringCleared), string(FiringNotPersisted), string(FiringBatched)})
}

func TestFiringRelationsMatchProto(t *testing.T) {
	sameSet(t, "firing relation", wire.StringIn(&contracts.FiringRecord{}, "dispatch_relation"),
		[]string{string(RelationBeforeDispatch), string(RelationAfterDispatch), string(RelationNoIncident)})
}

func TestLifecyclePhasesMatchProto(t *testing.T) {
	sameSet(t, "lifecycle phase", wire.StringIn(&contracts.LifecycleEvent{}, "event"), []string{
		PhaseOpened, PhaseWorkspacePrepared, PhaseDispatched,
		PhaseResponderCompleted, PhaseClosed,
		PhaseClosureCommitted, PhaseAcknowledged,
	})
}
