package runtime

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/runtime/internal/lifecycleio"
)

// memoryInstallStartup is the install's start-up record in memory.
type memoryInstallStartup struct{ period *contracts.StartupPeriod }

func (s *memoryInstallStartup) Load(context.Context) (*contracts.StartupPeriod, error) {
	if s.period == nil {
		return nil, nil
	}
	return proto.Clone(s.period).(*contracts.StartupPeriod), nil
}

func (s *memoryInstallStartup) Save(_ context.Context, period *contracts.StartupPeriod) error {
	if s.period == nil {
		s.period = proto.Clone(period).(*contracts.StartupPeriod)
	}
	return nil
}

// An install that was seen healthy records it outside its controller state. A
// restart whose controller state was retired (a restart from memory: the
// incident state is gone, the install is the same) continues that period and
// holds nothing; a fault then opens at once, as after any restart.
func TestRestartFromRetiredStateOfAnEstablishedInstallDoesNotHold(t *testing.T) {
	record := &memoryInstallStartup{}
	first := startupController(t, 30*time.Minute, 0, probeSamples(1, func(int) bool { return false }))
	if err := first.AttachStateStore(context.Background(), &memoryStateStore{}); err != nil {
		t.Fatal(err)
	}
	if err := first.AttachInstallStartup(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	for second := int64(0); second <= 120; second++ {
		step(t, first, second)
		if err := first.PersistState(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if record.period.GetEstablishedAt() == nil {
		t.Fatalf("the established period was not recorded for the install: %v", record.period)
	}
	second := startupController(t, 30*time.Minute, 500, probeSamples(1, func(int) bool { return true }))
	if err := second.AttachStateStore(context.Background(), &memoryStateStore{}); err != nil {
		t.Fatal(err)
	}
	if err := second.AttachInstallStartup(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	step(t, second, 500)
	if !second.IncidentOpen() {
		t.Fatal("a fault after a restart from retired state of an established install must open at once")
	}
}

// A new install (no record) still holds its openers until seen healthy.
func TestNewInstallWithoutARecordStillHolds(t *testing.T) {
	c := startupController(t, 30*time.Minute, 0, probeSamples(1, func(int) bool { return true }))
	if err := c.AttachStateStore(context.Background(), &memoryStateStore{}); err != nil {
		t.Fatal(err)
	}
	if err := c.AttachInstallStartup(context.Background(), &memoryInstallStartup{}); err != nil {
		t.Fatal(err)
	}
	step(t, c, 0)
	step(t, c, 1)
	if c.IncidentOpen() {
		t.Fatal("a new install must hold openers until seen healthy")
	}
}

// The reset that keeps the record and the runtime that writes it name the same
// ConfigMap.
func TestInstallStartupRecordNameIsTheResetsName(t *testing.T) {
	if lifecycleio.InstallStartupConfigMap != onboarding.InstallStartupConfigMapName {
		t.Fatalf("runtime writes %q, reset keeps %q", lifecycleio.InstallStartupConfigMap, onboarding.InstallStartupConfigMapName)
	}
}
