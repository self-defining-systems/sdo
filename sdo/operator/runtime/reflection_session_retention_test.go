package runtime

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

// The responder's session is the one reflection resumes, so closure pruning
// keeps it for as long as a queued reflection has not run, even past the
// retention window, and removes it only after reflection is processed.
func TestResponderSessionOutlivesRetentionUntilReflectionRuns(t *testing.T) {
	mount := t.TempDir()
	now := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	acknowledgedAt := now.Add(-30 * 24 * time.Hour)
	for _, tc := range []struct {
		name      string
		queued    bool
		processed bool
		kept      bool
	}{
		{"reflection queued", true, false, true},
		{"reflection processed", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := NativeIncidentBroker{Repository: &broker.Broker{}, LedgerRoot: t.TempDir(), SessionRoot: mount, SessionRetention: time.Hour}
			id := "incident-" + tc.name
			dir, err := memory.IncidentCorpusDir(mount, id)
			if err != nil {
				t.Fatal(err)
			}
			if err := native.saveLedger(&contracts.BrokerLedger{
				IncidentId: id, Acknowledged: true, AcknowledgedAt: timestamppb.New(acknowledgedAt),
				ReflectionQueued: tc.queued, ReflectionProcessed: tc.processed,
			}); err != nil {
				t.Fatal(err)
			}
			if err := native.PruneIncidentSessions(now); err != nil {
				t.Fatal(err)
			}
			_, statErr := os.Stat(filepath.Dir(dir))
			if kept := statErr == nil; kept != tc.kept {
				t.Fatalf("session kept = %v, want %v", kept, tc.kept)
			}
		})
	}
}
