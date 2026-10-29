package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/validate"
)

// Run the real broker Check producer into its real job request consumer. The
// job receipt itself is synthetic: compilation has a separate real-Go test.
func TestReplayPolicyReachesBrokerAndIsolatedValidator(t *testing.T) {
	for _, replay := range []string{"on", "off"} {
		t.Run(replay, func(t *testing.T) {
			root := repository(t)
			if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("synthetic repair\n"), 0644); err != nil {
				t.Fatal(err)
			}
			seen := false
			runner := JobValidator(validationJobFunc(func(_ context.Context, r ValidationJobRequest) (ValidationJobReceipt, error) {
				seen = true
				if r.Replay != replay {
					t.Fatalf("job replay %q, want %q", r.Replay, replay)
				}
				return ValidationJobReceipt{CandidateTree: r.CandidateTree, Accepted: true, DetectorCompiled: true, SourceValidated: true, ReadOnlyMountObserved: true, NoPublishingCredentialsObserved: true, EgressIsolationCanaryPassed: true}, nil
			}))
			b, err := New(root, t.TempDir(), runner, ObservationPolicy{Replay: replay})
			if err != nil {
				t.Fatal(err)
			}
			ds, err := b.checkCandidate(context.Background(), root, "HEAD", Responder, ProposalPhase, []string{"app.txt"}, runner, &validate.PublicationContext{BaseCommit: "HEAD"}, nil, nil)
			if err != nil || len(ds) > 0 || !seen {
				t.Fatalf("policy transport: %v %v seen=%v", ds, err, seen)
			}
		})
	}
}
