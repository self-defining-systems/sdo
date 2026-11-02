package broker

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory/validate"
)

// Run the real broker Check producer into its validator. The validator
// is synthetic: compilation has a separate real-Go test.
func TestReplayPolicyReachesBrokerAndValidator(t *testing.T) {
	for _, replay := range []string{"on", "off"} {
		t.Run(replay, func(t *testing.T) {
			root := repository(t)
			if err := os.WriteFile(filepath.Join(root, "app.txt"), []byte("synthetic repair\n"), 0644); err != nil {
				t.Fatal(err)
			}
			seen := false
			runner := Validator(func(ctx context.Context, _ string, _ []string) error {
				seen = true
				if got, _ := ctx.Value(replayKey{}).(string); got != replay {
					t.Fatalf("validator replay %q, want %q", got, replay)
				}
				return nil
			})
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
