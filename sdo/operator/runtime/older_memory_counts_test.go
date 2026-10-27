package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// Producer to consumer: outcome files on disk, as an earlier build and a
// damaged write left them, through the real repository reader into dispatch
// preparation. Neither may stop the controller from preparing a responder.
func TestProgramCountsSurviveOlderAndUnreadableOutcomes(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	write := func(incidentID, document string) {
		path := filepath.Join(root, layout.OutcomePath(incidentID))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(document), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("older", `{"schema_version":1,"incident_id":"older","classification":"success","reflection_eligibility":{"version":1,"unassigned":[{"program_id":"p","disposition":"question","reason":"evidence_gap"}]}}`)
	write("damaged", `{"schema_version":1,"incident_id":"damaged","classification":"nonsense"}`)
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "outcomes from earlier builds")
	d := KubernetesJobDispatcher{Outcomes: memory.OpenReader(root)}
	// Both files are skipped (one invalid at its schema, one damaged); the
	// scan returns instead of failing, and dispatch preparation goes on.
	if counts := d.programCounts(ctx); len(counts) != 0 {
		t.Fatalf("unreadable outcomes produced counts: %v", counts)
	}
}
