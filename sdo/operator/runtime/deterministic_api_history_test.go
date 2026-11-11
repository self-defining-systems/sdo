package runtime

import (
	"bytes"
	"context"
	"log/slog"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"strings"
	"testing"
)

func TestAPIHistoryRequiredForResponderButNotControllerOnlyClosure(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, path := range []string{"deterministic", "responder", ""} {
		t.Run(path, func(t *testing.T) {
			var logs bytes.Buffer
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			closure := IncidentClosure{Request: IncidentRequest{IncidentID: "synthetic", PriorStages: []*contracts.StageReport{{Stage: "deterministic"}}}, Result: &IncidentResult{IncidentID: "synthetic", ResolutionPath: path}}
			if _, err := toProtoClosure(closure); err != nil {
				t.Fatal(err)
			}
			_, _ = (ProtoOutcomeDeriver{SessionRoot: t.TempDir()}).Derive(context.Background(), closure, "", "", "")
			warned := strings.Contains(logs.String(), "incident_api_repair_receipts_unavailable")
			if warned != (path != "deterministic") {
				t.Fatalf("path %q history warning=%v: %s", path, warned, logs.String())
			}
		})
	}
}
