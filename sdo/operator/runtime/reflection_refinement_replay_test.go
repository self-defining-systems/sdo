package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Opt-in read-only replay: archived experiment outputs stay outside the repo.
// This reclassifies actual outcomes and checks protected capture presence; it
// never prepares sessions, executes archived code, contacts a model or cluster,
// or turns a refinement question into an automatically labelled positive case.
func TestRefinementQuestionAF3Replay(t *testing.T) {
	analysis := os.Getenv("SDO_RF1_AF3_ANALYSIS")
	if analysis == "" {
		t.Skip("set SDO_RF1_AF3_ANALYSIS to archived AF3 analysis")
	}
	for _, trial := range []struct {
		stage   int
		program string
		verdict contracts.Verdict_Result
	}{
		{3, "image-pull-failure", contracts.Verdict_RESULT_NO_MATCH},
		{4, "service-targetport-mismatch", contracts.Verdict_RESULT_UNKNOWN},
	} {
		t.Run(fmt.Sprintf("stage-%d", trial.stage), func(t *testing.T) {
			read := func(suffix string) []byte {
				t.Helper()
				raw, err := os.ReadFile(filepath.Join(analysis, fmt.Sprintf("stage-%d-%s.json", trial.stage, suffix)))
				if err != nil {
					t.Fatal(err)
				}
				return raw
			}
			raw := read("outcome")
			outcome := &contracts.OutcomeRecord{}
			if err := protojson.Unmarshal(raw, outcome); err != nil {
				t.Fatal(err)
			}
			var summary struct{ Path string }
			if err := json.Unmarshal(read("summary"), &summary); err != nil {
				t.Fatal(err)
			}
			sessions := t1OneGlob(t, filepath.Join(summary.Path, "runs/*/worker_0/results/sdo_codex/*/run_1/sdo_runtime/sessions"))
			digest := fmt.Sprintf("%x", sha256.Sum256([]byte(outcome.IncidentId)))[:32]
			capture := filepath.Join(sessions, digest, "corpus")
			for _, phase := range []string{"open@detect.json", "fixed.json"} {
				if _, err := os.Stat(filepath.Join(capture, phase)); err != nil {
					t.Fatal(err)
				}
			}
			work := classifyCauseWork(outcome, nil)
			eligibility := &contracts.ReflectionEligibility{Version: 1, Causes: work}
			found := false
			for _, cause := range work {
				for _, item := range cause.Items {
					if item.Reason == "program_refinement" && item.ProgramId == trial.program && item.GetEvaluation().GetVerdict().GetResult() == trial.verdict {
						found = true
						t.Logf("program=%s revision=%s verdict=%s resources=%v question=%s", item.ProgramId, item.ProgramRevision, trial.verdict, item.Resources, item.Question)
					}
				}
			}
			if !found {
				t.Fatalf("expected named abstaining candidate: %v", eligibility)
			}
			positive, negative := settledProgramCases(eligibility)
			if len(positive) != 0 || len(negative) != 0 {
				t.Fatal("archived abstentions cannot grant settled corpus credit")
			}
			t.Logf("input_sha256=%x capture=%s", sha256.Sum256(raw), capture)
			if output := os.Getenv("SDO_RF1_REPLAY_OUTPUT"); output != "" {
				data, err := (protojson.MarshalOptions{Indent: "  ", UseProtoNames: true}).Marshal(eligibility)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(output, fmt.Sprintf("stage-%d-work.json", trial.stage)), data, 0644); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
