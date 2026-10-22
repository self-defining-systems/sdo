package sdktest

import (
	"context"
	"os"
	"os/exec"
	"sdo.dev/operator/sdk/program"
	"strings"
	"testing"
)

type fixedNegativeProgram struct{ result program.Result }

func (fixedNegativeProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "example"} }
func (p fixedNegativeProgram) Diagnose(context.Context, program.ReadView, program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: p.result}, nil
}

func TestFixedNegativeReplayRejectsAnUncorrectedMatch(t *testing.T) {
	corpus := CorpusCase{Open: Snapshot{NamespaceName: "app"}, Provenance: CorpusProvenance{IncidentID: "verified", OpenAt: CapturedFixedNoMatchPhase}, NoMatchProgramIDs: []string{"example"}}
	if os.Getenv("SDO_TEST_FIXED_NEGATIVE_FAILURE") == "1" {
		CheckNoMatchCase(t, fixedNegativeProgram{result: program.Match}, corpus)
		return
	}
	child := exec.Command(os.Args[0], "-test.run=^TestFixedNegativeReplayRejectsAnUncorrectedMatch$")
	child.Env = append(os.Environ(), "SDO_TEST_FIXED_NEGATIVE_FAILURE=1")
	output, err := child.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "got {Result:Match") {
		t.Fatalf("uncorrected fixed Match passed replay: %s (%v)", output, err)
	}
	CheckNoMatchCase(t, fixedNegativeProgram{result: program.NoMatch}, corpus)
}

func TestFixedNegativeUsesFixedPrimitiveObservations(t *testing.T) {
	corpus := CorpusCase{Provenance: CorpusProvenance{OpenAt: CapturedFixedNoMatchPhase}, Recordings: []PrimitiveCall{{Phase: "open", Method: "open"}, {Phase: "fixed", Method: "fixed"}}}
	calls := corpus.PhaseRecordings("open")
	if len(calls) != 1 || calls[0].Method != "fixed" {
		t.Fatalf("negative replay read open evidence: %v", calls)
	}
}
