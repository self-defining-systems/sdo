package runtime

import (
	"reflect"
	"testing"

	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk/program"
)

// The incident ledger persists each program run through the wire contract. A
// NoMatch must keep the reads behind it, or the ledger cannot say which
// condition of a diagnosis returned it.
func TestProgramRunContractCarriesTheDiagnoseReadTrace(t *testing.T) {
	reads := []program.ReadStep{
		{Op: "log_matches", Target: "app/web-1/web", Outcome: "0 lines matched of 812 scanned"},
		{Op: "observe", Target: "app/db-0/db", Outcome: "exit 0"},
	}
	run := ProgramRun{ProgramID: "fix", Verdict: program.Verdict{Result: program.NoMatch, Reads: reads}}
	wired := programRunContract(run)
	if err := wire.Validate(wired); err != nil {
		t.Fatalf("a traced run is not a valid ProgramRun: %v", err)
	}
	if len(wired.GetReadTrace()) != 2 || wired.GetReadTrace()[0].GetOutcome() != "0 lines matched of 812 scanned" {
		t.Fatalf("wire trace = %v", wired.GetReadTrace())
	}
	if got := programRunState(wired).Verdict.Reads; !reflect.DeepEqual(got, reads) {
		t.Fatalf("round trip = %v, want %v", got, reads)
	}
}
