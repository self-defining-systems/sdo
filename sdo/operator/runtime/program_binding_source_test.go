package runtime

import (
	"context"
	"slices"
	"testing"
	"time"

	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// Every binding source a producer can emit is persisted in the incident request
// and checked by the wire contract; a value the contract rejects fails the
// controller handoff, so the constants and the proto rule must agree.
func TestEveryBindingSourceIsAValidProgramRunContract(t *testing.T) {
	for _, source := range BindingSources {
		run := ProgramRun{ProgramID: "fix", BindingSource: source, Verdict: program.Verdict{Result: program.Match}}
		if err := wire.Validate(programRunContract(run)); err != nil {
			t.Errorf("binding source %q is rejected by the ProgramRun contract: %v", source, err)
		}
	}
}

// The producers emit only declared sources, and the source-restore candidate
// (bound by suspect) and the credited and finding bindings all reach the wire.
func TestCandidateProducersEmitOnlyDeclaredBindingSources(t *testing.T) {
	registry := keyedRegistry(t, matchAll, "fix", "credited-fix", SourceRestoreID)
	findings := []sdk.Finding{keyedFinding("front")}
	keyed := KeyedMatching{
		Credits:     credits("credited-fix", "k1", "front"),
		OpeningKeys: []string{"k1"},
		Restore:     []sourceRestoreCandidate{{bindings: program.Bindings{Objects: program.ObjectBindings{ComponentRole: keyedDeployment("front")}}, fingerprint: "browse"}},
	}
	runs := SelectKeyedProgramCandidates(context.Background(), registry, keyedView("front"), findings, time.Second, keyed)
	seen := map[string]bool{}
	for _, run := range runs {
		seen[run.BindingSource] = true
		if !slices.Contains(BindingSources, run.BindingSource) {
			t.Errorf("%s emitted undeclared binding source %q", run.ProgramID, run.BindingSource)
		}
		if err := wire.Validate(programRunContract(run)); err != nil {
			t.Errorf("%s run with source %q fails the contract: %v", run.ProgramID, run.BindingSource, err)
		}
	}
	for _, want := range []string{BindingSourceFinding, BindingSourceSuspect, BindingSourceCredited} {
		if !seen[want] {
			t.Errorf("producers never emitted %q; runs: %+v", want, runs)
		}
	}
}
