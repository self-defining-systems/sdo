package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// Recorded hotel programs return binding/catalog reasons as resource-free
// evidence. The wire Evidence requires a Resource, so the producer must retain
// those reasons in ProgramRun.error rather than silently discard them.
func TestProgramUnknownReasonsSurviveProtoBoundary(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, fact := range []string{"diagnosis", "source-owned resource binding", "MongoDB catalog target", "restart target"} {
		for _, scoped := range []bool{false, true} {
			evidence := program.Evidence{Fact: fact, Observed: "missing or ambiguous relation", Expected: "one cause-bound object"}
			if scoped {
				evidence.Resource = sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "api"}
			}
			p := candidateProgram{spec: program.ProgramSpec{ID: "unknown-repair"}, run: func(context.Context, program.Bindings) program.Verdict {
				return program.Verdict{Result: program.Unknown, Evidence: []program.Evidence{evidence}}
			}}
			registry, err := program.NewRegistry([]program.Diagnoser{p}, nil)
			if err != nil {
				t.Fatal(err)
			}
			runs := SelectProgramCandidates(context.Background(), registry, &sdktest.ReplayReadView{}, []sdk.Finding{{Status: sdk.FindingActive}}, time.Second)
			if len(runs) != 1 {
				t.Fatalf("fixture lost diagnosis: %+v", runs)
			}
			encoded := programRunContract(runs[0])
			if err := validator.Validate(encoded); err != nil {
				t.Fatal(err)
			}
			raw, err := protojson.Marshal(encoded)
			if err != nil {
				t.Fatal(err)
			}
			decoded := &contracts.ProgramRun{}
			if err := protojson.Unmarshal(raw, decoded); err != nil {
				t.Fatal(err)
			}
			got := programRunState(decoded)
			if got.Verdict.Result != program.Unknown || !strings.Contains(string(raw), evidence.Observed) || (fact != "diagnosis" && !strings.Contains(string(raw), fact)) {
				t.Fatalf("fact=%q scoped=%t: unknown reason disappeared at the real wire boundary: %s", fact, scoped, raw)
			}
		}
	}
}
