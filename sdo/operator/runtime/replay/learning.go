package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/planwire"
	operator "sdo.dev/operator/runtime"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// LoadRepairCase uses session-authored observations. Source is explicit when a
// mitigation needs it; no controller capture or capture digest is required.
func LoadRepairCase(dir string) (Case, error) {
	corpus, err := sdktest.LoadCorpusCase(dir)
	if err != nil {
		return Case{}, err
	}
	c := Case{ID: filepath.Base(dir), Corpus: corpus, RepairOnly: true}
	for _, name := range []string{"accepted.json", "accepted.yaml"} {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return c, err
		}
		c.Accepted, err = LoadManifests(path)
		if err != nil {
			return c, err
		}
		break
	}
	return c, nil
}

// ValidateCapturedPlan checks eligibility for unattended execution. Its inputs
// are the positive case's opening state and bindings. Effects and recovery
// are separate replay observations.
func ValidateCapturedPlan(ctx context.Context, p program.Diagnoser, m program.Mitigator, c Case) error {
	if !slices.Contains(c.Corpus.RootCauseProgramIDs, p.Spec().ID) {
		return fmt.Errorf("case is not labeled as a positive example")
	}
	if c.Corpus.Open.NamespaceName == "" {
		return fmt.Errorf("opening snapshot has no namespace")
	}
	source, err := newSource(c)
	if err != nil {
		return err
	}
	bindings := bindingsFor(p.Spec(), c.Corpus.Bindings)
	open := func() *readView {
		return newReadView(ctx, c.Corpus.Open, c.Corpus.PhaseRecordings("open"), source)
	}
	verdict := program.RunDiagnose(ctx, p, open(), bindings, operator.DefaultDiagnosisDeadline)
	if verdict.Result != program.Match {
		return fmt.Errorf("opening Diagnose returned %s: %v", verdict.Result, verdict.Evidence)
	}
	_, err = operator.BuildMitigationPlan(ctx, capturedMitigator{m}, open(), bindings, operator.DefaultDiagnosisDeadline, operator.ReplayScriptProvenance(c.Corpus))
	return err
}

type capturedMitigator struct{ program.Mitigator }

func (m capturedMitigator) Mitigate(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Plan, error) {
	wire, err := memoryhost.BuildProgramPlan(ctx, m.Mitigator, view, bindings)
	if err != nil {
		return program.Plan{}, err
	}
	plan := program.Plan{Rationale: wire.GetRationale()}
	for _, action := range wire.GetActions() {
		plan.Actions = append(plan.Actions, planwire.ActionState(action))
	}
	return plan, plan.Validate()
}

// CheckRepair requires a positive diagnosis and an executed mitigation that
// reaches NoMatch. A skipped source, exec or rollout effect is a failure.
func CheckRepair(ctx context.Context, p program.Diagnoser, m program.Mitigator, c Case) Result {
	c.RepairOnly = true
	return Check(ctx, p, m, nil, c)
}

// A Service field repair cannot change a recorded Pod listener. Reuse only
// successful Pod probe observations whose exact observed object is unchanged.
func unchangedPodProbes(before map[sdk.ObjectRef]*unstructured.Unstructured, after sdktest.Snapshot, calls []sdktest.PrimitiveCall) []sdktest.PrimitiveCall {
	objects, err := snapshotObjects(after)
	if err != nil {
		return nil
	}
	var kept []sdktest.PrimitiveCall
	for _, call := range calls {
		if call.Method != "ProbeTCP" {
			continue
		}
		var args []json.RawMessage
		if json.Unmarshal(call.Arguments, &args) != nil || len(args) == 0 {
			continue
		}
		var ref sdk.ObjectRef
		if json.Unmarshal(args[0], &ref) != nil || ref.Kind != "Pod" {
			continue
		}
		for key, original := range before {
			if key.Kind == ref.Kind && key.Namespace == ref.Namespace && key.Name == ref.Name && objects[key] != nil && reflect.DeepEqual(original.Object, objects[key].Object) {
				kept = append(kept, call)
				break
			}
		}
	}
	return kept
}
