package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

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

// CheckRepair requires a positive diagnosis and an executed mitigation that
// reaches NoMatch. A skipped source, exec or rollout effect is a failure.
func CheckRepair(ctx context.Context, p program.Diagnoser, m program.Mitigator, c Case) Result {
	c.RepairOnly = true
	return Check(ctx, p, m, nil, c)
}

// CheckMitigationPreview builds and dry-runs a learned plan on isolated opening
// state. Only check phases and action types leave this advisory.
func CheckMitigationPreview(m program.Mitigator, c Case) string {
	ctx := context.Background()
	source, err := newSource(c)
	if err != nil {
		return "accepted source fixture unavailable"
	}
	open := c.Corpus.Open
	calls := c.Corpus.PhaseRecordings("open")
	view := newReadView(ctx, open, calls, source)
	bindings := bindingsFor(m.Spec(), c.Corpus.Bindings)
	plan, err := operator.BuildMitigationPlan(ctx, m, view, bindings, operator.DefaultDiagnosisDeadline, operator.ReplayScriptProvenance(c.Corpus))
	if err != nil {
		return "production plan build failed on recorded inputs"
	}
	client, err := newCluster(open)
	if err != nil {
		return "opening object fixture unavailable"
	}
	incident := operator.IncidentRequest{Namespace: open.NamespaceName, SourceCommit: "offline-accepted", RepairPolicy: "recorded-actions"}
	for ref := range client.initial {
		incident.Findings = append(incident.Findings, sdk.Finding{PrimaryResource: ref})
	}
	executor := operator.PlanExecutor{Client: client, Source: source, Values: program.ValueInputs{Spec: m.Spec(), Bindings: bindings}}
	result, err := executor.PreviewMitigation(ctx, incident, plan)
	if errors.Is(err, program.ErrValuePathUnavailable) {
		return "production dry-run could not resolve an action value path"
	}
	if errors.Is(err, operator.ErrAcceptedManifestAbsent) && len(c.Accepted) == 0 {
		return "production dry-run needs an accepted source fixture"
	}
	if err != nil {
		for _, report := range result.ActionReports {
			if report.FailureReason != "" && report.ActionIndex >= 0 && report.ActionIndex < len(plan.Actions) {
				return fmt.Sprintf("production dry-run unverified at action %d (%T)", report.ActionIndex+1, plan.Actions[report.ActionIndex])
			}
		}
		return "production preview could not encode or dry-run the plan"
	}
	return ""
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
