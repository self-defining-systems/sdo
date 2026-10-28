package validate

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

const workedExamples = "../../../skills/sdo-playbook-authoring/assets/examples"

// The authoring skill points reflection at these worked examples, so they pass
// the same reflection check a learned playbook must: compile, format, labels,
// receipt links and an open-phase Match on their own corpus case. One tree and
// one check cover every example, so a new example costs no extra compile.
func TestWorkedExamplesPassTheReflectionMemoryCheck(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join(workedExamples, "playbooks"))
	if err != nil {
		t.Fatal(err)
	}
	root := goodTree(t)
	copyTree(t, "../../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod", filepath.Join(root, ".sdo/diagnostics/go.mod"))
	outcome := &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "worked-example", Classification: "success"}
	var ids []string
	for _, entry := range entries {
		ids = append(ids, entry.Name())
	}
	cases, err := os.ReadDir(filepath.Join(workedExamples, "corpus"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		meta, err := sdktest.LoadCorpusMetadata(filepath.Join(workedExamples, "corpus", c.Name()))
		if err != nil {
			t.Fatalf("example case %s: %v", c.Name(), err)
		}
		for _, call := range meta.Recordings {
			if call.Method != "RepairReceipt" {
				continue
			}
			var link program.ScriptOriginReceipt
			if err := json.Unmarshal(call.Result, &link); err != nil {
				t.Fatal(err)
			}
			receipt := &contracts.RepairActionReceipt{ActionId: link.ID, Kind: "sdo-act", Target: c.Name(), Summary: "worked example repair", Details: "synthetic", Success: true, ScriptDigest: link.ScriptDigest, StartedAt: timestamppb.New(link.StartedAt), CompletedAt: timestamppb.New(link.CompletedAt)}
			for _, ref := range link.Resources {
				receipt.Resources = append(receipt.Resources, &contracts.ObjectRef{ApiVersion: ref.APIVersion, Kind: ref.Kind, Namespace: ref.Namespace, Name: ref.Name})
			}
			if link.ScriptDigest != "" {
				receipt.Kind = "run-script"
			}
			outcome.RepairActions = append(outcome.RepairActions, receipt)
			outcome.ConfirmedRootCauses = append(outcome.ConfirmedRootCauses, &contracts.ConfirmedRootCause{Summary: c.Name(), Resources: receipt.Resources})
		}
	}
	raw, err := protojson.Marshal(outcome)
	if err != nil {
		t.Fatal(err)
	}
	write(t, root, layout.OutcomePath(outcome.IncidentId), string(raw)+"\n")
	commit(t, root, "accepted outcome\n\nSDO-Incident: worked-example\nSDO-Phase: outcome")
	base := gitIn(t, root, "rev-parse", "HEAD")

	for _, part := range []string{"playbooks", "corpus"} {
		copyTree(t, filepath.Join(workedExamples, part), filepath.Join(root, ".sdo", part))
	}
	index := "# Playbooks\n\n- [base](base/README.md)\n"
	for _, id := range ids {
		index += "- [" + id + "](" + id + "/README.md)\n"
	}
	write(t, root, ".sdo/playbooks/README.md", index)
	paths, err := ChangedPaths(context.Background(), root, base)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NormalizeAuthoredPlaybooks(root, paths); err != nil {
		t.Fatal(err)
	}
	var notes []sdktest.MitigationReplayResult
	var gaps []Diagnostic
	decision := &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: ids}
	ds, err := Check(context.Background(), Request{CoverageGaps: &gaps, MitigationReplayNotes: &notes, Root: root, Base: base, Actor: Responder, Phase: ReflectionPhase, Decision: decision, Publication: &PublicationContext{BaseCommit: base, IncidentID: outcome.IncidentId}, OperatorRoot: filepath.Join("..", "..")}, CompileCandidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 0 {
		t.Fatalf("worked examples fail the reflection memory check: %+v", ds)
	}
	for _, id := range ids {
		if !slices.ContainsFunc(notes, func(n sdktest.MitigationReplayResult) bool {
			return n.ProgramID == id && slices.Contains(n.Effects, "open diagnosis: Match")
		}) {
			var mine []string
			for _, n := range notes {
				if n.ProgramID == id {
					mine = append(mine, string(n.Status)+": "+strings.Join(n.Effects, "; "))
				}
			}
			t.Errorf("example %s does not Match its own open capture: %v", id, mine)
		}
	}
}
