package runtime

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

var openCaptureName = regexp.MustCompile(`open@([a-z0-9*][a-z0-9*-]*)`)

// producedOpenPhases lists every open capture phase SDO can name: each one a
// string literal of the operator's source spells ("open@<phase>"), and the
// numbered alert-join phases.
func producedOpenPhases(t *testing.T) []string {
	t.Helper()
	phases := []string{sdktest.JoinCapturePhase(1), sdktest.JoinCapturePhase(12)}
	err := filepath.WalkDir("..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == "testdata" {
			return filepath.SkipDir
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.HasSuffix(path, ".pb.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatalf("%s: %v", path, err)
			}
			for _, match := range openCaptureName.FindAllStringSubmatch(value, -1) {
				phase := strings.TrimSuffix(match[1], "-recordings")
				if !strings.Contains(phase, "*") { // a glob over captures
					phases = append(phases, phase)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(phases)
	return slices.Compact(phases)
}

func roundTripCapturePhase(t *testing.T, phase string) error {
	t.Helper()
	broker := NativeIncidentBroker{LedgerRoot: t.TempDir()}
	ledger := &contracts.BrokerLedger{IncidentId: "synthetic", OutcomeDocument: &contracts.OutcomeRecord{
		SchemaVersion: 1, IncidentId: "synthetic", Classification: "success",
		ReflectionEligibility: &contracts.ReflectionEligibility{Version: 1, Unassigned: []*contracts.ReflectionWorkItem{
			{ProgramId: "later", Disposition: "question", Reason: "uncertain", CaptureOpenPhase: phase},
		}},
	}}
	if err := broker.saveLedger(ledger); err != nil {
		return err
	}
	read, err := broker.readLedger("synthetic")
	if err != nil {
		return err
	}
	if got := read.GetOutcomeDocument().GetReflectionEligibility().GetUnassigned()[0].GetCaptureOpenPhase(); got != phase {
		t.Fatalf("capture_open_phase = %q after the round trip, want %q", got, phase)
	}
	return nil
}

// Every open capture phase the operator names is one the broker ledger stores.
//
//ci:whole-tree
func TestEveryOpenCapturePhaseRoundTripsThroughTheBrokerLedger(t *testing.T) {
	phases := producedOpenPhases(t)
	for _, want := range []string{"detect", "pre-apply", "join-1"} {
		if !slices.Contains(phases, want) {
			t.Fatalf("the producer scan missed %q: %v", want, phases)
		}
	}
	// An item with no open capture leaves the field empty.
	for _, phase := range append(phases, "") {
		if err := roundTripCapturePhase(t, phase); err != nil {
			t.Errorf("phase %q: %v", phase, err)
		}
	}
}

// The settled origin of a program that matches only in a join capture is a
// value the ledger accepts: the incident's closure can be written.
func TestAJoinCaptureOriginCanBeStoredInTheLedger(t *testing.T) {
	registry, err := program.NewRegistry([]program.Diagnoser{joinedFault{}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	outcome := &contracts.OutcomeRecord{ProgramRuns: []*contracts.ProgramRun{{ProgramId: "later", Verdict: &contracts.Verdict{Result: contracts.Verdict_RESULT_MATCH}}}}
	origins := map[string]string{}
	fixedCaptureVerdicts(context.Background(), outcome, registry, joinCapture(t), origins)
	if err := roundTripCapturePhase(t, origins[matchedViewKey("later", program.Bindings{})]); err != nil {
		t.Fatal(err)
	}
}

func TestTheBrokerLedgerRejectsANameThatIsNoOpenCapture(t *testing.T) {
	for _, phase := range []string{"open@detect", "fixed", "join-0", "join-", "join-1x"} {
		if err := roundTripCapturePhase(t, phase); err == nil || !strings.Contains(err.Error(), "capture_open_phase") {
			t.Errorf("phase %q: error = %v, want a capture_open_phase violation", phase, err)
		}
	}
}
