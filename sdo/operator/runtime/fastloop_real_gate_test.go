package runtime

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"testing"
	"time"
)

const fastloopProgramRunner = `package main

import (
	"context"
	"encoding/json"
	"os"
	"app-diagnostics/generated"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func main() {
	var snapshot sdktest.Snapshot
	file, err := os.Open(os.Args[1]); if err != nil { panic(err) }
	if err := json.NewDecoder(file).Decode(&snapshot); err != nil { panic(err) }
	registry, err := generated.Programs(); if err != nil { panic(err) }
	diagnoser, ok := registry.Diagnoser("service-targetport-mismatch"); if !ok { panic("missing compiled diagnoser") }
	mitigator, ok := registry.Mitigator("service-targetport-mismatch"); if !ok { panic("missing compiled mitigator") }
	bindings := program.Bindings{"service": sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "peer"}}
	view := &sdktest.ReplayReadView{Snapshot: snapshot}
	verdict, err := diagnoser.Diagnose(context.Background(), view, bindings); if err != nil { panic(err) }
	plan, err := mitigator.Mitigate(context.Background(), view, bindings); if err != nil { panic(err) }
	if len(plan.Actions) != 1 { panic("expected one action") }
	action, ok := plan.Actions[0].(program.SetField); if !ok { panic("expected SetField") }
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"result": verdict.Result, "path": action.Path, "value": action.Value}); err != nil { panic(err) }
}
`

func copyFastloopTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(from, path)
		if err != nil {
			return err
		}
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func fastloopWriteProgram(t *testing.T, checkout, captureDir, incidentID string) {
	t.Helper()
	base := filepath.Join(checkout, ".sdo/playbooks/service-targetport-mismatch")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"diagnose.go", "mitigate.go"} {
		data, err := os.ReadFile(filepath.Join("../../../benchmarks/sregym/fastloop/scripted_program", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(base, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	readme := fmt.Sprintf("---\nschema_version: 1\nowner: responder\nfault_class: service-targetport-mismatch\noriginating_incident: %s\n---\n\nCheck the selected pod port against the `<SERVICE>` target port.\n", incidentID)
	if err := os.WriteFile(filepath.Join(base, "README.md"), []byte(readme), 0644); err != nil {
		t.Fatal(err)
	}
	index := filepath.Join(checkout, ".sdo/playbooks/README.md")
	entries, err := os.ReadDir(filepath.Dir(index))
	if err != nil {
		t.Fatal(err)
	}
	content := "# Operational playbooks\n\n"
	for _, entry := range entries {
		if entry.IsDir() {
			content += fmt.Sprintf("- [%s](%s/README.md)\n", entry.Name(), entry.Name())
		}
	}
	if err := os.WriteFile(index, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	writeCapturedProgramCorpus(t, checkout, captureDir, incidentID, "service-targetport-mismatch", "service", "Service", "app", "api", "peer", "detect")
}

func fastloopGateResult(request IncidentRequest, resources []sdk.ObjectRef) IncidentResult {
	result := completedResult(request.IncidentID)
	result.Timing.StartedAt = request.DetectorHistory[len(request.DetectorHistory)-1].EvaluatedAt.Add(100 * time.Millisecond)
	result.Timing.CompletedAt = result.Timing.StartedAt.Add(100 * time.Millisecond)
	detectorID := request.Findings[0].DetectorID
	result.ConfirmedRootCauses = []ConfirmedRootCause{{Summary: "Service target port drift", Resources: resources, Evidence: []RootCauseEvidence{{Kind: "detector-finding", Source: detectorID, Observation: "targetPort mismatch"}}, ExplainedDetectors: []string{detectorID}}}
	result.RepairActions = []RepairActionReceipt{{ActionID: "repair", Kind: "kubectl", Target: "app", Summary: "restored source target port", Details: "fake Kubernetes API reports the source port", Resources: resources, StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt, Success: true}}
	result.VerificationEvidence = []VerificationEvidence{{Name: "detector sweep", Passed: true, Details: "drift finding clears", ObservedAt: result.Timing.CompletedAt}}
	return result
}

type fastloopDriftView struct{ driftReadView }

func (v fastloopDriftView) DiffAgainstSource(ref sdk.ObjectRef) (program.Diff, error) {
	if ref.Name == "jaeger" {
		return program.Diff{Changed: true}, nil
	}
	return v.driftReadView.DiffAgainstSource(ref)
}

type fastloopGateDispatch struct {
	requests  chan IncidentRequest
	results   chan IncidentResult
	failFirst bool
	attempts  int
}

func (d *fastloopGateDispatch) Dispatch(ctx context.Context, request IncidentRequest) (IncidentResult, error) {
	d.attempts++
	if d.failFirst && d.attempts == 1 {
		return IncidentResult{}, errors.New("temporary result transport failure")
	}
	select {
	case d.requests <- request:
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
	select {
	case result := <-d.results:
		return result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

func fastloopSameRequest(left, right IncidentRequest) bool {
	left.Findings = append([]sdk.Finding(nil), left.Findings...)
	right.Findings = append([]sdk.Finding(nil), right.Findings...)
	for index := range left.Findings {
		left.Findings[index].Metadata = nil
	}
	for index := range right.Findings {
		right.Findings[index].Metadata = nil
	}
	return sameIncidentRequest(left, right)
}

type fastloopClearDetector struct{}

func (fastloopClearDetector) Spec() sdk.DetectorSpec {
	spec := fastloopDetector{}.Spec()
	return spec
}

func (fastloopClearDetector) Detect(context.Context, sdk.DetectionContext) ([]sdk.Finding, error) {
	return nil, nil
}
