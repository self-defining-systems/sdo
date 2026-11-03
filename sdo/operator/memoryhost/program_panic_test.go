package memoryhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type panicFixture struct{}

func (panicFixture) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "panic-fixture"} }
func (panicFixture) fail(v program.ReadView) error {
	text, err := v.Logs(sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "demo"}, "app", time.Second)
	if err != nil || text != "synthetic log" {
		return fmt.Errorf("read mismatch: %q: %v", text, err)
	}
	panic("synthetic program crash")
}
func (p panicFixture) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	return program.Verdict{}, p.fail(v)
}
func (p panicFixture) Mitigate(_ context.Context, v program.ReadView, _ program.Bindings) (program.Plan, error) {
	return program.Plan{}, p.fail(v)
}

type panicReads struct{ program.ReadView }

func (panicReads) Logs(sdk.ObjectRef, string, time.Duration) (string, error) {
	return "synthetic log", nil
}

func TestProgramPanicRecordsReplayableInputsAndStack(t *testing.T) {
	for _, operation := range []string{"diagnose", "mitigate"} {
		t.Run(operation, func(t *testing.T) {
			var logs bytes.Buffer
			prior := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
			defer slog.SetDefault(prior)
			server, err := NewServer("accepted", "binary", nil)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := program.NewRegistry([]program.Diagnoser{panicFixture{}}, []program.Mitigator{panicFixture{}})
			if err != nil {
				t.Fatal(err)
			}
			server.WithPrograms(registry).WithReadFactory(func(_ context.Context, s sdktest.Snapshot) (program.ReadView, error) {
				return panicReads{program.ReadFrom(s)}, nil
			})
			if err := server.Apply(&pb.MirrorRequest{Namespace: "app", Sequence: 1, Complete: true, ResetMirror: true}); err != nil {
				t.Fatal(err)
			}
			input := &pb.MemoryProgramInput{ProgramId: "panic-fixture", MirrorSequence: 1}
			invoke := func() {
				if operation == "diagnose" {
					r, e := server.Diagnose(context.Background(), &pb.DiagnoseRequest{Input: input})
					if e != nil || r.GetVerdict().GetResult() != pb.Verdict_RESULT_UNKNOWN {
						t.Fatalf("panic became success: %v %v", r, e)
					}
				} else {
					r, e := server.Mitigate(context.Background(), &pb.MitigateRequest{Input: input})
					if e != nil || !strings.Contains(r.GetError(), "panic") || r.Plan != nil {
						t.Fatalf("panic became success: %v %v", r, e)
					}
				}
			}
			invoke()
			var event struct {
				Stack        string `json:"stack"`
				CapturedCase string `json:"captured_case"`
			}
			if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &event); err != nil {
				t.Fatalf("panic had no structured capture: %s: %v", logs.String(), err)
			}
			if !strings.Contains(event.Stack, "panicFixture.fail") {
				t.Fatalf("missing program stack: %s", event.Stack)
			}
			var captured struct {
				Operation string
				Input     json.RawMessage
				Snapshot  sdktest.Snapshot
				Calls     []sdktest.PrimitiveCall
			}
			if err := json.Unmarshal([]byte(event.CapturedCase), &captured); err != nil {
				t.Fatal(err)
			}
			if captured.Operation != operation || captured.Snapshot.NamespaceName != "app" || len(captured.Calls) != 1 {
				t.Fatalf("incomplete case: %+v", captured)
			}
			input = new(pb.MemoryProgramInput)
			if err := protojson.Unmarshal(captured.Input, input); err != nil {
				t.Fatal(err)
			}
			server.WithReadFactory(func(context.Context, sdktest.Snapshot) (program.ReadView, error) {
				return &sdktest.ReplayReadView{Snapshot: captured.Snapshot, Calls: captured.Calls}, nil
			})
			logs.Reset()
			invoke()
			if !strings.Contains(logs.String(), "synthetic program crash") {
				t.Fatalf("captured inputs did not reproduce panic: %s", logs.String())
			}
		})
	}
}
