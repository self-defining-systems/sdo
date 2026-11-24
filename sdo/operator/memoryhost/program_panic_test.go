package memoryhost

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
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
			server.WithPrograms(registry)
			input := &pb.MemoryProgramInvocation{ProgramId: "panic-fixture", Operation: pb.MemoryProgramInvocation_OPERATION_DIAGNOSE}
			if operation == "mitigate" {
				input.Operation = pb.MemoryProgramInvocation_OPERATION_MITIGATE
			}
			invoke := func(read func(programread.Message) programread.Message) {
				r, err := server.ExecuteProgram(context.Background(), input, read)
				if err != nil {
					t.Fatal(err)
				}
				if operation == "diagnose" && r.GetVerdict().GetResult() != pb.Verdict_RESULT_UNKNOWN {
					t.Fatalf("diagnose panic became success: %v", r)
				}
				if operation == "mitigate" && (!strings.Contains(r.GetError(), "panic") || r.GetPlan() != nil) {
					t.Fatalf("mitigate panic became success: %v", r)
				}
			}
			invoke(func(call programread.Message) programread.Message {
				return programread.Read(panicReads{program.ReadFrom(sdktest.Snapshot{NamespaceName: "app"})}, call)
			})
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
			var captured programPanicCase
			if err := json.Unmarshal([]byte(event.CapturedCase), &captured); err != nil {
				t.Fatal(err)
			}
			if captured.SchemaVersion != "sdo.program-panic/v2" || captured.Truncated || len(captured.Reads) != 1 {
				t.Fatalf("incomplete case: %+v", captured)
			}
			input = new(pb.MemoryProgramInvocation)
			if err := protojson.Unmarshal(captured.Input, input); err != nil {
				t.Fatal(err)
			}
			logs.Reset()
			invoke(func(call programread.Message) programread.Message {
				if !reflect.DeepEqual(call, captured.Reads[0].Call) {
					t.Errorf("replay call = %+v, want %+v", call, captured.Reads[0].Call)
					return programread.Message{Error: "unrecorded call"}
				}
				return captured.Reads[0].Reply
			})
			if !strings.Contains(logs.String(), "synthetic program crash") {
				t.Fatalf("captured inputs did not reproduce panic: %s", logs.String())
			}
		})
	}
}
