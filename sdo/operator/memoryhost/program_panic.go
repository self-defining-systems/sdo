package memoryhost

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime/debug"

	"google.golang.org/protobuf/encoding/protojson"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

// The snapshot is the one used to construct the view, after observation overlay.
// Retaining it avoids a second mirror lookup racing history eviction on panic.
type programRecording struct {
	*sdktest.RecordingReadView
	snapshot sdktest.Snapshot
}

type programPanicCase struct {
	SchemaVersion string                  `json:"schema_version"`
	Operation     string                  `json:"operation"`
	Input         json.RawMessage         `json:"input"`
	Snapshot      sdktest.Snapshot        `json:"snapshot"`
	Calls         []sdktest.PrimitiveCall `json:"calls"`
}

// Record at the host boundary, before unwinding loses the program's frames.
// Input is ProtoJSON (including bindings, observation and prefetched reads);
// Snapshot and Calls replay with sdktest.ReplayReadView. The host's stderr is
// retained with controller logs; this never publishes unvalidated corpus files.
func (s *Server) recordProgramPanic(operation string, input *pb.MemoryProgramInput, view *programRecording, value any) {
	raw, inputErr := protojson.Marshal(input)
	captured := programPanicCase{SchemaVersion: "sdo.program-panic/v1", Operation: operation, Input: raw}
	if view != nil {
		captured.Snapshot = view.snapshot
		captured.Calls = view.SnapshotCalls()
	}
	data, captureErr := json.Marshal(captured)
	slog.Error("memory_program_panic", "operation", operation, "program_id", input.GetProgramId(), "memory_commit", s.identity.GetMemoryCommit(), "binary_digest", s.identity.GetBinaryDigest(), "panic", value, "stack", string(debug.Stack()), "captured_case", string(data), "input_error", inputErr, "capture_error", captureErr)
}

// RunDiagnose contains panics in its own goroutine and converts them to Unknown.
// Record inside that boundary; the transport's outer recover cannot see them.
type recordingDiagnoser struct {
	program.Diagnoser
	record func(any)
}

func (p recordingDiagnoser) Diagnose(ctx context.Context, view program.ReadView, bindings program.Bindings) (program.Verdict, error) {
	defer func() {
		if value := recover(); value != nil {
			p.record(value)
			panic(value)
		}
	}()
	return p.Diagnoser.Diagnose(ctx, view, bindings)
}

func (p recordingDiagnoser) Spec() program.ProgramSpec {
	defer func() {
		if value := recover(); value != nil {
			p.record(value)
			panic(value)
		}
	}()
	return p.Diagnoser.Spec()
}
