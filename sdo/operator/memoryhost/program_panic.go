package memoryhost

import (
	"context"
	"encoding/json"
	"log/slog"
	"runtime/debug"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/sdk/program"
)

// Keep the requested inputs for panic replay. The trace is bounded independently
// of the RPC so retaining a large read cannot consume the host's memory budget.
const maxPanicTraceBytes = 1 << 20
const maxPanicTraceReads = 256

type programPanicRead struct {
	Call  programread.Message `json:"call"`
	Reply programread.Message `json:"reply"`
}

type programPanicTrace struct {
	mu        sync.Mutex
	bytes     int
	truncated bool
	reads     []programPanicRead
}

func (t *programPanicTrace) add(call, reply programread.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.truncated {
		return
	}
	size := len(call.Method) + len(reply.Error) + len(reply.ErrorClass)
	if reply.UsageUnavailable != nil {
		size += len(reply.UsageUnavailable.Reason)
	}
	for _, arg := range call.Arguments {
		size += len(arg)
	}
	for _, value := range reply.Values {
		size += len(value)
	}
	if len(t.reads) >= maxPanicTraceReads || t.bytes+size > maxPanicTraceBytes {
		t.truncated = true
		return
	}
	t.bytes += size
	t.reads = append(t.reads, programPanicRead{Call: call, Reply: reply})
}

type programPanicCase struct {
	SchemaVersion string             `json:"schema_version"`
	Input         json.RawMessage    `json:"input"`
	Reads         []programPanicRead `json:"reads"`
	Truncated     bool               `json:"truncated,omitempty"`
}

// Record inside the child, before unwinding loses the program's frames.
func (s *Server) recordProgramPanic(invocation *pb.MemoryProgramInvocation, trace *programPanicTrace, value any) {
	raw, inputErr := protojson.Marshal(invocation)
	trace.mu.Lock()
	data, captureErr := json.Marshal(programPanicCase{SchemaVersion: "sdo.program-panic/v2", Input: raw, Reads: trace.reads, Truncated: trace.truncated})
	trace.mu.Unlock()
	slog.Error("memory_program_panic", "operation", invocation.GetOperation().String(), "program_id", invocation.GetProgramId(), "memory_commit", s.identity.GetMemoryCommit(), "binary_digest", s.identity.GetBinaryDigest(), "panic", value, "stack", string(debug.Stack()), "captured_case", string(data), "input_error", inputErr, "capture_error", captureErr)
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
