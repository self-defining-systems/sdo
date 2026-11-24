package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	pb "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/internal/programread"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
)

type dependentMetricsProgram struct{ runs *atomic.Int32 }

func (dependentMetricsProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: "dependent-metrics"}
}

func (p dependentMetricsProgram) Diagnose(_ context.Context, view program.ReadView, _ program.Bindings) (program.Verdict, error) {
	p.runs.Add(1)
	query := metrics.Query{ID: "same-key", Expr: "1"}
	first := view.QueryMetrics(query)
	if first.Error != "" || len(first.Series) != 1 {
		return program.Verdict{}, fmt.Errorf("first metric: %s", first.Error)
	}
	query.Expr = first.Series[0].Labels["next"]
	second := view.QueryMetrics(query)
	if second.Error != "" || second.Query != query || len(second.Series) != 1 || second.Series[0].Labels["next"] != "3" {
		return program.Verdict{}, fmt.Errorf("dependent metric: %+v", second)
	}
	return program.Verdict{Result: program.Match}, nil
}

// The generated gRPC client and server exchange data-dependent reads on one
// invocation. A query ID shared by different expressions cannot alias them.
func TestMemoryHostProgramStreamsDependentReads(t *testing.T) {
	var runs atomic.Int32
	fixture := dependentMetricsProgram{runs: &runs}
	remote := hostRunning(t, fixture)
	host := remote.remote.rpc.(acceptedReadsHost)
	rpc, _ := memoryTestRPC(t, host.Server)
	response, err := rpc.ExecuteProgram(context.Background(), &pb.MemoryProgramInvocation{
		ProgramId: fixture.Spec().ID, Operation: pb.MemoryProgramInvocation_OPERATION_DIAGNOSE,
	}, func(call programread.Message) programread.Message {
		return programread.Read(dependentMetricsView{}, call)
	})
	if err != nil || response.GetVerdict().GetResult() != pb.Verdict_RESULT_MATCH || runs.Load() != 1 {
		t.Fatalf("diagnosis = %v, %v, executions = %d", response, err, runs.Load())
	}
}

type dependentMetricsView struct{ program.ReadView }

func (dependentMetricsView) QueryMetrics(q metrics.Query) metrics.Result {
	next := map[string]string{"1": "2", "2": "3"}[q.Expr]
	return metrics.Result{Query: q, Series: []metrics.Series{{Labels: map[string]string{"next": next}}}}
}

func TestMemoryHostCancelsProgramWithOutstandingRead(t *testing.T) {
	remote := hostRunning(t, metricProgram{})
	host := remote.remote.rpc.(acceptedReadsHost)
	rpc, _ := memoryTestRPC(t, host.Server)
	invocation := &pb.MemoryProgramInvocation{ProgramId: remote.spec.ID, Operation: pb.MemoryProgramInvocation_OPERATION_DIAGNOSE}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	_, err := rpc.ExecuteProgram(ctx, invocation, func(programread.Message) programread.Message {
		cancel()
		<-release
		return programread.Message{}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel with pending read: %v", err)
	}

	// An oversized requested result becomes an error for this read; the
	// connection remains available to the next invocation.
	response, err := rpc.ExecuteProgram(context.Background(), invocation, func(programread.Message) programread.Message {
		return programread.Message{Values: []json.RawMessage{make([]byte, memoryhost.MaxMessageBytes)}}
	})
	if err != nil || response.GetVerdict().GetResult() != pb.Verdict_RESULT_UNKNOWN {
		t.Fatalf("oversized read: %v, %v", response, err)
	}
	response, err = rpc.ExecuteProgram(context.Background(), invocation, func(call programread.Message) programread.Message {
		return programread.Read(liveMetricsView{}, call)
	})
	if err != nil || response.GetVerdict().GetResult() != pb.Verdict_RESULT_MATCH {
		t.Fatalf("next invocation: %v, %v", response, err)
	}
}
