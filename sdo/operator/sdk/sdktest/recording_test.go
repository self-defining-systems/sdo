package sdktest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

type exampleMitigator struct{}

func (exampleMitigator) Spec() program.ProgramSpec {
	return program.ProgramSpec{Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}}}
}
func (exampleMitigator) Mitigate(_ context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
	for _, service := range v.Services() {
		if service.Name == b.Objects["service"].Name && service.Spec.Ports[0].TargetPort.IntVal == 9999 {
			return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: testPlanValue(8080)}}}, nil
		}
	}
	return program.Plan{}, nil
}

type liveReadView struct{ Snapshot }

func (liveReadView) Logs(_ sdk.ObjectRef, _ string, _ time.Duration) (string, error) {
	return "ready", nil
}
func (liveReadView) ProbeTCP(_ sdk.ObjectRef, _ int32) (program.ProbeResult, error) {
	return program.ProbeResult{Success: true}, nil
}
func (liveReadView) ProbeHTTP(_ sdk.ObjectRef, _ program.HTTPProbe) (program.ProbeResult, error) {
	return program.ProbeResult{}, errors.New("refused")
}
func (liveReadView) DiffAgainstSource(_ sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{Changed: true, Text: "source"}, nil
}
func (liveReadView) DiffAgainstBaseline(_ sdk.ObjectRef) (program.Diff, error) {
	return program.Diff{Changed: true, Text: "baseline"}, nil
}

func TestRecordingAndReplayReadView(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Pod", Name: "app", Namespace: "demo"}
	recording := &RecordingReadView{ReadView: liveReadView{Snapshot{NamespaceName: "live"}}}
	if got, err := recording.Logs(ref, "app", time.Minute); got != "ready" || err != nil {
		t.Fatalf("logs: %q, %v", got, err)
	}
	if got, err := recording.ProbeTCP(ref, 8080); !got.Success || err != nil {
		t.Fatalf("TCP: %+v, %v", got, err)
	}
	if _, err := recording.ProbeHTTP(ref, program.HTTPProbe{Method: "GET", Path: "/"}); err == nil {
		t.Fatal("expected HTTP error")
	}
	if got, err := recording.DiffAgainstSource(ref); !got.Changed || err != nil {
		t.Fatalf("source diff: %+v, %v", got, err)
	}
	if got, err := recording.DiffAgainstBaseline(ref); got.Text != "baseline" || err != nil {
		t.Fatalf("baseline diff: %+v, %v", got, err)
	}
	if len(recording.Calls) != 5 {
		t.Fatalf("recorded %d calls", len(recording.Calls))
	}
	replay := ReplayReadView{Snapshot: Snapshot{NamespaceName: "saved"}, Calls: recording.Calls}
	if replay.Namespace() != "saved" {
		t.Fatal("snapshot accessor used live view")
	}
	if got, err := replay.Logs(ref, "app", time.Minute); got != "ready" || err != nil {
		t.Fatalf("replayed logs: %q, %v", got, err)
	}
	if got, err := replay.ProbeTCP(ref, 8080); !got.Success || err != nil {
		t.Fatalf("replayed TCP: %+v, %v", got, err)
	}
	if _, err := replay.ProbeHTTP(ref, program.HTTPProbe{Method: "GET", Path: "/"}); err == nil || err.Error() != "refused" {
		t.Fatalf("replayed HTTP: %v", err)
	}
	if got, _ := replay.DiffAgainstSource(ref); got.Text != "source" {
		t.Fatalf("source: %+v", got)
	}
	if got, _ := replay.DiffAgainstBaseline(ref); got.Text != "baseline" {
		t.Fatalf("baseline: %+v", got)
	}
	if _, err := replay.Logs(ref, "other", time.Minute); err == nil || !strings.Contains(err.Error(), "unrecorded") {
		t.Fatalf("unrecorded call: %v", err)
	}
}

func TestReplayConsumesRepeatedCalls(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Pod", Name: "app"}
	replay := ReplayReadView{Calls: []PrimitiveCall{
		{Method: "Logs", Arguments: []byte(`[{"kind":"Pod","name":"app"},"app",60000000000]`), Result: []byte(`"first"`)},
		{Method: "Logs", Arguments: []byte(`[{"kind":"Pod","name":"app"},"app",60000000000]`), Result: []byte(`"second"`)},
	}}
	first, _ := replay.Logs(ref, "app", time.Minute)
	second, _ := replay.Logs(ref, "app", time.Minute)
	_, err := replay.Logs(ref, "app", time.Minute)
	if first != "first" || second != "second" || err == nil {
		t.Fatalf("replay sequence: %q, %q, %v", first, second, err)
	}
}

func TestLoadCorpusCaseRejectsMissingFiles(t *testing.T) {
	if _, err := LoadCorpusCase(t.TempDir()); err == nil {
		t.Fatal("accepted incomplete case")
	}
}

func TestMitigatorGeneralization(t *testing.T) {
	c, err := LoadCorpusCase("../program/testdata/.sdo/corpus/targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	CheckMitigatorGeneralization(t, exampleMitigator{}, c)
}

func TestConcurrentRecordingAndReplay(t *testing.T) {
	ref := sdk.ObjectRef{Kind: "Pod", Name: "app", Namespace: "demo"}
	var callbacks int
	recording := &RecordingReadView{
		ReadView: liveReadView{Snapshot{NamespaceName: "live"}},
		OnRecord: func(PrimitiveCall) { callbacks++ },
	}
	concurrentProbes := func(view program.ReadView) {
		t.Helper()
		start := make(chan struct{})
		var workers sync.WaitGroup
		for range 32 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				if got, err := view.ProbeTCP(ref, 8080); err != nil || !got.Success {
					t.Errorf("concurrent TCP probe: %+v %v", got, err)
				}
			}()
		}
		close(start)
		workers.Wait()
	}
	concurrentProbes(recording)
	if len(recording.Calls) != 32 || callbacks != 32 {
		t.Fatalf("lost observations: calls=%d callbacks=%d", len(recording.Calls), callbacks)
	}
	replay := &ReplayReadView{Calls: recording.Calls}
	concurrentProbes(replay)
	if _, err := replay.ProbeTCP(ref, 8080); err == nil {
		t.Fatal("concurrent replay did not consume every recorded call exactly once")
	}
}
