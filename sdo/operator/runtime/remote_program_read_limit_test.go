package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/metrics"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type readLimitProgram struct {
	read func(program.ReadView) error
}

func (readLimitProgram) Spec() program.ProgramSpec { return program.ProgramSpec{ID: "read-limit-test"} }
func (p readLimitProgram) Diagnose(_ context.Context, v program.ReadView, _ program.Bindings) (program.Verdict, error) {
	return program.Verdict{Result: program.Match}, p.read(v)
}

type readLimitSource struct {
	program.ReadView
	pods   []corev1.Pod
	metric metrics.Result
	checks map[string]string
	reads  *atomic.Int32
}

func (s readLimitSource) Pods() []corev1.Pod {
	s.reads.Add(1)
	return s.pods
}

func (s readLimitSource) QueryMetrics(metrics.Query) metrics.Result {
	s.reads.Add(1)
	return s.metric
}

func (s readLimitSource) DataChecks() (map[string]string, error) {
	s.reads.Add(1)
	return s.checks, nil
}

func TestOversizedProgramReadCaptureMatchesReplay(t *testing.T) {
	const reason = "program read result exceeds the memory-host message limit"
	large := strings.Repeat("x", memoryhost.MaxMessageBytes)
	query := metrics.Query{ID: "synthetic", Expr: "1"}
	small := metrics.Result{Query: query, Series: []metrics.Series{{Labels: map[string]string{"value": "small"}}}}
	snapshot := sdktest.Snapshot{NamespaceName: "app", Metrics: map[string]metrics.Result{query.ID: small}}
	source := readLimitSource{ReadView: snapshot.RuntimeReadView(), metric: small, reads: new(atomic.Int32)}
	cases := []struct {
		method string
		source readLimitSource
		read   func(program.ReadView) error
		want   program.Result
	}{
		{
			method: "QueryMetrics",
			source: readLimitSource{ReadView: source.ReadView, metric: metrics.Result{Query: query, Series: []metrics.Series{{Labels: map[string]string{"value": large}}}}},
			read: func(v program.ReadView) error {
				r := v.QueryMetrics(query)
				if r.Query != query || r.Error != reason || len(r.Series) != 0 {
					return fmt.Errorf("query shape or error differs: query=%v error=%q series=%d", r.Query, r.Error, len(r.Series))
				}
				return nil
			},
			want: program.Match,
		},
		{
			method: "DataChecks",
			source: readLimitSource{ReadView: source.ReadView, checks: map[string]string{"check": large}},
			read: func(v program.ReadView) error {
				checks, err := v.DataChecks()
				if checks != nil || !errors.Is(err, sdk.ErrUnavailable) || err.Error() != reason {
					return fmt.Errorf("data checks did not return the classified unavailable reply")
				}
				return err
			},
			want: program.Unknown,
		},
		{
			method: "Pods",
			source: readLimitSource{ReadView: source.ReadView, pods: []corev1.Pod{{Spec: corev1.PodSpec{NodeName: large}}}},
			read:   func(v program.ReadView) error { _ = v.Pods(); return nil },
			want:   program.Unknown,
		},
		{
			method: "Pods",
			source: readLimitSource{ReadView: source.ReadView, pods: []corev1.Pod{{Spec: corev1.PodSpec{NodeName: large}}}},
			read: func(v program.ReadView) error {
				func() {
					defer func() { _ = recover() }()
					_ = v.Pods()
				}()
				return nil
			},
			want: program.Unknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			p := readLimitProgram{read: tc.read}
			remote := hostRunning(t, p)
			tc.source.reads = new(atomic.Int32)
			cycle := NewCycleReadView(tc.source)
			sink := &sdktest.RecordingReadView{ReadView: cycle}
			run := func(p program.Diagnoser, v program.ReadView) {
				t.Helper()
				verdict := program.RunDiagnose(context.Background(), p, v, program.Bindings{}, 10*time.Second)
				if verdict.Result != tc.want || !strings.Contains(fmt.Sprint(verdict.Evidence), reason) || strings.Contains(fmt.Sprint(verdict.Evidence), "panic") {
					t.Errorf("diagnosis = %+v, want %s with transport limit evidence", verdict, tc.want)
				}
			}
			run(remote, sink)
			if count := tc.source.reads.Load(); count != 1 {
				t.Errorf("source read %d times, want one", count)
			}
			for _, calls := range [][]sdktest.PrimitiveCall{sink.SnapshotCalls(), cycle.SnapshotCalls()} {
				raw, err := json.Marshal(calls)
				if err != nil || len(calls) != 1 || len(raw) > 1024 || calls[0].Method != tc.method || !strings.Contains(string(raw), reason) {
					t.Errorf("bounded capture of %s: %d calls, %d bytes, marshal error %v", tc.method, len(calls), len(raw), err)
				}
			}
			run(p, &sdktest.ReplayReadView{Snapshot: snapshot, Calls: sink.SnapshotCalls()})
			captured := snapshot
			captured.Readings = cycle.SnapshotCalls()
			run(p, &sdktest.ReplayReadView{Snapshot: captured})
			if tc.method == "Pods" {
				verdict, err := remote.Diagnose(context.Background(), source, program.Bindings{})
				if err != nil || verdict.Result != program.Match {
					t.Errorf("next invocation = %+v, %v", verdict, err)
				}
			}
		})
	}
}
