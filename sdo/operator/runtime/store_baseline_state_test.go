package runtime

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/storestate"
)

var restartKV = storestate.QueryID(storestate.EngineRedis, "kv")

func restartQuery() podread.Query {
	return podread.Query{ID: restartKV, Args: []string{"redis-cli", "info"}}
}

func restartResult(query podread.Query, uid, stdout string) podread.Result {
	return podread.Result{Query: query, Readings: []podread.Reading{{Namespace: "app", Pod: "kv-0", UID: uid, Container: "redis", Stdout: stdout, ObservedAt: time.Unix(1000, 0).UTC()}}}
}

func fixedInputs(inputs *string) func() (string, error) {
	return func() (string, error) { return *inputs, nil }
}

// captureLog routes the default logger to a buffer for the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

// attached is the baseline a current result of the query gets.
func attached(b *PodReadBaseline, current podread.Result) podread.Result {
	results := map[string]podread.Result{current.Query.ID: current}
	b.attach(results)
	return results[current.Query.ID]
}

// A promoted store baseline survives a controller restart: the restarted
// controller's holder loads it from the controller state below the repository
// mount, adopts it on its first reading of the same store Pod, carries it (and
// its healthy history) with the current result at once, and logs the
// promotion harnesses wait for, without a new healthy stretch.
func TestPromotedStoreBaselinesSurviveAControllerRestart(t *testing.T) {
	log := captureLog(t)
	inputs := "source-inputs-1"
	path := StoreBaselineStatePath(t.TempDir())
	designer := podread.Query{ID: "designer-read", Args: []string{"cat", "/etc/app.conf"}}

	before := NewPodReadBaseline(DefaultStateSettle)
	before.State = &StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}
	before.Promote(map[string]podread.Result{
		restartKV:       restartResult(restartQuery(), "uid-kv", syntheticRedis),
		"designer-read": {Query: designer, Readings: []podread.Reading{{Namespace: "app", Pod: "web-0", UID: "uid-web", Stdout: "secret-ish"}}},
	})
	if data, err := os.ReadFile(path); err != nil || strings.Contains(string(data), "secret-ish") {
		t.Fatalf("persisted state = %q, %v; want only store-state reads", data, err)
	}

	after := NewPodReadBaseline(DefaultStateSettle)
	after.Restore(&StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}, func(err error) { t.Fatal(err) })
	log.Reset()
	got := attached(after, restartResult(restartQuery(), "uid-kv", "== info\nrole:master\n"))
	if len(got.Baseline) != 1 || got.Baseline[0].Stdout != syntheticRedis || len(got.History) != 1 {
		t.Fatalf("restarted controller's result = %+v, want the persisted baseline and history", got)
	}
	if !strings.Contains(log.String(), "store_baseline_promoted component=kv") {
		t.Fatalf("no promotion logged for the restored baseline: %q", log.String())
	}
	if got := attached(after, podread.Result{Query: designer, Readings: []podread.Reading{{Pod: "web-0", UID: "uid-web"}}}); len(got.Baseline) != 0 {
		t.Fatalf("a designer pod read was restored: %+v", got)
	}
}

// A restored store baseline is reused only for the same store state it was
// taken from: the same accepted source inputs, the same query declaration and
// the same store Pods (name and UID). Anything else is discarded, and the
// store is baselined again by an ordinary settled healthy stretch.
func TestARestoredStoreBaselineIsNotReusedForAnotherStoreState(t *testing.T) {
	changedQuery := restartQuery()
	changedQuery.Args = []string{"redis-cli", "info", "all"}
	for name, tc := range map[string]struct {
		inputs  string
		current podread.Result
	}{
		"accepted source changed":    {inputs: "source-inputs-2", current: restartResult(restartQuery(), "uid-kv", syntheticRedis)},
		"store Pod replaced":         {inputs: "source-inputs-1", current: restartResult(restartQuery(), "uid-kv-new", syntheticRedis)},
		"store Pod identity unknown": {inputs: "source-inputs-1", current: restartResult(restartQuery(), "", syntheticRedis)},
		"query declaration changed":  {inputs: "source-inputs-1", current: restartResult(changedQuery, "uid-kv", syntheticRedis)},
	} {
		t.Run(name, func(t *testing.T) {
			log := captureLog(t)
			inputs := "source-inputs-1"
			path := StoreBaselineStatePath(t.TempDir())
			before := NewPodReadBaseline(DefaultStateSettle)
			before.State = &StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}
			before.Promote(map[string]podread.Result{restartKV: restartResult(restartQuery(), "uid-kv", "== info\nrole:master\nstale\n")})

			inputs = tc.inputs
			after := NewPodReadBaseline(DefaultStateSettle)
			after.Restore(&StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}, func(err error) { t.Fatal(err) })
			log.Reset()
			if got := attached(after, tc.current); len(got.Baseline) != 0 {
				t.Fatalf("stale baseline reused: %+v", got.Baseline)
			}
			if strings.Contains(log.String(), "store_baseline_promoted") {
				t.Fatalf("a discarded baseline was reported promoted: %q", log.String())
			}
			// The store is baselined again the ordinary way.
			t0 := time.Unix(2000, 0).UTC()
			current := map[string]podread.Result{restartKV: tc.current}
			after.Observe(t0, true, current)
			after.Observe(t0.Add(DefaultStateSettle), true, current)
			if got := attached(after, tc.current); len(got.Baseline) != 1 || got.Baseline[0].Stdout != syntheticRedis {
				t.Fatalf("after a settled stretch = %+v", got.Baseline)
			}
		})
	}
}

// Unreadable state is recomputable: it is reported and the holder starts
// empty.
func TestUnreadableStoreBaselineStateStartsEmpty(t *testing.T) {
	path := StoreBaselineStatePath(t.TempDir())
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	inputs := "source-inputs-1"
	var reported []error
	b := NewPodReadBaseline(DefaultStateSettle)
	b.Restore(&StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}, func(err error) { reported = append(reported, err) })
	if len(reported) != 1 {
		t.Fatalf("reported = %v", reported)
	}
	if got := attached(b, restartResult(restartQuery(), "uid-kv", syntheticRedis)); len(got.Baseline) != 0 {
		t.Fatalf("baseline from unreadable state: %+v", got)
	}
}

// Restore text stays in memory only; an adopted baseline captures it again
// from the live store, where each entry's fact value guards its use.
func TestARestoredStoreBaselineCapturesRestoreTextAgain(t *testing.T) {
	inputs := "source-inputs-1"
	path := StoreBaselineStatePath(t.TempDir())
	mongo := storestate.QueryID(storestate.EngineMongoDB, "docs")
	reading := podread.Result{Query: podread.Query{ID: mongo}, Readings: []podread.Reading{{Namespace: "app", Pod: "docs-0", UID: "uid-docs", Container: "mongo", Stdout: syntheticMongoRead}}}
	before := NewPodReadBaseline(0)
	before.State = &StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}
	before.Promote(map[string]podread.Result{mongo: reading})
	if data, err := os.ReadFile(path); err != nil || strings.Contains(string(data), "jsonSchema") {
		t.Fatalf("persisted state = %q, %v; restore text must stay in memory", data, err)
	}

	var ran []string
	payloads := &StorePayloads{Run: func(_ context.Context, uid string, command program.Command, stdout io.Writer) (int32, error) {
		ran = append(ran, uid+"/"+command.Container)
		_, _ = io.WriteString(stdout, "== payload ejson\nvalidator\tapp\titems\tstrict/error/0a0b0c0d\t{\"$jsonSchema\":{}}\n== end\n")
		return 0, nil
	}}
	after := NewPodReadBaseline(0)
	after.Payloads = payloads
	after.Restore(&StoreBaselineState{Path: path, SourceInputs: fixedInputs(&inputs)}, func(err error) { t.Fatal(err) })
	attached(after, reading)
	payloads.wait()
	if strings.Join(ran, ",") != "uid-docs/mongo" {
		t.Fatalf("ran = %v", ran)
	}
	if _, ok := payloads.Payload("docs", "docs-0"); !ok {
		t.Fatal("no restore text for the restored baseline")
	}
}
