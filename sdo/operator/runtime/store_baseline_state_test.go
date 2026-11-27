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
	path := StoreBaselineStatePath(t.TempDir())
	designer := podread.Query{ID: "designer-read", Args: []string{"cat", "/etc/app.conf"}}

	before := NewPodReadBaseline(DefaultStateSettle)
	before.State = &StoreBaselineState{Path: path}
	before.Promote(map[string]podread.Result{
		restartKV:       restartResult(restartQuery(), "uid-kv", syntheticRedis),
		"designer-read": {Query: designer, Readings: []podread.Reading{{Namespace: "app", Pod: "web-0", UID: "uid-web", Stdout: "secret-ish"}}},
	})
	if data, err := os.ReadFile(path); err != nil || strings.Contains(string(data), "secret-ish") {
		t.Fatalf("persisted state = %q, %v; want only store-state reads", data, err)
	}

	after := NewPodReadBaseline(DefaultStateSettle)
	after.Restore(&StoreBaselineState{Path: path}, func(err error) { t.Fatal(err) })
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

// A restored store baseline is reused only for the same query declaration,
// the rule an in-memory reference follows (attach). Another declaration reads
// other facts, so the record is discarded and the store is baselined again by
// an ordinary settled healthy stretch.
func TestARestoredStoreBaselineIsNotReusedForAnotherQuery(t *testing.T) {
	changedQuery := restartQuery()
	changedQuery.Args = []string{"redis-cli", "info", "all"}
	for name, tc := range map[string]struct {
		current podread.Result
	}{
		"query declaration changed": {current: restartResult(changedQuery, "uid-kv", syntheticRedis)},
	} {
		t.Run(name, func(t *testing.T) {
			log := captureLog(t)
			path := StoreBaselineStatePath(t.TempDir())
			before := NewPodReadBaseline(DefaultStateSettle)
			before.State = &StoreBaselineState{Path: path}
			before.Promote(map[string]podread.Result{restartKV: restartResult(restartQuery(), "uid-kv", "== info\nrole:master\nstale\n")})

			after := NewPodReadBaseline(DefaultStateSettle)
			after.Restore(&StoreBaselineState{Path: path}, func(err error) { t.Fatal(err) })
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
	var reported []error
	b := NewPodReadBaseline(DefaultStateSettle)
	b.Restore(&StoreBaselineState{Path: path}, func(err error) { reported = append(reported, err) })
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
	path := StoreBaselineStatePath(t.TempDir())
	mongo := storestate.QueryID(storestate.EngineMongoDB, "docs")
	reading := podread.Result{Query: podread.Query{ID: mongo}, Readings: []podread.Reading{{Namespace: "app", Pod: "docs-0", UID: "uid-docs", Container: "mongo", Stdout: syntheticMongoRead}}}
	before := NewPodReadBaseline(0)
	before.State = &StoreBaselineState{Path: path}
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
	after.Restore(&StoreBaselineState{Path: path}, func(err error) { t.Fatal(err) })
	attached(after, reading)
	payloads.wait()
	if strings.Join(ran, ",") != "uid-docs/mongo" {
		t.Fatalf("ran = %v", ran)
	}
	if _, ok := payloads.Payload("docs", "docs-0"); !ok {
		t.Fatal("no restore text for the restored baseline")
	}
}

// An accepted change (a new source revision, an upgraded store image that
// replaced the store Pod) does not blind the store detector: the restarted
// controller keeps the last promoted healthy reference, as a running one does,
// until a settled healthy stretch of the new deployment replaces it, and
// captures restore text from the store Pod that runs now.
func TestARestoredStoreBaselineSurvivesAnAcceptedChange(t *testing.T) {
	log := captureLog(t)
	path := StoreBaselineStatePath(t.TempDir())
	mongo := storestate.QueryID(storestate.EngineMongoDB, "docs")
	reading := func(uid, stdout string) podread.Result {
		return podread.Result{Query: podread.Query{ID: mongo}, Readings: []podread.Reading{{Namespace: "app", Pod: "docs-0", UID: uid, Container: "mongo", Stdout: stdout, ObservedAt: time.Unix(1000, 0).UTC()}}}
	}
	before := NewPodReadBaseline(0)
	before.State = &StoreBaselineState{Path: path}
	before.Promote(map[string]podread.Result{mongo: reading("uid-docs", syntheticMongoRead)})

	var ran []string
	payloads := &StorePayloads{Run: func(_ context.Context, uid string, command program.Command, stdout io.Writer) (int32, error) {
		ran = append(ran, uid+"/"+command.Container)
		_, _ = io.WriteString(stdout, "== payload ejson\nvalidator\tapp\titems\tstrict/error/0a0b0c0d\t{\"$jsonSchema\":{}}\n== end\n")
		return 0, nil
	}}
	after := NewPodReadBaseline(0)
	after.Payloads = payloads
	after.Restore(&StoreBaselineState{Path: path}, func(err error) { t.Fatal(err) })
	log.Reset()
	current := reading("uid-docs-new", syntheticMongoRead)
	got := attached(after, current)
	payloads.wait()
	if len(got.Baseline) != 1 || got.Baseline[0].UID != "uid-docs" {
		t.Fatalf("after an accepted change the store has baseline %+v, want the last promoted one", got.Baseline)
	}
	if !strings.Contains(log.String(), "store_baseline_promoted component=docs") {
		t.Fatalf("no promotion logged for the kept baseline: %q", log.String())
	}
	if strings.Join(ran, ",") != "uid-docs-new/mongo" {
		t.Fatalf("restore text captured from %v, want the store Pod that runs now", ran)
	}
	// A settled healthy stretch of the new deployment replaces the kept reference.
	t0 := time.Unix(2000, 0).UTC()
	after.Observe(t0, true, map[string]podread.Result{mongo: current})
	after.Observe(t0.Add(DefaultStateSettle), true, map[string]podread.Result{mongo: current})
	if got := attached(after, current); len(got.Baseline) != 1 || got.Baseline[0].UID != "uid-docs-new" {
		t.Fatalf("after a settled stretch = %+v, want the new deployment's reading", got.Baseline)
	}
}
