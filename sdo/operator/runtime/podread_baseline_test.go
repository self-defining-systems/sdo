package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

func readingsSnapshot(query podread.Query, stdout ...string) sdktest.Snapshot {
	result := podread.Result{Query: query}
	for i, out := range stdout {
		result.Readings = append(result.Readings, podread.Reading{Namespace: "app", Pod: string(rune('a' + i)), Stdout: out})
	}
	return sdktest.Snapshot{NamespaceName: "app", PodReads: map[string]podread.Result{query.ID: result}}
}

func storeQuery() podread.Query { return storeReadDetector{}.PodReads()[0] }

func promoted(b *PodReadBaseline, query podread.Query) []string {
	results := map[string]podread.Result{query.ID: {Query: query}}
	b.attach(results)
	var out []string
	for _, r := range results[query.ID].Baseline {
		out = append(out, r.Stdout)
	}
	return out
}

// The pod-read baseline settles in every mode, with or without the
// configuration baseline (--state-baseline): the readings at the start of a
// healthy stretch become the reference only once the stretch lasted
// DefaultStateSettle, so neither the first clear evaluation nor a reading
// taken during a fault still below its firing threshold is absorbed. A
// restarted controller starts with an empty holder and waits the same way.
func TestPodReadBaselineSettlesInEveryMode(t *testing.T) {
	for name, baseline := range map[string]StateBaseline{"without state baseline": nil, "with state baseline": &boundaryBaseline{}} {
		t.Run(name, func(t *testing.T) {
			t0 := time.Unix(1000, 0).UTC()
			at := func(d time.Duration) time.Time { return t0.Add(d) }
			c := &Controller{Baseline: baseline, PodReadBaseline: NewPodReadBaseline(DefaultStateSettle)}
			query := storeQuery()

			c.promotePodReadBaseline(t0, readingsSnapshot(query, "first"), true)
			c.promotePodReadBaseline(at(time.Minute), readingsSnapshot(query, "drifted"), true)
			if got := promoted(c.PodReadBaseline, query); len(got) != 0 {
				t.Fatalf("promoted before the stretch settled: %v", got)
			}
			c.promotePodReadBaseline(at(DefaultStateSettle), readingsSnapshot(query, "settled"), true)
			if got := promoted(c.PodReadBaseline, query); len(got) != 1 || got[0] != "first" {
				t.Fatalf("settled baseline = %v, want the readings at the start of the stretch", got)
			}
			// An unhealthy evaluation ends the stretch: the next candidate
			// starts again and must settle again.
			c.promotePodReadBaseline(at(DefaultStateSettle+time.Minute), readingsSnapshot(query, "bad"), false)
			c.promotePodReadBaseline(at(DefaultStateSettle+2*time.Minute), readingsSnapshot(query, "again"), true)
			c.promotePodReadBaseline(at(2*DefaultStateSettle+time.Minute), readingsSnapshot(query, "x"), true)
			if got := promoted(c.PodReadBaseline, query); got[0] != "first" {
				t.Fatalf("a candidate promoted across an unhealthy evaluation: %v", got)
			}
			c.promotePodReadBaseline(at(2*DefaultStateSettle+2*time.Minute), readingsSnapshot(query, "y"), true)
			if got := promoted(c.PodReadBaseline, query); got[0] != "again" {
				t.Fatalf("second settled baseline = %v", got)
			}
		})
	}
}

// Every settled healthy stretch adds its readings to the query's history,
// carried with each current result; a stretch that ended unhealthy adds
// none, and the history stays bounded per Pod while spanning the whole
// healthy past.
func TestPodReadBaselineCarriesTheSettledHealthyHistory(t *testing.T) {
	t0 := time.Unix(1000, 0).UTC()
	query := storeQuery()
	b := NewPodReadBaseline(DefaultStateSettle)
	observe := func(i int, healthy bool) {
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		b.Observe(at, healthy, map[string]podread.Result{query.ID: {Query: query, Readings: []podread.Reading{{Namespace: "app", Pod: "a", ObservedAt: at, Stdout: fmt.Sprintf("r%d", i)}}}})
	}
	history := func() []string {
		results := map[string]podread.Result{query.ID: {Query: query}}
		b.attach(results)
		var out []string
		for _, r := range results[query.ID].History {
			out = append(out, r.Stdout)
		}
		return out
	}
	for i := 0; i <= 4; i++ { // r0..r3 settle at r4
		observe(i, true)
	}
	if got := strings.Join(history(), ","); got != "r0,r1,r2,r3" {
		t.Fatalf("history after one settled stretch = %s", got)
	}
	observe(5, true)
	observe(6, false) // r4, r5 never settle
	if got := strings.Join(history(), ","); got != "r0,r1,r2,r3" {
		t.Fatalf("an unhealthy stretch entered the history: %s", got)
	}
	for i := 7; i <= 60; i++ {
		observe(i, true)
	}
	got := history()
	// Stretches settle every four readings from r7; the last settled one is
	// r55..r58 (r59, r60 are the open candidate).
	if len(got) != podread.HistoryPerPod || got[0] != "r0" || got[len(got)-1] != "r58" {
		t.Fatalf("history = %v, want %d readings from r0 to r58", got, podread.HistoryPerPod)
	}
	if slices.Contains(got, "r4") || slices.Contains(got, "r5") {
		t.Fatalf("history kept a reading of the unhealthy stretch: %v", got)
	}
}

// boundaryBaseline is a StateBaseline that adopts its first observation at
// once, as the StateTracker does; the pod-read settle does not follow it.
type boundaryBaseline struct{ baselineAt time.Time }

func (b *boundaryBaseline) Observe(now time.Time, healthy bool) {
	if healthy && b.baselineAt.IsZero() {
		b.baselineAt = now
	}
}
func (b *boundaryBaseline) Changes(time.Time) *StateChanges { return nil }
func (b *boundaryBaseline) BaselineAt() time.Time           { return b.baselineAt }
func (b *boundaryBaseline) BaselineCandidateAt() time.Time  { return time.Time{} }

// A store read with a refusal never becomes the reference, however long
// health lasted: the store stays unobserved (a coverage gap), and a store
// that was observed keeps its earlier healthy reference, so a later refusal
// is still compared with it and fires.
func TestPodReadBaselineNeverPromotesARefusedStoreRead(t *testing.T) {
	id := storestate.QueryID(storestate.EngineRedis, "kv")
	query := podread.Query{ID: id}
	refused := ""
	for _, section := range []string{"info", "dbsize", "role", "config", "acl"} {
		refused += "== " + section + "\nNOAUTH Authentication required.\n"
	}
	b := NewPodReadBaseline(DefaultStateSettle)
	b.Promote(map[string]podread.Result{id: readingsSnapshot(query, refused).PodReads[id]})
	if got := promoted(b, query); len(got) != 0 {
		t.Fatalf("a refused store read became the baseline: %v", got)
	}
	b.Promote(map[string]podread.Result{id: readingsSnapshot(query, syntheticRedis).PodReads[id]})
	b.Promote(map[string]podread.Result{id: readingsSnapshot(query, refused).PodReads[id]})
	b.Promote(map[string]podread.Result{id: readingsSnapshot(query, syntheticRedis, refused).PodReads[id]})
	if got := promoted(b, query); len(got) != 1 || got[0] != syntheticRedis {
		t.Fatalf("baseline = %v, want the earlier healthy read", got)
	}
}

// Only complete readings form a baseline, a query whose readings all failed
// keeps its earlier reference, and a changed declaration gets none.
func TestPodReadBaselineKeepsOnlyCompleteReadingsOfTheSameQuery(t *testing.T) {
	query := storeQuery()
	b := NewPodReadBaseline(DefaultStateSettle)
	b.Promote(map[string]podread.Result{query.ID: {Query: query, Readings: []podread.Reading{{Pod: "a", Stdout: "good"}, {Pod: "b", Error: "exec timed out"}, {Pod: "c", Stdout: "partial", Truncated: true}}}})
	if got := promoted(b, query); len(got) != 1 || got[0] != "good" {
		t.Fatalf("baseline = %v", got)
	}
	b.Promote(map[string]podread.Result{query.ID: {Query: query, Error: "no running Pod holds the target role"}})
	if got := promoted(b, query); len(got) != 1 || got[0] != "good" {
		t.Fatalf("a failed read replaced the baseline: %v", got)
	}
	changed := query
	changed.Args = []string{"store-cli", "get", "other"}
	if got := promoted(b, changed); len(got) != 0 {
		t.Fatalf("a changed query inherited the baseline: %v", got)
	}
}

// The snapshot carries the promoted readings with each current result, so a
// detector, a remote memory host and a capture all see the same reference.
func TestPodReadsSnapshotCarriesThePromotedBaseline(t *testing.T) {
	now := time.Unix(1000, 0).UTC()
	observer := podReadObserver(t, answering("good"), func() time.Time { return now })
	baseline := NewPodReadBaseline(DefaultStateSettle)
	baseline.Promote(map[string]podread.Result{storeReadID: {Query: storeQuery(), Readings: []podread.Reading{{Pod: "a", Stdout: "healthy"}}}})
	provider := PodReadsSnapshotProvider{Base: staticProvider{snapshot: storeSnapshot(runningStorePod("a"))}, Observer: observer, Baseline: baseline}
	snapshot, err := provider.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, ok := podread.ResultFrom(snapshot, storeReadID)
	if !ok || len(result.Baseline) != 1 || result.Baseline[0].Stdout != "healthy" || result.Readings[0].Stdout != "good" {
		t.Fatalf("result = %+v", result)
	}
	captured, err := sdktest.CaptureSnapshot(snapshot, "app")
	if err != nil {
		t.Fatal(err)
	}
	if got := captured.PodReads[storeReadID].Baseline; len(got) != 1 || got[0].Stdout != "healthy" {
		t.Fatalf("capture lost the baseline: %+v", captured.PodReads[storeReadID])
	}
}

// syntheticRedis is synthetic output of the redis store-state read.
const syntheticRedis = "== info\nrole:master\nconnected_clients:3\nmaxclients:10000\nused_memory:1048576\nmaxmemory:0\nmaxmemory_policy:noeviction\nrdb_last_bgsave_status:ok\naof_last_write_status:ok\ndb0:keys=12,expires=0,avg_ttl=0\n" +
	"== dbsize\n12\n== role\nmaster\n== config\nmaxmemory\n0\nmaxmemory-policy\nnoeviction\nmaxclients\n10000\nmin-replicas-to-write\n0\nmin-replicas-max-lag\n10\nreplica-read-only\nyes\nstop-writes-on-bgsave-error\nyes\nappendonly\nno\nrequirepass\nempty\n" +
	"== acl\nuser default on nopass sanitize-payload ~* &* +@all\n"

// The incident request names each store item that changed since the healthy
// baseline beside the configuration diff, so a responder sees a changed
// setting even when no store-state rule fires on it.
func TestIncidentStateChangesIncludeStoreChanges(t *testing.T) {
	now := time.Unix(2000, 0).UTC()
	id := storestate.QueryID(storestate.EngineRedis, "kv")
	reading := func(out string) podread.Reading {
		return podread.Reading{Namespace: "app", Pod: "kv-0", ObservedAt: now, Stdout: out}
	}
	changed := strings.Replace(syntheticRedis, "maxclients\n10000", "maxclients\n20", 1)
	view := sdktest.Snapshot{NamespaceName: "app", PodReads: map[string]podread.Result{id: {Query: podread.Query{ID: id}, Readings: []podread.Reading{reading(changed)}, Baseline: []podread.Reading{reading(syntheticRedis)}}}}

	state := withStoreChanges(nil, view, now)
	if state == nil || len(state.Changes) != 1 {
		t.Fatalf("state = %+v", state)
	}
	change := state.Changes[0]
	if change.Kind != StoreStateChangeKind || change.Name != "kv/kv-0" || change.Change != StateChangeModified {
		t.Fatalf("change = %+v", change)
	}
	if len(change.Fields) != 1 || change.Fields[0].Field != "setting maxclients" || change.Fields[0].Before != "10000" || change.Fields[0].After != "20" {
		t.Fatalf("fields = %+v", change.Fields)
	}
	existing := &StateChanges{Changes: []StateChange{{Kind: "ConfigMap", Name: "settings", Change: StateChangeModified}}}
	if merged := withStoreChanges(existing, view, now); len(merged.Changes) != 2 || len(existing.Changes) != 1 {
		t.Fatalf("merge = %+v, original mutated: %+v", merged, existing)
	}
	unchanged := view
	unchanged.PodReads = map[string]podread.Result{id: {Query: podread.Query{ID: id}, Readings: []podread.Reading{reading(syntheticRedis)}, Baseline: []podread.Reading{reading(syntheticRedis)}}}
	if got := withStoreChanges(nil, unchanged, now); got != nil {
		t.Fatalf("no store change still produced %+v", got)
	}
	var _ sdk.DetectionContext = view
}
