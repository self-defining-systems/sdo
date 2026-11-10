package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/storestate"
)

// boundaryBaseline is a StateBaseline whose adopted and candidate boundaries
// the test sets, as the StateTracker's settle would.
type boundaryBaseline struct{ baselineAt, candidateAt time.Time }

func (b *boundaryBaseline) Observe(time.Time, bool)         {}
func (b *boundaryBaseline) Changes(time.Time) *StateChanges { return nil }
func (b *boundaryBaseline) BaselineAt() time.Time           { return b.baselineAt }
func (b *boundaryBaseline) BaselineCandidateAt() time.Time  { return b.candidateAt }
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

// The pod-read baseline is promoted with the configuration baseline: the
// readings of the candidate moment become the reference only once that
// candidate settled, and a reading taken after it (a fault still below its
// firing threshold) is never absorbed.
func TestPodReadBaselinePromotesWithTheHealthyBaseline(t *testing.T) {
	t0 := time.Unix(1000, 0).UTC()
	t1, t2, t3 := t0.Add(time.Minute), t0.Add(2*time.Minute), t0.Add(3*time.Minute)
	boundaries := &boundaryBaseline{}
	c := &Controller{Baseline: boundaries, PodReadBaseline: NewPodReadBaseline()}
	query := storeQuery()

	// No adopted baseline yet: nothing is promoted.
	c.promotePodReadBaseline(t0, readingsSnapshot(query, "first"), true)
	if got := promoted(c.PodReadBaseline, query); len(got) != 0 {
		t.Fatalf("promoted before any baseline: %v", got)
	}
	boundaries.baselineAt = t0
	c.promotePodReadBaseline(t0, readingsSnapshot(query, "first"), true)
	if got := promoted(c.PodReadBaseline, query); len(got) != 1 || got[0] != "first" {
		t.Fatalf("first baseline = %v", got)
	}
	boundaries.candidateAt = t1
	c.promotePodReadBaseline(t1, readingsSnapshot(query, "candidate"), true)
	c.promotePodReadBaseline(t2, readingsSnapshot(query, "drifted"), true)
	if got := promoted(c.PodReadBaseline, query); got[0] != "first" {
		t.Fatalf("candidate promoted before it settled: %v", got)
	}
	boundaries.baselineAt, boundaries.candidateAt = t1, t3
	c.promotePodReadBaseline(t3, readingsSnapshot(query, "later"), true)
	if got := promoted(c.PodReadBaseline, query); len(got) != 1 || got[0] != "candidate" {
		t.Fatalf("settled baseline = %v, want the candidate's readings", got)
	}
	// An unhealthy evaluation never records a candidate.
	boundaries.candidateAt = t3.Add(time.Minute)
	c.promotePodReadBaseline(boundaries.candidateAt, readingsSnapshot(query, "bad"), false)
	boundaries.baselineAt = boundaries.candidateAt
	c.promotePodReadBaseline(boundaries.candidateAt.Add(time.Minute), readingsSnapshot(query, "x"), true)
	if got := promoted(c.PodReadBaseline, query); got[0] != "candidate" {
		t.Fatalf("an unhealthy observation was promoted: %v", got)
	}
}

// Only complete readings form a baseline, a query whose readings all failed
// keeps its earlier reference, and a changed declaration gets none.
func TestPodReadBaselineKeepsOnlyCompleteReadingsOfTheSameQuery(t *testing.T) {
	query := storeQuery()
	b := NewPodReadBaseline()
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
	baseline := NewPodReadBaseline()
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
