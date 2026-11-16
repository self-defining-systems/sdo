package onboardcheck

import (
	"context"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/appmap"
)

// cacheMap: operations a (web, db) and b (job, cache); scenario sa probes a,
// sb probes b.
const cacheMap = `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web]}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}
  - {name: job, workload: Deployment/job, services: [job]}
  - {name: cache, workload: Deployment/cache, services: [cache]}
edges:
  - {from: web, to: db, evidence: [config], source: x}
  - {from: job, to: cache, evidence: [config], source: x}
entries: []
operations:
  - id: a
    entry: {service: web, port: 80, method: GET, path: /a}
    path: [web, db]
  - id: b
    entry: {service: job, port: 80, method: GET, path: /b}
    path: [job, cache]
probes:
  - {scenario: sa, kind: call, operations: [a]}
  - {scenario: sb, kind: call, operations: [b]}
`

func cacheInputs(t *testing.T) Inputs {
	t.Helper()
	m, err := appmap.Parse(cacheMap)
	if err != nil {
		t.Fatal(err)
	}
	return Inputs{
		Map: m,
		Files: map[string]string{
			".sdo/diagnostics/traffic/generators/a.go":                 "scenario sa steps",
			".sdo/diagnostics/traffic/generators/b.go":                 "scenario sb steps",
			".sdo/diagnostics/traffic/generators/common.go":            "shared helper",
			".sdo/diagnostics/traffic/workloads/wa.yaml":               "scenarios: [sa]",
			".sdo/diagnostics/traffic/workloads/wb.yaml":               "scenarios: [sb]",
			".sdo/diagnostics/detectors/health/store-state-db/main.go": "watches db",
			".sdo/diagnostics/detectors/health/queue-depth/main.go":    "watches cache",
			".sdo/diagnostics/detectors/health/traffic-wa/main.go":     "workload wa",
			".sdo/diagnostics/detectors/health/traffic-wb/main.go":     "workload wb",
		},
		Deploy: map[string]string{"web": "w1", "db": "d1", "job": "j1", "cache": "c1"},
		Salt:   "v1",
	}
}

func TestKeysAreStableAndCoverTheInputsOnAnOperationsPaths(t *testing.T) {
	base := cacheInputs(t)
	keys := func(in Inputs) map[string]string {
		out := map[string]string{}
		for _, c := range []string{"web", "db", "job", "cache"} {
			out[c] = in.ComponentKey(c)
			if out[c] == "" {
				t.Fatalf("no key for %s", c)
			}
		}
		return out
	}
	want := keys(base)
	if got := keys(cacheInputs(t)); strings.Join(mapVals(got), ",") != strings.Join(mapVals(want), ",") {
		t.Fatal("the same memory gives the same keys")
	}
	changed := func(name string, edit func(*Inputs), moved ...string) {
		t.Run(name, func(t *testing.T) {
			in := cacheInputs(t)
			edit(&in)
			got := keys(in)
			for _, c := range []string{"web", "db", "job", "cache"} {
				should := false
				for _, m := range moved {
					should = should || m == c
				}
				if (got[c] != want[c]) != should {
					t.Fatalf("%s: key changed=%v, want %v", c, got[c] != want[c], should)
				}
			}
		})
	}
	changed("a probe of operation a", func(in *Inputs) { in.Files[".sdo/diagnostics/traffic/generators/a.go"] += " edited" }, "web", "db")
	changed("a workload of operation b", func(in *Inputs) { in.Files[".sdo/diagnostics/traffic/workloads/wb.yaml"] += " edited" }, "job", "cache")
	changed("a shared generator helper", func(in *Inputs) { in.Files[".sdo/diagnostics/traffic/generators/common.go"] += " edited" }, "web", "db", "job", "cache")
	changed("a detector that names nothing of the map (it matches by label or a loop)", func(in *Inputs) {
		in.Files[".sdo/diagnostics/detectors/health/generic-loop/main.go"] = "for each workload"
	}, "web", "db", "job", "cache")
	changed("a detector that names only db", func(in *Inputs) { in.Files[".sdo/diagnostics/detectors/health/store-state-db/main.go"] += " edited" }, "web", "db")
	changed("the traffic detector of operation b", func(in *Inputs) { in.Files[".sdo/diagnostics/detectors/health/traffic-wb/main.go"] += " edited" }, "job", "cache")
	changed("the deployment of cache", func(in *Inputs) { in.Deploy["cache"] = "c2" }, "job", "cache")
	changed("the declaration of operation a", func(in *Inputs) { in.Map.Operations[0].Entry.Path = "/a2" }, "web", "db")
	changed("a probe declaration of operation b", func(in *Inputs) { in.Map.Probes[1].Kind = "write" }, "job", "cache")
	changed("a new probe of operation a", func(in *Inputs) {
		in.Map.Probes = append(in.Map.Probes, appmap.Probe{Scenario: "sa2", Kind: "call", Operations: []string{"a"}})
	}, "web", "db")
	changed("the salt (bound, mode, build)", func(in *Inputs) { in.Salt = "v2" }, "web", "db", "job", "cache")
	t.Run("an unknown deployment makes no key", func(t *testing.T) {
		in := cacheInputs(t)
		delete(in.Deploy, "db")
		if in.ComponentKey("web") != "" || in.ComponentKey("job") == "" {
			t.Fatal("a component's key needs the deployment of every component on its operations' paths")
		}
	})
	t.Run("a component without a workload does not keep its operation from being keyed", func(t *testing.T) {
		in := cacheInputs(t)
		in.Map.Components[1].Workload = "" // db declares none
		delete(in.Deploy, "db")
		if in.ComponentKey("web") == "" || in.ComponentKey("db") == "" {
			t.Fatal("nothing is deployed for it, so nothing is unknown")
		}
	})
}

func mapVals(m map[string]string) []string {
	var out []string
	for _, k := range []string{"web", "db", "job", "cache"} {
		out = append(out, m[k])
	}
	return out
}

func TestDetectorKeysFollowTheirOwnFiles(t *testing.T) {
	base := cacheInputs(t)
	k := func(in Inputs, id string) string { return in.DetectorKey(id) }
	in := cacheInputs(t)
	in.Files[".sdo/diagnostics/detectors/health/queue-depth/main.go"] += " edited"
	if k(in, "queue-depth") == k(base, "queue-depth") || k(in, "store-state-db") != k(base, "store-state-db") {
		t.Fatal("only the edited detector's key changes")
	}
	in = cacheInputs(t)
	in.Files[".sdo/diagnostics/traffic/workloads/wa.yaml"] += " edited"
	if k(in, "traffic-wa") == k(base, "traffic-wa") || k(in, "traffic-wb") != k(base, "traffic-wb") {
		t.Fatal("a traffic detector follows its workload file")
	}
	in = cacheInputs(t)
	in.Deploy["web"] = "w2"
	if k(in, "queue-depth") == k(base, "queue-depth") {
		t.Fatal("a detector's healthy verdict is about the deployed application")
	}
	if k(in, "service-endpoints") == k(base, "service-endpoints") {
		t.Fatal("a generated detector follows the deployment too")
	}
}

// cacheRun runs the break item of cacheMap with the cache, returning the report
// and the log of breaks.
func cacheRun(t *testing.T, w *world, in Inputs, cache *Cache, reuse bool) Report {
	t.Helper()
	return cacheRunWith(t, w, in, cache, reuse, false)
}

func cacheRunWith(t *testing.T, w *world, in Inputs, cache *Cache, reuse, oneAtATime bool) Report {
	t.Helper()
	cfg := liveConfig(t, w, false, true)
	cfg.Break.OneAtATime = oneAtATime
	cfg.Map = in.Map
	cfg.Break.Cache = cache
	cfg.Break.Reuse = reuse
	cfg.Break.Keys = in.BreakKeys(nil)
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func namedWorld() *world {
	w := newWorld()
	w.raiseFor["web"] = raiseNaming("traffic-probe-call", "scenario sa failed: a")
	w.raiseFor["db"] = raiseNaming("store-state", "postgres db is unreachable")
	w.raiseFor["job"] = raiseNaming("traffic-probe-call", "scenario sb failed: b")
	w.raiseFor["cache"] = raiseNaming("workload-health", "workload cache has no ready pods")
	return w
}

func brokenNames(w *world) []string {
	var out []string
	for _, e := range w.log {
		if c, ok := strings.CutPrefix(e, "break "); ok {
			out = append(out, c)
		}
	}
	return out
}

func TestUnchangedMemoryReusesEveryBreakVerdict(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	first := namedWorld()
	r1 := cacheRun(t, first, in, cache, true)
	if len(brokenNames(first)) != 4 || r1.Mode != ModeFull {
		t.Fatalf("a first run breaks everything: %v mode %s", brokenNames(first), r1.Mode)
	}
	second := namedWorld()
	r2 := cacheRun(t, second, in, cache, true)
	if len(second.log) != 0 {
		t.Fatalf("nothing re-broken: %v", second.log)
	}
	if r2.Mode != ModeIncremental || r2.Clean() {
		t.Fatalf("a run that reuses is incremental and never clean: %s", r2.Mode)
	}
	if got, want := verdicts(r2), verdicts(r1); got != want {
		t.Fatalf("reused verdicts equal the fresh ones:\n%s\nfresh:\n%s", got, want)
	}
	for _, c := range []string{"web", "db", "job", "cache"} {
		if !strings.Contains(detail(r2, ItemBreak, c), "reused from") {
			t.Fatalf("%s is labelled reused: %s", c, detail(r2, ItemBreak, c))
		}
	}
	if !strings.Contains(r2.Text(), "mode: incremental") {
		t.Fatal("the report says it is incremental and not the gate")
	}
}

func detail(r Report, item, subject string) string {
	for _, x := range r.Results {
		if x.Item == item && x.Subject == subject {
			return x.Detail
		}
	}
	return ""
}

func TestAProbeChangeOnOneOperationReBreaksOnlyItsComponents(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	cacheRun(t, namedWorld(), in, cache, true)
	in.Files[".sdo/diagnostics/traffic/generators/b.go"] += " edited"
	w := namedWorld()
	r := cacheRun(t, w, in, cache, true)
	got := strings.Join(brokenNames(w), ",")
	if got != "job,cache" && got != "cache,job" {
		t.Fatalf("only the components of b are re-broken: %v", w.log)
	}
	if !strings.Contains(detail(r, ItemBreak, "web"), "reused from") || strings.Contains(detail(r, ItemBreak, "job"), "reused") {
		t.Fatalf("web reused, job fresh:\n%s", r.Text())
	}
	if status(r, ItemBreak, "a") != Green || status(r, ItemBreak, "b") != Green {
		t.Fatalf("both operations stay green:\n%s", r.Text())
	}
	// The re-broken ones are recorded: a third run reuses all again.
	w3 := namedWorld()
	cacheRun(t, w3, in, cache, true)
	if len(w3.log) != 0 {
		t.Fatalf("the fresh verdicts were recorded: %v", w3.log)
	}
}

func TestFullIgnoresTheCacheButRecordsIt(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	cacheRun(t, namedWorld(), in, cache, true)
	w := namedWorld()
	r := cacheRun(t, w, in, cache, false)
	if len(brokenNames(w)) != 4 || r.Mode != ModeFull {
		t.Fatalf("--full breaks everything: %v", w.log)
	}
	if strings.Contains(r.Text(), "reused") {
		t.Fatalf("nothing reused:\n%s", r.Text())
	}
}

func TestOnlyGreenVerdictsAreReused(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	w := namedWorld()
	delete(w.raiseFor, "cache") // nothing names cache: red
	r1 := cacheRun(t, w, in, cache, true)
	if status(r1, ItemBreak, "cache") != Red {
		t.Fatalf("setup: %s", r1.Text())
	}
	w2 := namedWorld()
	delete(w2.raiseFor, "cache")
	cacheRun(t, w2, in, cache, true)
	if got := brokenNames(w2); len(got) != 1 || got[0] != "cache" {
		t.Fatalf("a red component is re-broken, a green one reused: %v", got)
	}
}

func TestReusedFaultsAreGreenOnly(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	run := func() (*world, Report) {
		w := namedWorld()
		w.raiseFor["data-wrong"] = []Raise{{DetectorID: "request-metrics", Summary: "operation a failing"}}
		cfg := liveConfig(t, w, false, true)
		cfg.Map = in.Map
		cfg.Break.Faults = []Fault{&fakeFault{w: w, name: "data-wrong", ops: []string{"a"}}}
		cfg.Break.Cache, cfg.Break.Reuse = cache, true
		cfg.Break.Keys = in.BreakKeys(cfg.Break.Faults)
		r, err := Run(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		return w, r
	}
	run()
	w, r := run()
	if len(w.log) != 0 || !strings.Contains(detail(r, ItemBreak, "data-wrong"), "reused from") || status(r, ItemBreak, "data-wrong") != Green {
		t.Fatalf("an unchanged fault is reused: %v\n%s", w.log, r.Text())
	}
}

func quietCacheRun(t *testing.T, w *world, in Inputs, cache *Cache, reuse bool) (Report, int) {
	t.Helper()
	cfg := liveConfig(t, w, true, false)
	cfg.Map = in.Map
	cfg.Quiet.Cache, cfg.Quiet.Reuse, cfg.Quiet.DetectorKey = cache, reuse, in.DetectorKey
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r, w.evals
}

func TestQuietWindowNeedsOnlyOneJudgementFromUnchangedDetectors(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	mk := func() *world {
		w := newWorld()
		w.sim = []detectorSim{{"queue-depth", 30 * time.Second, time.Minute}, {"traffic-wa", 30 * time.Second, time.Minute}}
		return w
	}
	r1, full := quietCacheRun(t, mk(), in, cache, true)
	if status(r1, ItemQuiet, "detectors") != Green || r1.Mode != ModeFull {
		t.Fatalf("first run:\n%s", r1.Text())
	}
	r2, same := quietCacheRun(t, mk(), in, cache, true)
	if status(r2, ItemQuiet, "detectors") != Green || r2.Mode != ModeIncremental || same >= full {
		t.Fatalf("unchanged detectors end the window sooner (%d vs %d evaluations), incremental:\n%s", same, full, r2.Text())
	}
	if !strings.Contains(detail(r2, ItemQuiet, "detectors"), "reused from") {
		t.Fatalf("labelled: %s", detail(r2, ItemQuiet, "detectors"))
	}
	in.Files[".sdo/diagnostics/detectors/health/queue-depth/main.go"] += " edited"
	_, changed := quietCacheRun(t, mk(), in, cache, true)
	if changed <= same {
		t.Fatalf("a changed detector is judged the full number of times (%d vs %d)", changed, same)
	}
	_, forced := quietCacheRun(t, mk(), in, cache, false)
	if forced != full {
		t.Fatalf("--full waits for every detector (%d vs %d)", forced, full)
	}
}

func TestAReusedDetectorThatRaisesIsStillRed(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	w := newWorld()
	w.sim = []detectorSim{{"queue-depth", 30 * time.Second, time.Minute}}
	quietCacheRun(t, w, in, cache, true)
	w2 := newWorld()
	w2.sim = []detectorSim{{"queue-depth", 30 * time.Second, time.Minute}}
	w2.healthyRaise = raiseNaming("queue-depth", "queue is backed up")
	r, _ := quietCacheRun(t, w2, in, cache, true)
	if status(r, ItemQuiet, "queue-depth") != Red {
		t.Fatalf("a raise is red whatever was cached:\n%s", r.Text())
	}
}

func TestTrafficDetectorWithoutAWorkloadFileFollowsEveryScenarioFile(t *testing.T) {
	base := cacheInputs(t)
	base.Files[".sdo/diagnostics/detectors/health/traffic-probe-call/main.go"] = "runs every scenario"
	in := cacheInputs(t)
	in.Files[".sdo/diagnostics/detectors/health/traffic-probe-call/main.go"] = "runs every scenario"
	in.Files[".sdo/diagnostics/traffic/generators/b.go"] += " edited"
	if in.DetectorKey("traffic-probe-call") == base.DetectorKey("traffic-probe-call") {
		t.Fatal("the detector that runs every scenario follows each scenario file")
	}
	if in.DetectorKey("traffic-wa") != base.DetectorKey("traffic-wa") {
		t.Fatal("a workload that names its scenarios follows only those")
	}
}

// A component whose break named an operation off its path (a missing edge)
// carries a verdict that depends on operations outside its key and on the wave
// it ran in: it is never recorded.
func TestAStrayOutcomeIsNeverReused(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	mk := func() *world {
		w := namedWorld()
		// breaking cache fails operation a, which does not list it
		w.raiseFor["cache"] = append(raiseNaming("workload-health", "workload cache has no ready pods"), raiseNaming("traffic-probe-call", "scenario sa failed: a")...)
		return w
	}
	r1 := cacheRunWith(t, mk(), in, cache, true, true)
	if status(r1, ItemMapAccuracy, "cache") == Green {
		t.Fatalf("setup: a missing edge shows: %s", r1.Text())
	}
	w := mk()
	cacheRunWith(t, w, in, cache, true, true)
	got := brokenNames(w)
	if len(got) != 1 || got[0] != "cache" {
		t.Fatalf("only the stray one is broken again: %v", got)
	}
}

func TestDetectorReuseKeepsModeWhenTheQuietItemIsRed(t *testing.T) {
	in := cacheInputs(t)
	cache := &Cache{}
	mk := func() *world {
		w := newWorld()
		w.sim = []detectorSim{{"queue-depth", 30 * time.Second, time.Minute}}
		return w
	}
	quietCacheRun(t, mk(), in, cache, true)
	w := mk()
	w.pending = nil
	cfg := liveConfig(t, w, true, false)
	cfg.Map = in.Map
	cfg.Quiet.Window = 90 * time.Second // too short for a changed detector's three judgements
	in.Files[".sdo/diagnostics/detectors/health/queue-depth/main.go"] += " edited"
	cfg.Quiet.Cache, cfg.Quiet.Reuse, cfg.Quiet.DetectorKey = cache, true, in.MemoDetectorKey()
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if status(r, ItemQuiet, "queue-depth") != Red {
		t.Fatalf("setup: the changed detector is short of judgements:\n%s", r.Text())
	}
}

func TestOperationNoteFollowsTheComponentTheDetailQuotes(t *testing.T) {
	op := appmap.Operation{ID: "a", Path: []string{"web", "db"}}
	at := time.Unix(100, 0)
	fresh := &breakOutcome{opFailure: map[string]*foundRaise{"a": {}}}
	reused := &breakOutcome{reusedAt: at, opFailure: map[string]*foundRaise{"a": {}}}
	if opReuseNote(op, map[string]*breakOutcome{"web": fresh, "db": reused}) != "" {
		t.Fatal("the detail quotes web's fresh break: no reuse note")
	}
	if !strings.Contains(opReuseNote(op, map[string]*breakOutcome{"web": reused, "db": fresh}), "reused from") {
		t.Fatal("the detail quotes web's reused break")
	}
}
