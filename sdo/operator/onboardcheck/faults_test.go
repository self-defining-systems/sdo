package onboardcheck

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/memory/appmap"
)

type fakeFault struct {
	w          *world
	name       string
	ops        []string
	notWhy     string
	breakErr   error
	restoreErr error
}

func (f *fakeFault) Name() string                         { return f.name }
func (f *fakeFault) Describe() string                     { return "fake " + f.name }
func (f *fakeFault) Operations() []string                 { return f.ops }
func (f *fakeFault) NotApplicable(context.Context) string { return f.notWhy }
func (f *fakeFault) Break(context.Context) error {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	f.w.log = append(f.w.log, "break "+f.name)
	if f.breakErr != nil {
		return f.breakErr
	}
	f.w.broken[f.name] = true
	return nil
}
func (f *fakeFault) Restore(context.Context) error {
	f.w.mu.Lock()
	defer f.w.mu.Unlock()
	f.w.log = append(f.w.log, "restore "+f.name)
	if f.restoreErr != nil {
		return f.restoreErr
	}
	delete(f.w.broken, f.name)
	return nil
}

func faultRun(t *testing.T, w *world, faults ...Fault) Report {
	t.Helper()
	cfg := liveConfig(t, w, false, true)
	cfg.Break.Faults = faults
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestFaultNamingTheHurtOperationIsGreen(t *testing.T) {
	w := newWorld()
	w.raiseFor["worker-quiet"] = []Raise{{DetectorID: "request-metrics", Summary: "operation send-mail has no traffic"}}
	r := faultRun(t, w, &fakeFault{w: w, name: "worker-quiet", ops: []string{"send-mail"}})
	if status(r, ItemBreak, "worker-quiet") != Green || !strings.Contains(r.Text(), "naming send-mail") {
		t.Fatalf("expected green naming the operation:\n%s", r.Text())
	}
}

func TestFaultOnlyNamingTheComponentIsRed(t *testing.T) {
	w := newWorld()
	w.raiseFor["data-wrong"] = []Raise{{DetectorID: "workload-health", Summary: "Deployment db restarted"}}
	r := faultRun(t, w, &fakeFault{w: w, name: "data-wrong", ops: []string{"list-items"}})
	if status(r, ItemBreak, "data-wrong") != Red || !strings.Contains(r.Text(), "no detector named list-items") {
		t.Fatalf("expected red:\n%s", r.Text())
	}
}

func TestFaultIsAlwaysRestored(t *testing.T) {
	w := newWorld()
	f := &fakeFault{w: w, name: "data-wrong", ops: []string{"list-items"}, breakErr: errors.New("no table")}
	r := faultRun(t, w, f)
	if status(r, ItemBreak, "data-wrong") != Red || !strings.Contains(strings.Join(w.log, ";"), "restore data-wrong") {
		t.Fatalf("a failed break must still restore:\n%s\n%v", r.Text(), w.log)
	}
	if len(w.broken) != 0 {
		t.Fatalf("left broken: %v", w.broken)
	}
}

func TestFaultsRunAfterComponentBreaksAndStopOnUncertainRestore(t *testing.T) {
	w := newWorld()
	// Both hurt send-mail, so neither joins the other's wave nor a component's.
	f1 := &fakeFault{w: w, name: "worker-quiet", ops: []string{"send-mail"}, restoreErr: errors.New("policy gone?")}
	f2 := &fakeFault{w: w, name: "data-wrong", ops: []string{"send-mail"}}
	r := faultRun(t, w, f1, f2)
	if status(r, ItemBreak, "worker-quiet") != Red {
		t.Fatalf("uncertain restore is red:\n%s", r.Text())
	}
	if strings.Contains(strings.Join(w.log, ";"), "break data-wrong") {
		t.Fatalf("a fault ran after an uncertain restore: %v", w.log)
	}
}

func TestFaultNotApplicableIsANote(t *testing.T) {
	w := newWorld()
	r := faultRun(t, w, &fakeFault{w: w, name: "worker-quiet", ops: []string{"send-mail"}, notWhy: "the CNI does not enforce NetworkPolicy"})
	if status(r, ItemBreak, "worker-quiet") != Note || strings.Contains(strings.Join(w.log, ";"), "break worker-quiet") {
		t.Fatalf("expected a note and no break:\n%s", r.Text())
	}
}

func TestFaultTargetsFindWorkersAndTheStore(t *testing.T) {
	m, err := appmap.Parse(liveMap)
	if err != nil {
		t.Fatal(err)
	}
	workers, store := FaultTargets(m)
	if len(workers) != 1 || workers[0].Name != "worker" || store == nil || store.Name != "db" {
		t.Fatalf("workers %v store %v", workers, store)
	}
	if got := strings.Join(OperationsThrough(m, "worker"), ","); got != "send-mail" {
		t.Fatalf("operations through worker: %s", got)
	}
}

func TestFaultTargetsPreferPostgresAndSkipStores(t *testing.T) {
	m, err := appmap.Parse(`version: 1
components:
  - {name: web, workload: web, services: [web]}
  - {name: git, workload: git, services: [git], holds_data: true, data: {engine: other, role: authoritative}}
  - {name: db, workload: db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}
  - {name: cache, workload: cache, services: [], data: {engine: redis, role: derived, derived_from: [db]}}
edges: []
entries: []
operations:
  - id: op
    entry: {service: web, port: 80, method: GET, path: /x}
    path: [web, git, db, cache]
probes: []
`)
	if err != nil {
		t.Fatal(err)
	}
	workers, store := FaultTargets(m)
	if len(workers) != 0 {
		t.Fatalf("a derived store is not a worker: %v", workers)
	}
	if store == nil || store.Name != "db" {
		t.Fatalf("the postgres store is the target, got %v", store)
	}
}

// A fault judges raises it causes, so the raises the last component break left
// must have cleared before it starts; after the last fault nothing waits.
func TestFaultWaitsForTheLastComponentBreakToClearButNothingWaitsAfterTheLastFault(t *testing.T) {
	w := newWorld()
	var evalsAtBreak int
	f := &probeFault{fakeFault: fakeFault{w: w, name: "data-wrong", ops: []string{"list-items"}}, onBreak: func() { evalsAtBreak = w.sinceRestore }}
	faultRun(t, w, f)
	if evalsAtBreak == 0 {
		t.Fatal("the fault started without waiting for the last component break's raises to clear")
	}
	if w.sinceRestore != 0 {
		t.Fatalf("%d evaluations after the last restore: nothing later needs a cleared baseline", w.sinceRestore)
	}
}

type probeFault struct {
	fakeFault
	onBreak func()
}

func (f *probeFault) Break(ctx context.Context) error {
	f.w.mu.Lock()
	f.onBreak()
	f.w.mu.Unlock()
	return f.fakeFault.Break(ctx)
}

func (f *probeFault) Restore(ctx context.Context) error {
	f.w.mu.Lock()
	f.w.sinceRestore = 0
	f.w.mu.Unlock()
	return f.fakeFault.Restore(ctx)
}

// The operations of the concurrency map: a (web, db), b (job, cache), c
// (mailer). A fault hurting c shares no operation with the wave of db and
// cache, so it is broken in that wave instead of after every component.
const faultWaveMap = `version: 1
components:
  - {name: web, workload: web, services: [web]}
  - {name: db, workload: db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}
  - {name: job, workload: job, services: [job]}
  - {name: cache, workload: cache, services: [cache]}
  - {name: mailer, workload: mailer, services: [mailer]}
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
  - id: c
    entry: {service: mailer, port: 80, method: GET, path: /c}
    path: [mailer]
`

func faultWaveRun(t *testing.T, w *world, oneAtATime bool, faults ...Fault) Report {
	t.Helper()
	cfg := liveConfig(t, w, false, true)
	m, err := appmap.Parse(faultWaveMap)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Map = m
	cfg.Break.Faults = faults
	cfg.Break.OneAtATime = oneAtATime
	r, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestFaultSharingNoOperationWithAComponentWaveIsBrokenInThatWave(t *testing.T) {
	w := newWorld()
	f := &fakeFault{w: w, name: "worker-quiet", ops: []string{"c"}}
	w.raiseFor["worker-quiet"] = []Raise{{DetectorID: "request-metrics", Summary: "operation c has no traffic"}}
	r := faultWaveRun(t, w, false, f)
	// waves: [web job mailer], [db cache worker-quiet]; the fault shares c with
	// mailer, so it cannot be in the first.
	log := strings.Join(w.log, ",")
	if !strings.Contains(log, "break worker-quiet") {
		t.Fatalf("fault not run: %v", w.log)
	}
	brokenTogether := 0
	for i, e := range w.log {
		if e == "break worker-quiet" {
			for _, o := range w.log[i-2 : i+3] {
				if o == "break db" || o == "break cache" {
					brokenTogether++
				}
			}
		}
	}
	if brokenTogether != 2 {
		t.Fatalf("the fault is broken with db and cache: %v", w.log)
	}
	if status(r, ItemBreak, "worker-quiet") != Green {
		t.Fatalf("fault named its operation:\n%s", r.Text())
	}
}

func TestFaultInAComponentWaveGivesTheVerdictsOfSeparateBreaks(t *testing.T) {
	setups := map[string]func(*world){
		"nothing noticed": func(w *world) {},
		"fault named": func(w *world) {
			w.raiseFor["worker-quiet"] = []Raise{{DetectorID: "request-metrics", Rule: "request-metrics.error", Summary: "operation c failing"}}
		},
		"components named too": func(w *world) {
			w.raiseFor["worker-quiet"] = []Raise{{DetectorID: "request-metrics", Rule: "request-metrics.error", Summary: "operation c failing"}}
			w.raiseFor["db"] = raiseNaming("traffic-probe-call", "scenario failed: a")
			w.raiseFor["cache"] = raiseNaming("store-state", "cache is unreachable")
		},
	}
	for name, setup := range setups {
		t.Run(name, func(t *testing.T) {
			waved, separate := newWorld(), newWorld()
			setup(waved)
			setup(separate)
			a := faultWaveRun(t, waved, false, &fakeFault{w: waved, name: "worker-quiet", ops: []string{"c"}})
			b := faultWaveRun(t, separate, true, &fakeFault{w: separate, name: "worker-quiet", ops: []string{"c"}})
			if got, want := verdicts(a), verdicts(b); got != want {
				t.Fatalf("waves:\n%s\nseparate:\n%s", got, want)
			}
		})
	}
}
