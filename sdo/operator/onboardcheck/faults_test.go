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
	f1 := &fakeFault{w: w, name: "worker-quiet", ops: []string{"send-mail"}, restoreErr: errors.New("policy gone?")}
	f2 := &fakeFault{w: w, name: "data-wrong", ops: []string{"list-items"}}
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
