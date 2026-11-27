package appmap

import "testing"

// effectMap is a web tier that hands work to a worker through a queue: the
// worker's operation is internal (background work), and the user's order
// only completes once the worker ran.
const effectMap = `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
  - {name: worker, workload: Deployment/worker, services: [worker-metrics], holds_data: false}
  - {name: queue, workload: StatefulSet/queue, services: [queue], holds_data: false, data: {engine: redis, role: authoritative}}
edges:
  - {from: web, to: queue, evidence: [manifest], source: "web config"}
  - {from: worker, to: queue, evidence: [manifest], source: "worker config"}
entries:
  - {service: worker-metrics, metric: jobs_total, labels: {route: queue, status: outcome}, errors: 'failed'}
operations:
  - id: place-order
    writes: true
    entry: {service: web, port: 80, method: POST, path: /orders}
    path: [web, queue]
    correct: "201 with an id"
  - id: process-order
    writes: true
    entry: {service: worker-metrics, path: orders}
    path: [worker, queue]
    correct: "the order is processed within 20 s"
probes:
  - {scenario: order-processed, kind: write-read, operations: [place-order, process-order]}
`

func effectScenario(waits bool) Scenario {
	return Scenario{ID: "order-processed", SideEffect: "idempotent-write", DependsOn: []string{"web", "queue", "worker-metrics"}, WaitsForEffect: waits}
}

func TestProbeOfBackgroundWorkMustWaitForItsEffect(t *testing.T) {
	m := mustParse(t, effectMap)
	if findings := m.CheckScenarios([]Scenario{effectScenario(false)}); !has(findings, RuleEffectUnobserved, "process-order") {
		t.Fatalf("a synchronous probe claimed background work unchallenged: %v", findings)
	}
	if findings := m.CheckScenarios([]Scenario{effectScenario(true)}); has(findings, RuleEffectUnobserved, "") {
		t.Fatalf("a probe that waits for the effect was flagged: %v", findings)
	}
}

// A probe of background work alone enters at no user entry, so it is no
// user test and cannot open an incident: it must also perform the user
// action that hands the work off.
func TestProbeOfBackgroundWorkNamesTheUserActionThatStartsIt(t *testing.T) {
	m := mustParse(t, effectMap)
	m.Probes[0].Operations = []string{"process-order"}
	if findings := m.CheckScenarios([]Scenario{effectScenario(true)}); !has(findings, RuleEffectUnobserved, "performs a user operation") {
		t.Fatalf("a background-only probe was accepted: %v", findings)
	}
}

// The current map form: background work is a step of the user operation that
// triggers it. A probe that lists the operation must wait for that work; a
// call probe beside it need not.
func TestOperationThatTriggersWorkNeedsAProbeThatWaitsForIt(t *testing.T) {
	m := mustParse(t, effectMap)
	m.Operations = m.Operations[:1]
	m.Operations[0].Triggers = []string{"OrderJob"}
	m.Probes = []Probe{
		{Scenario: "call-place-order", Kind: KindCall, Operations: []string{"place-order"}},
		{Scenario: "order-processed", Kind: KindWriteRead, Operations: []string{"place-order"}},
	}
	call := Scenario{ID: "call-place-order", SideEffect: "idempotent-write", DependsOn: []string{"web", "queue"}}
	write := func(waits bool) Scenario {
		return Scenario{ID: "order-processed", SideEffect: "idempotent-write", DependsOn: []string{"web", "queue"}, WaitsForEffect: waits}
	}
	if findings := m.CheckScenarios([]Scenario{call, write(false)}); !has(findings, RuleEffectUnobserved, "OrderJob") {
		t.Fatalf("triggered work no probe waits for was accepted: %v", findings)
	}
	if findings := m.CheckScenarios([]Scenario{call, write(true)}); has(findings, RuleEffectUnobserved, "") {
		t.Fatalf("one waiting probe must be enough: %v", findings)
	}
}
