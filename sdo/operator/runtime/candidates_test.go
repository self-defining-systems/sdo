package runtime

import (
	"strings"
	"testing"
	"time"
)

// A synthetic application shaped like a web service over a shared database:
// every user operation enters the proxy, and the issue, project and package
// operations all lean on the database.
const candidateDoc = "# Architecture\n\n" + `<!-- sdo:application-map:begin -->
` + "```yaml" + `
version: 1
components:
  - {name: proxy, workload: Deployment/proxy, services: [proxy], holds_data: false}
  - {name: shell, workload: Deployment/shell, services: [shell], holds_data: false}
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
  - {name: repo, workload: StatefulSet/repo, services: [repo], holds_data: false}
  - {name: db, workload: StatefulSet/db, services: [db], holds_data: true, data: {engine: postgres, role: authoritative}}
  - {name: kv, workload: StatefulSet/kv, services: [kv], holds_data: true, data: {engine: redis, role: derived, derived_from: [db]}}
  - {name: blobs, workload: StatefulSet/blobs, services: [blobs], holds_data: true, data: {engine: other, role: authoritative}}
  - {name: queue, workload: Deployment/queue, services: [queue], holds_data: false}
edges:
  - {from: proxy, to: web, evidence: [config], source: x}
  - {from: proxy, to: shell, evidence: [config], source: x}
  - {from: shell, to: repo, evidence: [config], source: x}
  - {from: web, to: db, evidence: [config], source: x}
  - {from: web, to: kv, evidence: [config], source: x}
  - {from: web, to: repo, evidence: [config], source: x}
  - {from: web, to: blobs, evidence: [config], source: x}
  - {from: queue, to: db, evidence: [config], source: x}
  - {from: kv, to: db, evidence: [config], source: x}
operations:
  - {id: issue-read, writes: false, entry: {service: proxy, port: 80, method: GET, path: /issues}, path: [proxy, web, db, kv]}
  - {id: project-list, writes: false, entry: {service: proxy, port: 80, method: GET, path: /projects}, path: [proxy, web, db]}
  - {id: package-call, writes: true, entry: {service: proxy, port: 80, method: PUT, path: /packages}, path: [proxy, web, db, blobs]}
  - {id: ssh-call, writes: false, entry: {service: shell, port: 22, method: EXEC, path: ls}, path: [shell, repo]}
probes:
  - {scenario: issue-read, kind: call, operations: [issue-read]}
  - {scenario: project-list, kind: call, operations: [project-list]}
  - {scenario: package-call, kind: write-read, operations: [package-call]}
  - {scenario: ssh-call, kind: call, operations: [ssh-call]}
` + "```" + `
<!-- sdo:application-map:end -->
`

var candidateOnset = time.Date(2026, 11, 13, 6, 52, 0, 0, time.UTC)

func candidateGraph(t *testing.T) *mapGraph {
	t.Helper()
	g, err := newMapGraph(candidateDoc)
	if err != nil {
		t.Fatal(err)
	}
	return &g
}

func mappedTest(g *mapGraph, id string, state testState) candidateTest {
	return candidateTest{ID: id, Symptom: "HTTP 500", Path: g.testPath(id), Source: "map", State: state, Failures: 5, Samples: 5}
}

func nodeNamed(v candidateView, name string) *candidateNode {
	for _, n := range v.Nodes {
		if n.Name == name {
			return n
		}
	}
	return nil
}

func nodeNames(v candidateView) []string {
	var names []string
	for _, n := range v.Nodes {
		names = append(names, n.Name)
	}
	return names
}

// issuesTitle is the shape of an incident opened by a Kubernetes condition on
// the entry workload while the cause sits on the shared database: a churn
// rollout on web, a catalog change in db (read, not timed), passing tests
// through both.
func issuesTitle(t *testing.T) candidateInput {
	g := candidateGraph(t)
	return candidateInput{
		Graph:      g,
		Conditions: []string{"web"},
		Tests: []candidateTest{
			mappedTest(g, "issue-read", testPassing),
			mappedTest(g, "project-list", testPassing),
			mappedTest(g, "ssh-call", testPassing),
		},
		Onset: candidateOnset, Now: candidateOnset.Add(65 * time.Second),
		Rows: []IncidentChange{
			{At: candidateOnset.Add(-41 * time.Second), Source: "kubernetes", Component: "web", ReaderOrObject: "Deployment/web", Key: "spec.template.metadata.annotations", Before: "accepted-source", After: "changed", Writer: "kubectl-rollout"},
			{ReadAt: candidateOnset.Add(40 * time.Second), Source: "store", Component: "db", ReaderOrObject: "StatefulSet/db-0", Key: "app definitions (table group 07 of 16): +1 table", Before: "9 tables", After: "10 tables"},
		},
		Observed: map[string]string{"db": "", "kv": "", "blobs": "no state reader"},
	}
}

func TestIssuesTitleCauseOnTheSharedDataHolderRanksAboveTheEntryChurn(t *testing.T) {
	v := computeCandidates(issuesTitle(t))
	names := nodeNames(v)
	if len(names) == 0 || names[0] != "db" {
		t.Fatalf("the unexplained store change on the shared data holder must lead, got %v", names)
	}
	if web := nodeNamed(v, "web"); web == nil || indexOf(names, "web") <= 0 {
		t.Fatalf("the condition's component stays a candidate below it: %v", names)
	}
	for _, name := range []string{"db", "web"} {
		if n := nodeNamed(v, name); n == nil || len(n.Serves) == 0 {
			t.Fatalf("%s serves passing ops and must say so, not be cleared: %+v", name, n)
		}
	}
	for _, name := range []string{"kv", "repo", "blobs"} {
		if nodeNamed(v, name) == nil {
			t.Fatalf("%s is within two hops of the condition and must be listed: %v", name, names)
		}
	}
	text := v.render(candidateRenderCap)
	if strings.Contains(text, "on every failing path") {
		t.Fatalf("a condition-opened incident has no failing path to claim:\n%s", text)
	}
	if !strings.Contains(text, "serves passing ops") || strings.Contains(text, "cleared by passing") {
		t.Fatalf("exonerated nodes stay listed with the label:\n%s", text)
	}
}

func TestStoreChangeRowsAreLabelledReadTimeNeverAnOnsetOffset(t *testing.T) {
	text := computeCandidates(issuesTitle(t)).render(candidateRenderCap)
	if !strings.Contains(text, "read 06:52:40Z") || !strings.Contains(text, "change untimed") {
		t.Fatalf("a store row shows its read time and says the change is untimed:\n%s", text)
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, "table group") && (strings.Contains(line, "+40s") || strings.Contains(line, "-11s")) {
			t.Fatalf("a read time must never be shown as an onset offset: %q", line)
		}
	}
}

func TestUnexplainedEvidenceRanksBeforeRoutineRolloutWithinANode(t *testing.T) {
	in := issuesTitle(t)
	in.Rows = append(in.Rows, IncidentChange{At: candidateOnset.Add(-5 * time.Second), Source: "kubernetes", Component: "web", ReaderOrObject: "ConfigMap/web-env", Key: "data.POOL", Before: "accepted-source", After: "changed", Writer: "kubectl-edit"})
	web := nodeNamed(computeCandidates(in), "web")
	if len(web.Evidence) < 2 || !web.Evidence[0].Unexplained || web.Evidence[1].Unexplained {
		t.Fatalf("the manual edit is unexplained and the rollout is churn; unexplained first: %+v", web.Evidence)
	}
}

func TestSideEffectPageDoesNotDefineTheCandidates(t *testing.T) {
	g := candidateGraph(t)
	ssh := mappedTest(g, "ssh-call", testFailing)
	pkg := mappedTest(g, "package-call", testAlsoFailing)
	pkg.Symptom = "HTTP 502"
	pkg.Failures, pkg.Samples = 2, 5
	v := computeCandidates(candidateInput{Graph: g, Tests: []candidateTest{ssh, pkg, mappedTest(g, "issue-read", testPassing)}, Onset: candidateOnset, Now: candidateOnset.Add(30 * time.Second), Observed: map[string]string{"db": "", "blobs": ""}})
	if nodeNamed(v, "blobs") == nil || nodeNamed(v, "repo") == nil {
		t.Fatalf("the unevaluated test that failed contributes its path next to the page's: %v", nodeNames(v))
	}
	if len(v.Groups) != 2 {
		t.Fatalf("two symptoms are two groups: %+v", v.Groups)
	}
	text := v.render(candidateRenderCap)
	for _, want := range []string{"package-call", "also failing", "ssh-call", "opened on"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	if db := nodeNamed(v, "db"); db == nil || len(db.Serves) != 1 || db.Serves[0] != "issue-read" {
		t.Fatalf("db is on the failed package path and serves the passing read: %+v", db)
	}
}

func TestATestWithAFailedSampleNeverExonerates(t *testing.T) {
	g := candidateGraph(t)
	flaky := mappedTest(g, "project-list", testAlsoFailing)
	v := computeCandidates(candidateInput{Graph: g, Tests: []candidateTest{mappedTest(g, "package-call", testFailing), flaky}, Onset: candidateOnset, Now: candidateOnset})
	if db := nodeNamed(v, "db"); db == nil || len(db.Serves) != 0 || db.Class != PositionFailingOnly {
		t.Fatalf("a test with a failed sample is not a passing operation: %+v", db)
	}
}

func TestPartialFailureContributesItsWholePathAndExoneratesNothing(t *testing.T) {
	g := candidateGraph(t)
	partial := mappedTest(g, "issue-read", testFailing)
	partial.Failures, partial.Samples = 2, 10
	v := computeCandidates(candidateInput{Graph: g, Tests: []candidateTest{partial, mappedTest(g, "project-list", testPassing)}, Onset: candidateOnset, Now: candidateOnset})
	for _, name := range []string{"proxy", "web", "db", "kv"} {
		if nodeNamed(v, name) == nil {
			t.Fatalf("%s is on the partially failing path: %v", name, nodeNames(v))
		}
	}
	if kv := nodeNamed(v, "kv"); len(kv.Serves) != 0 {
		t.Fatalf("kv is only on the failing path: %+v", kv)
	}
	if !strings.Contains(v.render(candidateRenderCap), "2/10") {
		t.Fatalf("a partial test says how much of it fails:\n%s", v.render(candidateRenderCap))
	}
}

func TestUserTestIncidentListsFailingPathsAndOneHopUpstream(t *testing.T) {
	g := candidateGraph(t)
	// Only the web tier fails (a probe whose path stops there).
	web := candidateTest{ID: "web-only", Symptom: "HTTP 500", Path: []string{"proxy", "web"}, Source: "map", State: testFailing, Failures: 3, Samples: 3}
	v := computeCandidates(candidateInput{Graph: g, Tests: []candidateTest{web, mappedTest(g, "ssh-call", testPassing)}, Onset: candidateOnset, Now: candidateOnset})
	got := map[string]string{}
	for _, n := range v.Nodes {
		got[n.Name] = n.Class
	}
	if got["proxy"] != PositionFailingOnly || got["web"] != PositionFailingOnly {
		t.Fatalf("the failing path: %v", got)
	}
	for _, name := range []string{"db", "kv", "repo", "blobs"} {
		if got[name] != PositionDependency {
			t.Fatalf("%s is what web calls: %v", name, got)
		}
	}
	if _, ok := got["queue"]; ok {
		t.Fatalf("queue only calls db and is not a dependency of web: %v", got)
	}
	if got["shell"] != PositionDependency {
		t.Fatalf("proxy calls shell: %v", got)
	}
	if text := v.render(candidateRenderCap); !strings.Contains(text, "on every failing path") {
		t.Fatalf("a user-test incident states the header:\n%s", text)
	}
}

func TestConditionClosureStopsAtTwoHops(t *testing.T) {
	g := candidateGraph(t)
	v := computeCandidates(candidateInput{Graph: g, Conditions: []string{"proxy"}, Onset: candidateOnset, Now: candidateOnset})
	names := nodeNames(v)
	for _, want := range []string{"proxy", "web", "shell", "db", "kv", "repo", "blobs"} {
		if indexOf(names, want) < 0 {
			t.Fatalf("%s is within two hops of proxy: %v", want, names)
		}
	}
	if indexOf(names, "queue") >= 0 {
		t.Fatalf("queue only calls db: %v", names)
	}
}

func TestNoMapStillRendersFromTheTrafficDescriptors(t *testing.T) {
	failing := candidateTest{ID: "create", Symptom: "HTTP 500", Path: []string{"front", "api"}, Source: "descriptor", State: testFailing, Failures: 3, Samples: 3}
	passing := candidateTest{ID: "list", Path: []string{"front"}, Source: "descriptor", State: testPassing}
	v := computeCandidates(candidateInput{MapNote: "no application map: the accepted architecture has no application-map region", Tests: []candidateTest{failing, passing}, Onset: candidateOnset, Now: candidateOnset,
		Rows: []IncidentChange{
			{At: candidateOnset.Add(-3 * time.Second), Source: "kubernetes", Component: "api", ReaderOrObject: "Deployment/api", Key: "spec.image", Writer: "kubectl-edit"},
			{At: candidateOnset.Add(-9 * time.Second), Source: "kubernetes", Component: "front", ReaderOrObject: "Deployment/front", Key: "spec.image", Writer: "kubectl-edit"},
		}})
	if got := nodeNames(v); len(got) != 2 || got[0] != "api" {
		t.Fatalf("descriptor services are the candidates, nearest evidence first: %v", got)
	}
	text := v.render(candidateRenderCap)
	if !strings.Contains(text, "map: unavailable (no application map") {
		t.Fatalf("a missing map is its own line:\n%s", text)
	}
	if front := nodeNamed(v, "front"); len(front.Serves) != 1 {
		t.Fatalf("exoneration still uses the descriptor paths: %+v", front)
	}
}

func TestEveryDataHolderWithoutObservedStateIsNamedWhetherOrNotACandidate(t *testing.T) {
	in := issuesTitle(t)
	in.Observed = map[string]string{"db": "", "kv": "store read failed: no Ready Pod", "blobs": "no state reader"}
	text := computeCandidates(in).render(candidateRenderCap)
	line := ""
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, "not observed:") {
			line = l
		}
	}
	for _, want := range []string{"kv [data] store read failed", "blobs [data] no state reader"} {
		if !strings.Contains(line, want) {
			t.Fatalf("missing %q in %q", want, line)
		}
	}
	if strings.Contains(line, "db [data]") {
		t.Fatalf("db was observed: %q", line)
	}
	// An off-path holder is named too.
	g := candidateGraph(t)
	off := computeCandidates(candidateInput{Graph: g, Tests: []candidateTest{mappedTest(g, "ssh-call", testFailing)}, Onset: candidateOnset, Now: candidateOnset, Observed: map[string]string{"db": "", "kv": "", "blobs": "no state reader"}}).render(candidateRenderCap)
	if !strings.Contains(off, "not observed: blobs [data] no state reader") {
		t.Fatalf("blobs is off the failing path and still unobserved:\n%s", off)
	}
}

func TestElsewhereNamesEveryOffPathComponentWithARowAndTheNewestThree(t *testing.T) {
	g := candidateGraph(t)
	in := candidateInput{Graph: g, Tests: []candidateTest{mappedTest(g, "ssh-call", testFailing)}, Onset: candidateOnset, Now: candidateOnset.Add(time.Minute)}
	for i, c := range []string{"queue", "db", "kv", "blobs"} {
		in.Rows = append(in.Rows, IncidentChange{At: candidateOnset.Add(-time.Duration(i+1) * time.Minute), Source: "kubernetes", Component: c, ReaderOrObject: "Deployment/" + c, Key: "spec.image", Writer: "kubectl-edit"})
	}
	in.Rows = append(in.Rows, IncidentChange{At: candidateOnset.Add(-30 * time.Second), Source: "app-state", ReaderOrObject: "limits", Key: "rate", Before: "1", After: "2"})
	text := computeCandidates(in).render(candidateRenderCap)
	for _, want := range []string{"elsewhere: 4 rows on 4 components (queue, db, kv, +1)", "unplaced: 1 row on objects the map does not place"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	if strings.Count(text, "  newest: ") != 1 || !strings.Contains(text, "-1m00s queue") || strings.Contains(text, "-4m00s blobs") {
		t.Fatalf("elsewhere keeps the newest rows:\n%s", text)
	}
}

func TestByteCapCollapsesTheLowestRankedNodesFirstAndKeepsTheCause(t *testing.T) {
	in := issuesTitle(t)
	for i := 0; i < 30; i++ {
		in.Rows = append(in.Rows, IncidentChange{At: candidateOnset.Add(-time.Duration(i+1) * time.Second), Source: "kubernetes", Component: "web", ReaderOrObject: "ConfigMap/web-env", Key: strings.Repeat("k", 50), Before: strings.Repeat("a", 60), After: strings.Repeat("b", 60), Writer: "kubectl-edit"})
	}
	v := computeCandidates(in)
	text := v.render(900)
	if len(text) > 900 {
		t.Fatalf("%d bytes:\n%s", len(text), text)
	}
	if !strings.Contains(text, "app definitions") {
		t.Fatalf("the cause on the top-ranked node survives the cap:\n%s", text)
	}
}

func indexOf(items []string, want string) int {
	for i, item := range items {
		if item == want {
			return i
		}
	}
	return -1
}

func TestLongObjectNamesAreNotMistakenForTokens(t *testing.T) {
	e, _ := rowEvidence(IncidentChange{At: candidateOnset.Add(-time.Second), Source: "kubernetes", Component: "web", ReaderOrObject: "Deployment/gitlab-webservice-default", Key: "spec.image", After: "changed", Writer: "kubectl-patch"}, candidateOnset)
	if !strings.Contains(e.Text, "Deployment/gitlab-webservice-default changed vs accepted source") {
		t.Fatalf("object name redacted: %q", e.Text)
	}
}
