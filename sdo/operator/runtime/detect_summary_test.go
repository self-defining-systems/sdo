package runtime

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
	"sdo.dev/operator/sdk/traffic"
)

var detectOnset = time.Date(2026, 11, 12, 7, 28, 9, 0, time.UTC)

// detectFake is a synthetic snapshot whose container logs are canned.
type detectFake struct {
	sdktest.Snapshot
	logs map[string]string
}

func (f detectFake) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	key := ref.Name + "/" + options.Container
	if options.Previous {
		key += "/previous"
	}
	return f.logs[key], nil
}

var detectComponents = []string{"front", "api", "db", "jobs"}

// detectApp is a four-component application: front calls api, api calls db,
// and jobs is off every user path. Two tests: list (front only, passing) and
// create (front, api, db), which fails with a 500.
func detectApp(failing ...string) detectFake {
	s := sdktest.Snapshot{NamespaceName: "app"}
	application := program.AppMap{}
	for _, name := range detectComponents {
		labels := map[string]string{"app": name}
		s.ServiceList = append(s.ServiceList, corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: labels}})
		template := corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}}
		kind := "Deployment"
		if name == "db" {
			kind = "StatefulSet"
			s.StatefulSetList = append(s.StatefulSetList, appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: appsv1.StatefulSetSpec{Template: template}})
		} else {
			s.DeploymentList = append(s.DeploymentList, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: template}})
		}
		s.PodList = append(s.PodList, corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name + "-0", Namespace: "app", Labels: labels, CreationTimestamp: metav1.NewTime(detectOnset.Add(-time.Hour)),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: kind, Name: name, Controller: boolPtr(true)}}},
			Spec: template.Spec,
		})
		application.Components = append(application.Components, program.AppComponent{Name: name, Workload: kind + "/" + name, Services: []string{name}})
	}
	s.NetworkPolicyList = []networkingv1.NetworkPolicy{{ObjectMeta: metav1.ObjectMeta{Name: "restrict-db", Namespace: "app"}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}}}}}
	s.ApplicationMap = &application
	now := detectOnset.Add(34 * time.Second)
	window := traffic.Window{Workload: traffic.Workload{Name: "health"}, ObservedAt: now}
	for _, scenario := range []struct {
		id   string
		path []string
	}{{"list", []string{"front"}}, {"create", []string{"front", "api", "db"}}} {
		observed := traffic.ScenarioObservations{Qualified: true, Scenario: traffic.Descriptor{ID: scenario.id, Target: traffic.Target{Service: scenario.path[0]}, DependsOn: scenario.path[1:]}}
		bad := false
		for _, id := range failing {
			bad = bad || id == scenario.id
		}
		for i := 0; i < 20; i++ {
			sample := traffic.Sample{At: now.Add(-time.Duration(20-i) * time.Second), Outcome: traffic.OutcomeOK, Latency: 10 * time.Millisecond}
			if bad && i >= 8 {
				sample.Outcome, sample.Status, sample.Step, sample.Request = traffic.OutcomeError, 500, "write-row", "POST /items"
				sample.Error = "db: connection refused password=hunter2"
				sample.At = detectOnset.Add(time.Duration(i-8) * time.Second)
			}
			observed.Samples = append(observed.Samples, sample)
		}
		window.Scenarios = append(window.Scenarios, observed)
	}
	s.Traffic = map[string]traffic.Window{"health": window}
	return detectFake{Snapshot: s, logs: map[string]string{
		"db-0/main": detectOnset.Add(-time.Second).Format(time.RFC3339Nano) + " ERROR old\n" +
			detectOnset.Add(time.Second).Format(time.RFC3339Nano) + " ERROR dial tcp 10.0.0.1:5432: i/o timeout\n" +
			detectOnset.Add(2*time.Second).Format(time.RFC3339Nano) + " ERROR again\n",
	}}
}

func probeFinding(scenario string) sdk.Finding {
	return sdk.Finding{DetectorID: memory.TrafficDetectorPrefix + "health", RuleID: traffic.RuleIDPrefix + scenario, Status: sdk.FindingActive, Summary: scenario + " failing"}
}

func boolPtr(b bool) *bool { return &b }

func detectRequest() IncidentRequest {
	changedAt := detectOnset.Add(-4 * time.Second)
	return IncidentRequest{
		IncidentID: "inc-1", Namespace: "app", DetectedAt: detectOnset.Add(5 * time.Second),
		DriftEvents: []sdk.DriftEvent{
			{Ref: sdk.ObjectRef{Kind: "NetworkPolicy", Namespace: "app", Name: "restrict-db"}, Manager: "kubectl", ManagerAt: &changedAt, ObservedAt: changedAt, Fields: []sdk.DriftField{{Path: "spec.egress"}}},
			{Ref: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}, Manager: "sdo", ObservedAt: detectOnset.Add(-time.Hour)},
		},
	}
}

func detectNow() time.Time { return detectOnset.Add(34 * time.Second) }

func TestDetectSummaryShowsUsersFirstThenSuspects(t *testing.T) {
	view := detectApp("create")
	request := detectRequest()
	request.Findings = []sdk.Finding{probeFinding("create"), {DetectorID: "d", RuleID: "events", Status: sdk.FindingActive, Summary: "db Unhealthy readiness probe 2x", PrimaryResource: sdk.ObjectRef{Kind: "StatefulSet", Namespace: "app", Name: "db"}}}
	request.OpeningKeys = []string{findingKey(request.Findings[0])}
	summary := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: view, Now: detectNow()})
	text := summary.Text
	if len(text) > detectSummaryCap {
		t.Fatalf("summary is %d bytes:\n%s", len(text), text)
	}
	// What users see leads: the error response comes before the component path.
	status, path := strings.Index(text, `HTTP 500 "db: connection refused`), strings.Index(text, "front>api>db")
	if status < 0 || path < 0 || status > path {
		t.Fatalf("the user-facing error must come first:\n%s", text)
	}
	for _, want := range []string{"step write-row", "5/5 failing", "since 07:28:09Z", "passing: list", "[redacted]"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "hunter2") {
		t.Fatalf("credential leaked:\n%s", text)
	}
	// db has the latest change before onset, so it leads; front is on a passing path.
	db, api, front := strings.Index(text, "\ndb  StatefulSet/db"), strings.Index(text, "\napi  Deployment/api"), strings.Index(text, "\nfront  Deployment/front")
	if db < 0 || api < 0 || front < 0 || !(db < api && db < front) {
		t.Fatalf("suspect order (db, then the path order):\n%s", text)
	}
	if !strings.Contains(text, "front  Deployment/front  (also on passing paths)") {
		t.Fatalf("front must be marked:\n%s", text)
	}
	for _, want := range []string{
		"change: NetworkPolicy/restrict-db spec.egress changed at 07:28:05Z by kubectl (not sdo)",
		"more: sdo source NetworkPolicy/app/restrict-db",
		`logs: 2 error lines since onset, 1 before: "ERROR dial tcp 10.0.0.1:5432: i/o timeout"`,
		"clue: db Unhealthy readiness probe 2x",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
	if !strings.Contains(text, "recovered: no (") {
		t.Fatalf("recovered state:\n%s", text)
	}
}

func TestDetectSummaryAttachesMatchingPlaybooks(t *testing.T) {
	request := detectRequest()
	request.PlaybookCoverage = &contracts.PlaybookCoverage{Matches: []*contracts.PlaybookMatch{{
		PlaybookId: "restore-policy", CanMitigate: true,
		MitigateCommand: "sdo mitigate restore-policy --bind component=StatefulSet/app/db",
		Bindings:        []*contracts.Binding{{Name: "component", Object: &contracts.ObjectRef{Kind: "StatefulSet", Namespace: "app", Name: "db"}}},
	}}}
	text := buildDetectSummary(detectInput{Request: request, View: detectApp("create"), Now: detectNow()}).Text
	if !strings.Contains(text, "playbooks: restore-policy") || !strings.Contains(text, "more: sdo diagnose restore-policy --bind component=StatefulSet/app/db") {
		t.Fatalf("matching playbook with its diagnose command:\n%s", text)
	}
}

// Every finding, test and suspect is dropped by count, never silently.
func TestDetectSummaryWorstCaseFitsAndCountsWhatItDrops(t *testing.T) {
	long := strings.Repeat("x", 300)
	view := detectApp("create")
	// Eight failing tests, six suspects, twenty clues.
	window := view.Traffic["health"]
	window.Scenarios = nil
	var path []string
	application := program.AppMap{}
	for i := 0; i < 6; i++ {
		name := fmt.Sprintf("component-%d-%s", i, long[:40])
		path = append(path, name)
		application.Components = append(application.Components, program.AppComponent{Name: name, Workload: "Deployment/" + name, Services: []string{name}})
	}
	for i := 0; i < 8; i++ {
		observed := traffic.ScenarioObservations{Qualified: true, Scenario: traffic.Descriptor{ID: fmt.Sprintf("test-%d-%s", i, long[:30]), Target: traffic.Target{Service: path[0]}, DependsOn: path[1:]}}
		for j := 0; j < 20; j++ {
			observed.Samples = append(observed.Samples, traffic.Sample{At: detectOnset.Add(time.Duration(j) * time.Second), Outcome: traffic.OutcomeError, Status: 503, Step: long[:50], Error: long})
		}
		window.Scenarios = append(window.Scenarios, observed)
	}
	view.Traffic = map[string]traffic.Window{"health": window}
	view.ApplicationMap = &application
	request := detectRequest()
	var findings []sdk.Finding
	for i := 0; i < 20; i++ {
		findings = append(findings, sdk.Finding{DetectorID: "d", RuleID: fmt.Sprintf("r%d", i), Status: sdk.FindingActive, Summary: long, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: fmt.Sprintf("other-%d", i)}})
	}
	request.Findings = append([]sdk.Finding{probeFinding("test-0")}, findings...)
	request.OpeningKeys = []string{findingKey(request.Findings[0])}
	summary := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: view, Now: detectNow()})
	if len(summary.Text) > detectSummaryCap {
		t.Fatalf("worst case is %d bytes:\n%s", len(summary.Text), summary.Text)
	}
	shown := strings.Count(summary.Text, "more: sdo detect --finding test-")
	dropped := regexp.MustCompile(`\+(\d+) more: sdo detect --finding tests`).FindStringSubmatch(summary.Text)
	if shown+atoi(t, dropped) != 8 {
		t.Fatalf("%d tests shown, dropped note %v; want all 8 counted:\n%s", shown, dropped, summary.Text)
	}
	clues := regexp.MustCompile(`\+(\d+) more: sdo detect --finding background`).FindStringSubmatch(summary.Text)
	if atoi(t, clues) < 18 {
		t.Fatalf("clues must be counted when dropped (%v):\n%s", clues, summary.Text)
	}
	assertMoreLinesParse(t, summary)
}

func atoi(t *testing.T, match []string) int {
	t.Helper()
	if len(match) != 2 {
		return 0
	}
	n, err := strconv.Atoi(match[1])
	if err != nil {
		t.Fatal(err)
	}
	return n
}

var moreLine = regexp.MustCompile(`more: (.+)$`)

// Every `more:` line is a command SDO or kubectl runs, and every `sdo detect
// --finding` handle resolves.
func assertMoreLinesParse(t *testing.T, summary detectSummary) {
	t.Helper()
	for _, line := range strings.Split(summary.Text, "\n") {
		match := moreLine.FindStringSubmatch(line)
		if match == nil {
			continue
		}
		command := match[1]
		switch {
		case strings.HasPrefix(command, "sdo detect --finding "):
			id := strings.TrimPrefix(command, "sdo detect --finding ")
			if _, ok := summary.item(id); !ok {
				t.Fatalf("handle %q resolves to nothing:\n%s", id, summary.Text)
			}
		case strings.HasPrefix(command, "sdo source "), strings.HasPrefix(command, "sdo diagnose "), strings.HasPrefix(command, "kubectl "):
		default:
			t.Fatalf("more: line is not a command: %q", line)
		}
	}
}

func TestDetectSummaryItemsResolve(t *testing.T) {
	request := detectRequest()
	finding := sdk.Finding{DetectorID: "d", RuleID: "events", Status: sdk.FindingActive, Summary: "queue backlog", Evidence: "depth 900", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "jobs"}}
	request.Findings = []sdk.Finding{probeFinding("create"), finding}
	request.OpeningKeys = []string{findingKey(request.Findings[0])}
	summary := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: detectApp("create"), Now: detectNow()})
	assertMoreLinesParse(t, summary)
	for id, want := range map[string]string{"create": "write-row", "tests": "create", "db": "change:", "background": "queue backlog", shortID(FindingFingerprint(finding))[:4]: "depth 900"} {
		text, ok := summary.item(id)
		if !ok || !strings.Contains(text, want) {
			t.Fatalf("item %q = %q, %v; want %q", id, text, ok, want)
		}
	}
	if _, ok := summary.item("no-such-item"); ok {
		t.Fatal("an unknown item must not resolve")
	}
}

func TestDetectSummaryForAKubernetesCondition(t *testing.T) {
	view := detectApp()
	finding := sdk.Finding{DetectorID: "workload-health", RuleID: "crash-looping", Status: sdk.FindingActive, Summary: "1 of 2 Pods CrashLoopBackOff", PrimaryResource: sdk.ObjectRef{Kind: "Deployment", Namespace: "app", Name: "api"}}
	request := detectRequest()
	request.Findings = []sdk.Finding{finding}
	request.OpeningKeys = []string{findingKey(finding)}
	text := buildDetectSummary(detectInput{Request: request, Findings: request.Findings, View: view, Now: detectNow()}).Text
	for _, want := range []string{"opened on: Kubernetes condition", "Deployment/api  1 of 2 Pods CrashLoopBackOff", "more: kubectl -n app describe deployment api", "passing: create list", "api  Deployment/api"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q:\n%s", want, text)
		}
	}
}

func TestDetectCommandReturnsTheCompactText(t *testing.T) {
	view := detectApp("create")
	request := detectRequest()
	loop := NewResponderLoop(request, nil, staticProvider{view.Snapshot}, nil, PlanExecutor{}, 10)
	loop.ReadView = func(v sdk.DetectionContext) program.ReadView { return program.ReadFrom(v) }
	response := loop.Handle(context.Background(), ToolRequest{Command: "detect"})
	if response.Error != "" || !strings.Contains(response.Text, "OPENED ON") {
		t.Fatalf("detect text = %q, error %q", response.Text, response.Error)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "detect", Finding: "create"})
	if response.Error != "" || !strings.Contains(response.Text, "last failing samples") {
		t.Fatalf("--finding create = %q, error %q", response.Text, response.Error)
	}
	response = loop.Handle(context.Background(), ToolRequest{Command: "detect", Finding: "nothing"})
	if response.Error == "" {
		t.Fatal("an unknown --finding is an error")
	}
}
