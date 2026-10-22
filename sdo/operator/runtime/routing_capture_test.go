package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/runtime/prober"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/program/testdata/routingport"
	"sdo.dev/operator/sdk/sdktest"
)

type routingTestProber struct {
	mu        sync.Mutex
	calls     []string
	deadlines []time.Duration
}

func (p *routingTestProber) Probe(ctx context.Context, ref sdk.ObjectRef, port int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, fmt.Sprintf("%s/%s:%d", ref.Kind, ref.Name, port))
	if deadline, ok := ctx.Deadline(); ok {
		p.deadlines = append(p.deadlines, time.Until(deadline))
	}
	return program.ProbeResult{Success: false, Detail: "refused"}, nil
}
func (p *routingTestProber) ProbePod(ctx context.Context, ref sdk.ObjectRef, ip string, port int32) (program.ProbeResult, error) {
	wanted := "10.0.0.5"
	if ref.Name == "peer-pod" {
		wanted = "10.0.0.6"
	}
	if ip != wanted {
		return program.ProbeResult{}, fmt.Errorf("wrong captured PodIP %s", ip)
	}
	result, err := p.Probe(ctx, ref, port, nil)
	result.Success = port == 8080
	return result, err
}
func TestLiveReadViewConcurrentPodTCPProbes(t *testing.T) {
	probe := &routingTestProber{}
	view := LiveReadView{DetectionContext: routingCaptureSnapshot(), Prober: probe}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 32 {
		workers.Go(func() {
			<-start
			result, err := view.ProbeTCP(ref, 8080)
			if err != nil || !result.Success {
				t.Errorf("concurrent Pod probe: %+v %v", result, err)
			}
		})
	}
	close(start)
	workers.Wait()
	if len(probe.calls) != 32 || len(probe.deadlines) != 32 {
		t.Fatalf("lost probe observations: calls=%d deadlines=%d", len(probe.calls), len(probe.deadlines))
	}
}

func routingCaptureSnapshot() sdktest.Snapshot {
	return sdktest.Snapshot{NamespaceName: "app", ServiceList: []corev1.Service{{ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "app"}, Spec: corev1.ServiceSpec{Selector: map[string]string{"app": "api"}, Ports: []corev1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(9999)}}}}}, PodList: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "api-pod", Namespace: "app", Labels: map[string]string{"app": "api"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "api", Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}}}, Status: corev1.PodStatus{PodIP: "10.0.0.5", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}}}
}
func TestLiveReadViewPodTCPUsesCapturedAddress(t *testing.T) {
	probe := &routingTestProber{}
	view := LiveReadView{DetectionContext: routingCaptureSnapshot(), Prober: probe}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
	result, err := view.ProbeTCP(ref, 8080)
	if err != nil || !result.Success {
		t.Fatalf("pod listener observation %+v %v", result, err)
	}
	ref.Namespace = "elsewhere"
	if _, err := view.ProbeTCP(ref, 8080); err == nil {
		t.Fatal("out-of-namespace Pod probed")
	}
	ref.Namespace = "app"
	ref.Name = "absent"
	if _, err := view.ProbeTCP(ref, 8080); err == nil {
		t.Fatal("missing Pod probed")
	}
}
func TestIncidentCaptureRecordsRoutingTCPBeforeAnyProgramExists(t *testing.T) {
	ctx := context.Background()
	snapshot := routingCaptureSnapshot()
	probe := &routingTestProber{}
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	loop := NewResponderLoop(IncidentRequest{IncidentID: "routing", Namespace: "app", Findings: []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: ref}}}, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{}, 1)
	loop.CorpusSessionRoot = t.TempDir()
	loop.ReadView = func(s sdk.DetectionContext) program.ReadView { return LiveReadView{DetectionContext: s, Prober: probe} }
	loop.InitialDiagnose(ctx)
	dir, err := memory.IncidentCorpusDir(loop.CorpusSessionRoot, "routing")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(data, &calls); err != nil {
		t.Fatal(err)
	}
	view := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
	pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
	for _, port := range []int32{8080, 9999} {
		result, err := view.ProbeTCP(pod, port)
		if err != nil || result.Success != (port == 8080) {
			t.Fatalf("captured port %d: %+v %v; calls=%+v", port, result, err, calls)
		}
	}
	if _, err := view.ProbeTCP(pod, 12345); err == nil {
		t.Fatal("unrecorded probe invented")
	}
	if len(probe.calls) == 0 || len(probe.calls) > 24 {
		t.Fatalf("unbounded or missing capture: %+v", probe.calls)
	}
	for _, deadline := range probe.deadlines {
		if deadline < time.Second || deadline > routingCaptureProbeTimeout {
			t.Fatalf("routing probe deadline %s must be generous yet bounded by %s", deadline, routingCaptureProbeTimeout)
		}
	}
	prior := len(probe.calls)
	loop.InitialDiagnose(ctx)
	if len(probe.calls) != prior {
		t.Fatal("repeated detect capture probed again")
	}
}

func TestRoutingCaptureHasCountAndTimeBudgets(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprint(blocked), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				snapshot := routingCaptureSnapshot()
				base := snapshot.ServiceList[0]
				snapshot.ServiceList = nil
				findings := []sdk.Finding{}
				for i := 0; i < 40; i++ {
					service := *base.DeepCopy()
					service.Name = fmt.Sprintf("api-%02d", i)
					snapshot.ServiceList = append(snapshot.ServiceList, service)
					findings = append(findings, sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: service.Name}})
				}
				probe := &boundedRoutingProber{blocked: blocked}
				root := t.TempDir()
				if err := memory.CaptureIncidentSnapshot(root, "bounded", "open@detect", snapshot, "app"); err != nil {
					t.Fatal(err)
				}
				start := time.Now()
				err := captureRoutingEvidence(context.Background(), root, "bounded", "open@detect", findings, LiveReadView{DetectionContext: snapshot, Prober: probe})
				if err != nil {
					t.Fatal(err)
				}
				if probe.calls == 0 || probe.calls > maxRoutingCaptureProbes || time.Since(start) > routingCaptureBudget {
					t.Fatalf("capture exceeded bounds: calls=%d duration=%s", probe.calls, time.Since(start))
				}
			})
		})
	}
}

// Probes skipped for lack of budget are missing evidence, not a complete sample.
func TestRoutingCaptureNotesProbesSkippedForBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		snapshot := routingCaptureSnapshot()
		base := snapshot.ServiceList[0]
		snapshot.ServiceList = nil
		findings := []sdk.Finding{}
		for i := 0; i < 10; i++ {
			service := *base.DeepCopy()
			service.Name = fmt.Sprintf("api-%02d", i)
			snapshot.ServiceList = append(snapshot.ServiceList, service)
			findings = append(findings, sdk.Finding{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: service.Name}})
		}
		probe := &boundedRoutingProber{delay: routingCaptureBudget / 4}
		root := t.TempDir()
		if err := memory.CaptureIncidentSnapshot(root, "skipped", "open@detect", snapshot, "app"); err != nil {
			t.Fatal(err)
		}
		if err := captureRoutingEvidence(context.Background(), root, "skipped", "open@detect", findings, LiveReadView{DetectionContext: snapshot, Prober: probe}); err != nil {
			t.Fatal(err)
		}
		dir, err := memory.IncidentCorpusDir(root, "skipped")
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
		if err != nil {
			t.Fatalf("budget-truncated capture left no note: %v", err)
		}
		var note routingCaptureNote
		if err := json.Unmarshal(raw, &note); err != nil || note.Reason != "budget_exhausted" {
			t.Fatalf("wrong note %s %v", raw, err)
		}
	})
}

type boundedRoutingProber struct {
	mu      sync.Mutex
	blocked bool
	delay   time.Duration
	calls   int
}

func (p *boundedRoutingProber) Probe(ctx context.Context, _ sdk.ObjectRef, _ int32, _ *program.HTTPProbe) (program.ProbeResult, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.blocked {
		<-ctx.Done()
		return program.ProbeResult{}, ctx.Err()
	}
	time.Sleep(p.delay)
	return program.ProbeResult{Success: true}, nil
}
func (p *boundedRoutingProber) ProbePod(ctx context.Context, ref sdk.ObjectRef, _ string, port int32) (program.ProbeResult, error) {
	return p.Probe(ctx, ref, port, nil)
}

func TestHermeticRoutingCaptureCompilesReplayableMitigator(t *testing.T) {
	ctx := context.Background()
	snapshot := routingCaptureSnapshot()
	peer := *snapshot.ServiceList[0].DeepCopy()
	peer.Name = "peer"
	peer.Spec.Selector = map[string]string{"app": "peer"}
	peer.Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	peerPod := *snapshot.PodList[0].DeepCopy()
	peerPod.Name = "peer-pod"
	peerPod.Status.PodIP = "10.0.0.6"
	peerPod.Labels = map[string]string{"app": "peer"}
	snapshot.ServiceList = append(snapshot.ServiceList, peer)
	snapshot.PodList = append(snapshot.PodList, peerPod)
	provider := &changingProvider{snapshot: snapshot}
	probe := inProcessProberClient(noTrafficProberHandler(t))
	ref := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
	finding := sdk.Finding{DetectorID: "health", RuleID: "routing", Status: sdk.FindingActive, Severity: sdk.SeverityWarning, PrimaryResource: ref, Summary: "routing unavailable", Evidence: "no Service connection"}
	detector := controllerDetector("health", time.Second, finding, finding, sdk.Finding{}, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 2, Clearing: 2}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityWarning}
	detector.spec.OriginatingCommit = "health-objective"
	captureRoot := t.TempDir()
	config := testControllerConfig()
	config.Namespace = "app"
	config.CorpusSessionRoot = captureRoot
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 1)}
	controller, err := NewController(config, []sdk.Detector{detector}, provider, dispatcher, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	controller.ConfigureDrift(DriftControllerConfig{View: func(s sdk.DetectionContext) program.ReadView { return LiveReadView{DetectionContext: s, Prober: probe} }})
	for i := 0; i < 2; i++ {
		if err := controller.Step(ctx, time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	executePendingEffect(t, controller)
	request := awaitRequest(t, dispatcher.requests)
	captureDir, err := memory.IncidentCorpusDir(captureRoot, request.IncidentID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ServiceList[0].Spec.Ports[0].TargetPort = intstr.FromInt32(8080)
	provider.snapshot = snapshot
	awaitDispatchCompletionQueued(t, controller)
	for i := 2; i < 4; i++ {
		if err := controller.Step(ctx, time.Unix(int64(i), 0), nil); err != nil {
			t.Fatal(err)
		}
	}
	root := nativeRepository(t)
	copyFastloopTree(t, "../memory/validate/testdata/good/.sdo", filepath.Join(root, ".sdo"))
	module, err := os.ReadFile("../../../tests/fixtures/sdo/playbook_program_app/.sdo/diagnostics/go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".sdo/diagnostics/go.mod"), module, 0644); err != nil {
		t.Fatal(err)
	}
	programDir := filepath.Join(root, ".sdo/playbooks/service-targetport-mismatch")
	if err := os.MkdirAll(programDir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"diagnose.go", "mitigate.go"} {
		source, err := os.ReadFile(filepath.Join("../../../examples/playbook-programs/playbooks/service-targetport-mismatch", name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(programDir, name), source, 0644); err != nil {
			t.Fatal(err)
		}
	}
	readme := []byte("---\nschema_version: 1\nowner: responder\nfault_class: service-targetport-mismatch\noriginating_incident: " + request.IncidentID + "\n---\n\nRestore `<SERVICE>` target port only after observed selected-Pod listener evidence.\n")
	if err := os.WriteFile(filepath.Join(programDir, "README.md"), readme, 0644); err != nil {
		t.Fatal(err)
	}
	writeCapturedProgramCorpus(t, root, captureDir, request.IncidentID, "service-targetport-mismatch", "service", "Service", "app", "api", "peer", "detect")
	operatorRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	var notes []sdktest.MitigationReplayResult
	diagnostics, err := validate.CompileCandidate(ctx, validate.Request{Root: root, OperatorRoot: operatorRoot, MitigationReplayNotes: &notes})
	if err != nil || len(diagnostics) != 0 {
		t.Fatalf("captured routing diagnosis/generalization/mitigation gate: %+v %v", diagnostics, err)
	}
	if len(notes) != 1 || notes[0].Status != sdktest.MitigationReplayPassed {
		t.Fatalf("SetField was not replayed: %+v", notes)
	}

	// A trusted generalized routing program can cover both targetPort faults.
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "accepted recorded routing fixture")
	commit := nativeGit(t, root, "rev-parse", "HEAD")
	revision, err := programDependencyRevision(ctx, root, commit, "service-targetport-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	for i := range snapshot.ServiceList {
		snapshot.ServiceList[i].Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	}
	client := fake.NewSimpleClientset(&snapshot.ServiceList[0], &snapshot.ServiceList[1], &snapshot.PodList[0], &snapshot.PodList[1])
	liveProvider := fastloopObjects{client: client}
	p := routingport.Program{}
	registry, err := program.NewRegistry([]program.Diagnoser{p}, []program.Mitigator{p})
	if err != nil {
		t.Fatal(err)
	}
	repeated := goldenRequest(t)
	repeated.IncidentID = "two-routing-faults"
	repeated.Namespace = "app"
	repeated.SourceCommit = commit
	repeated.RepositoryBaseCommit = commit
	repeated.RepositoryWorktree = root
	repeated.RepairPolicy = "recorded-actions"
	repeated.Findings, err = (fastloopDetector{}).Detect(ctx, snapshot)
	if err != nil || len(repeated.Findings) != 2 {
		t.Fatalf("two routing findings: %+v %v", repeated.Findings, err)
	}
	outcomes := []memory.Outcome{}
	for i := 0; i < 3; i++ {
		outcomes = append(outcomes, memory.Outcome{IncidentID: fmt.Sprintf("routing-success-%d", i), Record: verifiedTrustOutcome(t, p.Spec().ID, revision)})
	}
	warm := KubernetesJobDispatcher{Client: client, Namespace: "app", Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), ProgramFastPath: true, ServiceAccount: "sdo-responder", RepositoryPVC: "repository", RepositoryMountPath: root, Outcomes: priorOutcomeReader{outcomes: outcomes}, LoopFactory: func(request IncidentRequest) *ResponderLoop {
		loop := NewResponderLoop(request, registry, liveProvider, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: TypedPlanClient{Client: client}, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: liveProvider}}, 1)
		loop.ReadView = func(s sdk.DetectionContext) program.ReadView { return LiveReadView{DetectionContext: s, Prober: probe} }
		loop.CorpusReplayPassed = true
		loop.DiagnosisSettleDelay = time.Millisecond
		return loop
	}}
	repeated, err = warm.PrepareDispatch(ctx, repeated)
	if err != nil {
		t.Fatal(err)
	}
	result, err := warm.Dispatch(ctx, repeated)
	if err != nil || result.ResolutionPath != "fast_path" || len(result.PlanApplications) != 1 || len(result.PlanApplications[0].Result.ActionsApplied) != 2 || result.Usage.GetInputTokens() != 0 {
		t.Fatalf("two-targetPort fast path: %+v %v", result, err)
	}
	jobs, err := client.BatchV1().Jobs("app").List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatalf("routing responder dispatched: %+v %v", jobs, err)
	}
	t.Log("captured TCP evidence passes fixed/healthy peer, rename and SetField repair replay; at three prior credits both targetPort faults resolve with fresh live probes and no responder Job")
}

func TestRoutingCaptureRecordsUnavailableCapabilityOnce(t *testing.T) {
	unavailable := HTTPProberClient{BaseURL: func(context.Context, bool) (string, error) {
		return "", errors.New("prober pod is not ready")
	}}
	for _, test := range []struct {
		name   string
		view   func(sdk.DetectionContext) program.ReadView
		reason string
	}{
		{"missing", func(s sdk.DetectionContext) program.ReadView { return LiveReadView{DetectionContext: s} }, "prober_not_configured"},
		{"non-live", func(s sdk.DetectionContext) program.ReadView {
			return &sdktest.ReplayReadView{Snapshot: s.(sdktest.Snapshot)}
		}, "read_view_not_live"},
		{"unreachable", func(s sdk.DetectionContext) program.ReadView {
			return LiveReadView{DetectionContext: s, Prober: unavailable}
		}, "probe_failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := routingCaptureSnapshot()
			root := t.TempDir()
			findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
			loop := NewResponderLoop(IncidentRequest{IncidentID: "no-prober", Namespace: "app", Findings: findings}, nil, &changingProvider{snapshot: snapshot}, nil, PlanExecutor{}, 1)
			loop.CorpusSessionRoot, loop.ReadView = root, test.view
			var logs bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			defer slog.SetDefault(previous)
			loop.InitialDiagnose(context.Background())
			loop.InitialDiagnose(context.Background())
			// A later capture phase must preserve and not log the same gap again.
			if err := memory.CaptureIncidentSnapshot(root, "no-prober", "fixed", snapshot, "app"); err != nil {
				t.Fatal(err)
			}
			if err := captureRoutingEvidence(context.Background(), root, "no-prober", "fixed", findings, test.view(snapshot)); err != nil {
				t.Fatal(err)
			}
			dir, err := memory.IncidentCorpusDir(root, "no-prober")
			if err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
			if err != nil {
				t.Fatalf("missing typed unavailable capture note: %v", err)
			}
			var note routingCaptureNote
			if err := json.Unmarshal(raw, &note); err != nil {
				t.Fatal(err)
			}
			if note.Capability != "routing_probe" || note.Status != "unavailable" || note.Reason != test.reason || note.Phase != "open@detect" {
				t.Fatalf("wrong capability note: %s", raw)
			}
			if test.reason == "probe_failed" && !strings.Contains(note.Detail, "prober pod is not ready") {
				t.Fatalf("missing failure detail: %s", raw)
			}
			if strings.Count(logs.String(), "routing_probe=unavailable:"+test.reason) != 1 {
				t.Fatalf("capability must be logged once per incident: %s", &logs)
			}
			calls, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
			if err != nil {
				t.Fatal(err)
			}
			var recordings []sdktest.PrimitiveCall
			if err := json.Unmarshal(calls, &recordings); err != nil {
				t.Fatal(err)
			}
			for _, call := range recordings {
				if call.Method == "ProbeTCP" && call.Error == "" {
					t.Fatalf("unavailable probe invented an observation: %+v", call)
				}
			}
		})
	}
}

func TestRoutingCaptureRecordsHangingDialAsFailedObservation(t *testing.T) {
	for _, kind := range []string{"Service", "Pod"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
					if strings.HasSuffix(address, ":9999") {
						if (kind == "Pod") == strings.HasPrefix(address, "10.0.0.5:") {
							<-ctx.Done()
							return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
						}
						return nil, errors.New("connection refused")
					}
					left, right := net.Pipe()
					_ = right.Close()
					return left, nil
				}})
				if err != nil {
					t.Fatal(err)
				}
				snapshot := routingCaptureSnapshot()
				root := t.TempDir()
				const incident = "hanging-dial"
				if err := memory.CaptureIncidentSnapshot(root, incident, "open@detect", snapshot, "app"); err != nil {
					t.Fatal(err)
				}
				service := sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}
				findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: service}}
				view := LiveReadView{DetectionContext: snapshot, Prober: inProcessProberClient(prober.Handler(p))}
				if err := captureRoutingEvidence(context.Background(), root, incident, "open@detect", findings, view); err != nil {
					t.Fatal(err)
				}
				dir, err := memory.IncidentCorpusDir(root, incident)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
				if err != nil {
					t.Fatal(err)
				}
				var calls []sdktest.PrimitiveCall
				if err := json.Unmarshal(raw, &calls); err != nil {
					t.Fatal(err)
				}
				if len(calls) != 4 {
					t.Fatalf("lost other routing results: %+v", calls)
				}
				replay := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
				pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
				for _, ref := range []sdk.ObjectRef{pod, service} {
					result, err := replay.ProbeTCP(ref, 9999)
					if err != nil || result.Success || result.Detail == "" {
						t.Fatalf("failed dial must replay as an observation: %+v %v", result, err)
					}
					if ref.Kind == kind && !strings.Contains(result.Detail, "deadline exceeded") {
						t.Fatalf("missing dial timeout detail: %+v", result)
					}
				}
				if result, err := replay.ProbeTCP(pod, 8080); err != nil || !result.Success {
					t.Fatalf("lost successful peer probe: %+v %v", result, err)
				}
				if _, err := os.Stat(filepath.Join(dir, "routing-probe-note.json")); !os.IsNotExist(err) {
					t.Fatalf("dial timeout marked routing unavailable: %v", err)
				}
			})
		})
	}
}

// inProcessProber serves the real prober API through the client's transport, so
// tests exercise the HTTP contract without sockets and run on fake time.
type inProcessProber struct{ handler http.Handler }

func (p inProcessProber) RoundTrip(request *http.Request) (*http.Response, error) {
	recorder := httptest.NewRecorder()
	p.handler.ServeHTTP(recorder, request)
	return recorder.Result(), nil
}

func inProcessProberClient(handler http.Handler) HTTPProberClient {
	return HTTPProberClient{BaseURL: StaticProberURL("http://prober.invalid"), Client: &http.Client{Transport: inProcessProber{handler}}}
}

// A healthy but CPU-starved prober answers slowly. That is wall-clock noise,
// not routing evidence: the capture must wait for the real answer instead of
// recording the probe as unavailable.
func TestRoutingCaptureWaitsForSlowHealthyProber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
			select {
			case <-time.After(750 * time.Millisecond): // a starved dial that still completes
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			if !strings.HasSuffix(address, ":8080") {
				return nil, errors.New("connection refused")
			}
			left, right := net.Pipe()
			_ = right.Close()
			return left, nil
		}})
		if err != nil {
			t.Fatal(err)
		}
		snapshot := routingCaptureSnapshot()
		root := t.TempDir()
		const incident = "slow-healthy"
		if err := memory.CaptureIncidentSnapshot(root, incident, "open@detect", snapshot, "app"); err != nil {
			t.Fatal(err)
		}
		findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: "api"}}}
		view := LiveReadView{DetectionContext: snapshot, Prober: inProcessProberClient(prober.Handler(p))}
		if err := captureRoutingEvidence(context.Background(), root, incident, "open@detect", findings, view); err != nil {
			t.Fatal(err)
		}
		dir, err := memory.IncidentCorpusDir(root, incident)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(dir, "routing-probe-note.json")); !os.IsNotExist(err) {
			note, _ := os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
			t.Fatalf("slow healthy prober marked routing unavailable: %s", note)
		}
		raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
		if err != nil {
			t.Fatal(err)
		}
		var calls []sdktest.PrimitiveCall
		if err := json.Unmarshal(raw, &calls); err != nil {
			t.Fatal(err)
		}
		replay := &sdktest.ReplayReadView{Snapshot: snapshot, Calls: calls}
		pod := sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api-pod"}
		if result, err := replay.ProbeTCP(pod, 8080); err != nil || !result.Success {
			t.Fatalf("slow listener not observed: %+v %v calls=%+v", result, err, calls)
		}
		if result, err := replay.ProbeTCP(pod, 9999); err != nil || result.Success || result.Detail != "connection refused" {
			t.Fatalf("slow refusal must replay as the dial's own answer: %+v %v", result, err)
		}
	})
}

func TestRoutingCaptureContinuesAfterProberTransportFailure(t *testing.T) {
	p, err := prober.New(prober.Config{Namespace: "app", DialContext: func(context.Context, string, string) (net.Conn, error) {
		left, right := net.Pipe()
		_ = right.Close()
		return left, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	api := inProcessProberClient(prober.Handler(p))
	resolutions := 0
	client := HTTPProberClient{BaseURL: func(context.Context, bool) (string, error) {
		resolutions++
		if resolutions <= 2 { // Both attempts for the first probe fail.
			return "", errors.New("prober unreachable")
		}
		return "http://prober.invalid", nil
	}, Client: api.Client}
	snapshot := routingCaptureSnapshot()
	root := t.TempDir()
	const incident = "transport-failure"
	if err := memory.CaptureIncidentSnapshot(root, incident, "open@detect", snapshot, "app"); err != nil {
		t.Fatal(err)
	}
	findings := []sdk.Finding{{Status: sdk.FindingActive, PrimaryResource: sdk.ObjectRef{Kind: "Service", Namespace: "app", Name: "api"}}}
	if err := captureRoutingEvidence(context.Background(), root, incident, "open@detect", findings, LiveReadView{DetectionContext: snapshot, Prober: client}); err != nil {
		t.Fatal(err)
	}
	dir, err := memory.IncidentCorpusDir(root, incident)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "open@detect-recordings.json"))
	if err != nil {
		t.Fatal(err)
	}
	var calls []sdktest.PrimitiveCall
	if err := json.Unmarshal(raw, &calls); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 4 || !strings.Contains(calls[0].Error, "prober unreachable") {
		t.Fatalf("missing transport failure: %+v", calls)
	}
	for _, call := range calls[1:] {
		var result program.ProbeResult
		if err := json.Unmarshal(call.Result, &result); err != nil || call.Error != "" || !result.Success {
			t.Fatalf("transport failure lost later result: %+v %v", call, err)
		}
	}
	raw, err = os.ReadFile(filepath.Join(dir, "routing-probe-note.json"))
	if err != nil || !bytes.Contains(raw, []byte("prober unreachable")) {
		t.Fatalf("missing transport capability note: %s %v", raw, err)
	}
}
