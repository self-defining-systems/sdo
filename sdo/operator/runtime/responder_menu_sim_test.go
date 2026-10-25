package runtime

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

type menuSimTask struct {
	request  IncidentRequest
	result   IncidentResult
	done     chan struct{}
	sessions map[string]bool
	reports  []*contracts.MitigationReport
}

// Rejoining a controller dispatch reconnects to the existing scripted worker,
// exactly as a surviving responder Job does in the restart simulation harness.
type menuSimDispatcher struct {
	mu      sync.Mutex
	tasks   map[string]*menuSimTask
	started chan *menuSimTask
}

func (d *menuSimDispatcher) Dispatch(ctx context.Context, r IncidentRequest) (IncidentResult, error) {
	d.mu.Lock()
	task := d.tasks[r.IncidentID]
	if task == nil {
		task = &menuSimTask{request: r, done: make(chan struct{}), sessions: map[string]bool{}}
		d.tasks[r.IncidentID] = task
		d.started <- task
	}
	d.mu.Unlock()
	select {
	case <-task.done:
		return task.result, nil
	case <-ctx.Done():
		return IncidentResult{}, ctx.Err()
	}
}

type menuSimProgram struct {
	id   string
	port int
}

func (p menuSimProgram) Spec() program.ProgramSpec {
	return program.ProgramSpec{ID: p.id, Params: []program.ParamSpec{{Name: "service", Kind: "Service", Required: true}, {Name: "port", ValueKind: program.IntValue, Default: p.port}}}
}
func (p menuSimProgram) Diagnose(_ context.Context, v program.ReadView, b program.Bindings) (program.Verdict, error) {
	for _, service := range v.Services() {
		if service.Name == b.Objects["service"].Name && service.Spec.Ports[0].TargetPort.IntVal != 8080 {
			return program.Verdict{Result: program.Match}, nil
		}
	}
	return program.Verdict{Result: program.NoMatch}, nil
}
func (p menuSimProgram) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	return program.Plan{Actions: []program.Action{program.SetField{Ref: b.Objects["service"], Path: "spec.ports[0].targetPort", Value: program.FromParam("port")}}}, nil
}

func TestSimResponderMenuSeededSchedules(t *testing.T) {
	// Fixed seeds and a finite tick budget keep this sweep below two minutes.
	for _, seed := range []uint64{7, 81} {
		for _, schedule := range []string{"alternate", "five-fail-next", "restart-between", "restart-mid-option", "health-flip"} {
			t.Run(fmt.Sprintf("seed-%d/%s", seed, schedule), func(t *testing.T) { testSimResponderMenuSchedule(t, seed, schedule) })
		}
	}
}

func testSimResponderMenuSchedule(t *testing.T, seed uint64, schedule string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	w := newSimWorld(t, seed)
	// Isolate the requested schedules; broad store/API faults have their own sweep.
	w.client.ReactionChain = w.client.ReactionChain[2:]
	w.kube.faults.churnPerPatch = func() int { return 0 }
	w.kube.faults.failAfterCommit = func() bool { return false }
	w.cache.faults.staleReads = func() int { return 0 }
	accepted := nativeRepository(t)
	nativeMemoryFixture(t, accepted)
	revision := nativeGit(t, accepted, "rev-parse", "HEAD")
	repository, err := broker.New(accepted, filepath.Join(t.TempDir(), "proposals"), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	native := &NativeIncidentBroker{Repository: repository, LedgerRoot: t.TempDir(), Now: w.clock.Now, Deriver: ProtoOutcomeDeriver{}, ResponderBackend: "scripted", ResponderModel: "none", Reflector: ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		return ReflectionDecision{LearningDecision: "no_change", NoChangeReason: "synthetic simulation"}, nil
	})}
	driver := &menuSimDispatcher{tasks: map[string]*menuSimTask{}, started: make(chan *menuSimTask, 4)}
	w.config.SourceCommit = revision
	w.config.DeployedCommit = revision
	w.config.RepositoryWorktree = accepted
	w.config.VerificationTimeout = 5 * time.Second
	bind := func() {
		w.controller.dispatcher = driver
		if err := w.controller.SetIncidentBroker(native); err != nil {
			t.Fatal(err)
		}
	}
	w.restart()
	bind()
	restart := func() { w.persist(); w.restart(); bind() }
	bad, good := menuSimProgram{"fail", 9999}, menuSimProgram{"repair", 8080}
	registry, err := program.NewRegistry([]program.Diagnoser{bad, good}, []program.Mitigator{bad, good})
	if err != nil {
		t.Fatal(err)
	}
	interrupted := false
	badRounds := 1 + w.rng.IntN(2)
	restartWrite := 1 + w.rng.IntN(badRounds+1)
	writes := 0
	flipService := simServiceName(1 + w.rng.IntN(simStreamServices-1))
	runTask := func(task *menuSimTask, failAll bool) {
		request := task.request
		var saved []byte
		var lostReplySnapshot []byte
		newLoop := func() *ResponderLoop {
			loop := NewResponderLoop(request, registry, w.cache, []sdk.Detector{fastloopDetector{}}, PlanExecutor{Client: w.kube, Rollouts: &planFakeRollout{}, Verifier: PlanHealthVerifier{Detectors: []sdk.Detector{fastloopDetector{}}, Provider: w.cache}, Observer: w.cache, Now: w.clock.Now}, 5)
			loop.MenuConfig = productionResponderMenu()
			loop.ReadView = func(v sdk.DetectionContext) program.ReadView {
				return &sdktest.ReplayReadView{Snapshot: v.(sdktest.Snapshot)}
			}
			loop.saveToolState = func(_ context.Context, payload []byte) error { saved = append([]byte(nil), payload...); return nil }
			return loop
		}
		loop := newLoop()
		restoreLoop := func(snapshot []byte) {
			replacement := newLoop()
			if err := replacement.restoreToolSnapshot(snapshot); err != nil {
				t.Fatal(err)
			}
			loop = replacement
		}

		if schedule == "restart-mid-option" && !interrupted {
			w.kube.beforeWrite = append(w.kube.beforeWrite, func() {
				writes++
				if !interrupted && writes == restartWrite {
					interrupted = true
					lostReplySnapshot = append([]byte(nil), saved...)
					restart()
				}
			})
		}
		if schedule == "health-flip" && !interrupted {
			w.kube.beforeWrite = append(w.kube.beforeWrite, func() {
				if !interrupted {
					interrupted = true
					w.setPort(flipService, 9999)
				}
			})
		}
		for round := 0; round < 5; round++ {
			listing := loop.Handle(ctx, ToolRequest{Command: "menu"})
			task.sessions[listing.Menu.SessionId] = true
			wanted := "repair"
			if failAll || round < badRounds {
				wanted = "fail"
			}
			option := ""
			for _, candidate := range listing.Menu.Options {
				if candidate.ProgramId == wanted {
					option = candidate.OptionId
					break
				}
			}
			// A health flip creates another finding while the original repair remains.
			if schedule == "health-flip" && round == badRounds+1 {
				w.setPort(flipService, 8080)
			}
			response := loop.Handle(ctx, ToolRequest{Command: "mitigate", Selection: &contracts.MitigationSelection{OptionId: option, RoundLimit: 1}, FindingFingerprints: []string{FindingFingerprint(request.Findings[0])}})
			if response.Report == nil {
				t.Fatalf("missing typed report: %s", response.Error)
			}
			if len(lostReplySnapshot) > 0 {
				// The effect may have landed but its reply and latest report
				// were lost. Restore only the ordinary snapshot, never replay.
				restoreLoop(lostReplySnapshot)
				lostReplySnapshot = nil
				continue
			}
			task.reports = append(task.reports, response.Report)
			task.sessions[response.Report.SessionId] = true
			if round == 0 && response.Report.Status == contracts.MitigationReport_STATUS_SUCCESS {
				t.Fatal("failed option claimed success")
			}
			if schedule == "restart-between" && round == badRounds-1 && !interrupted {
				interrupted = true
				restart()
				restoreLoop(saved)
			}
			if schedule == "health-flip" && round == badRounds {
				if response.Report.Status != contracts.MitigationReport_STATUS_SUCCESS || !response.TargetedHealthy || response.Healthy {
					t.Fatalf("scoped repair must succeed while the peer fault remains: %+v", response)
				}
			}
			// A scoped mitigation may succeed while the injected peer is still
			// broken. The scripted worker must finish the global repair too.
			if response.Report.Status == contracts.MitigationReport_STATUS_SUCCESS && response.Healthy {
				break
			}
		}
		task.result = completedResult(request.IncidentID)
		task.result.Timing.StartedAt = w.clock.Now()
		task.result.Timing.CompletedAt = w.clock.Now()
		task.result.RepairActions = loop.MenuRepairActions()
		task.result.ProgramToolCalls = loop.ToolCalls()
		task.result.ResponseLoopsUsed = loop.LoopsUsed()
		if failAll {
			task.result.Status = IncidentFailed
			task.result.Error = "five mitigation rounds unresolved"
			task.result.ResponseLoopExhausted = true
		}
		if err := validateIncidentResultFor(task.result, request); err != nil {
			t.Fatal(err)
		}
		if len(task.sessions) != 1 || loop.LoopsUsed() > 5 {
			t.Fatal("session or round invariant failed")
		}
		close(task.done)
	}
	pump := func(failAll bool) {
		w.step()
		for effectRound := 0; effectRound < 20; effectRound++ {
			current := w.controller
			if effect, ok := current.PendingWorkspaceEffect(); ok {
				if err := current.ExecuteWorkspaceEffect(ctx, effect); err != nil {
					t.Fatal(err)
				}
				w.await(func() bool { return len(current.workspaceResults) > 0 })
			} else if effect, ok := current.PendingDispatchEffect(); ok {
				if err := current.ExecuteDispatchEffect(ctx, effect); err != nil {
					t.Fatal(err)
				}
				select {
				case task := <-driver.started:
					runTask(task, failAll)
				case <-time.After(time.Millisecond):
				}
				// A worker may first be scheduled after the short select. Await it or its
				// already-cached completion, without advancing the simulated world.
				deadline := time.Now().Add(5 * time.Second)
				for len(current.results) == 0 && time.Now().Before(deadline) {
					select {
					case task := <-driver.started:
						runTask(task, failAll)
					default:
						time.Sleep(time.Millisecond)
					}
				}
				if len(current.results) == 0 {
					t.Fatal("scripted responder did not settle")
				}
			} else if effect, ok := current.PendingClosureEffect(); ok {
				if err := current.ExecuteClosureEffect(ctx, effect); err != nil {
					t.Fatal(err)
				}
				w.await(func() bool { return len(current.closureResults) > 0 })
			} else if effect, ok := current.PendingClosureAcknowledgmentEffect(); ok {
				if err := current.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
					t.Fatal(err)
				}
				w.await(func() bool { return len(current.acknowledgmentResults) > 0 })
			} else {
				return
			}
			w.step()
		}
		t.Fatal("simulation effect pump stuck")
	}
	w.setPort(simServiceName(0), 9999)
	failAll := schedule == "five-fail-next"
	for tick := 0; tick < 30; tick++ {
		w.clock.now = w.clock.now.Add(time.Second)
		pump(failAll)
		if w.idle() || w.endedUnverified() {
			break
		}
	}
	if len(w.closed) != 1 || w.controller.IncidentOpen() {
		t.Fatal("first incident did not close exactly once")
	}
	first := w.closed[0]
	if failAll {
		if first.Result.ResponseLoopsUsed != 5 || len(first.FinalDetectorStates) != 0 {
			t.Fatal("five failed rounds did not end unresolved")
		}
		w.setPort(simServiceName(0), 8080)
		pump(false)
		restart()
		w.setPort(simServiceName(2), 9999)
		for tick := 0; tick < 30; tick++ {
			w.clock.now = w.clock.now.Add(time.Second)
			pump(false)
			if w.idle() {
				break
			}
		}
		if len(w.closed) != 2 || w.closed[1].Request.IncidentID == first.Request.IncidentID || !w.idle() {
			t.Fatal("next incident was not admitted and verified")
		}
	}
	for _, closure := range w.closed {
		ledger, err := native.readLedger(closure.Request.IncidentID)
		if err != nil {
			t.Fatal(err)
		}
		if !ledger.Acknowledged || ledger.OutcomeCommit == "" {
			t.Fatal("missing independently acknowledged broker outcome")
		}
		task := driver.tasks[closure.Request.IncidentID]
		if task == nil || len(task.sessions) != 1 || task.result.ResponseLoopsUsed > 5 {
			t.Fatal("incident session/round invariant failed")
		}
		verified := len(closure.FinalDetectorStates) > 0
		if verified && ledger.OutcomeDocument.Classification != "success" {
			t.Fatal("verified menu repair lost action evidence before broker ACK")
		}
		if !verified && ledger.OutcomeDocument.Classification != "failed" {
			t.Fatal("unverified closure received success credit")
		}
		assertMenuSimStrictReceipt(t, ctx, ledger, verified)
	}
	if (strings.HasPrefix(schedule, "restart-") || schedule == "health-flip") && !interrupted {
		t.Fatal("seeded schedule did not reach its interruption")
	}
	if len(w.violations) > 0 {
		t.Fatal(w.violations)
	}
	t.Logf("seed=%d schedule=%s outcomes=%d sessions=%d restarts=%d", seed, schedule, len(w.closed), len(driver.tasks), w.restarts)
}

func assertMenuSimStrictReceipt(t *testing.T, ctx context.Context, ledger *contracts.BrokerLedger, verified bool) {
	t.Helper()
	data, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "ledger.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "uv", "run", "python", "-c", menuStrictPython, path, filepath.Join(directory, "strict.json"), "synthetic-sim-job", "synthetic-sim-job-uid")
	command.Dir = root
	command.Env = append(os.Environ(), "PYTHONPATH="+root)
	output, err := command.CombinedOutput()
	if verified {
		if err != nil || !strings.Contains(string(output), "strict receipt PASS: sdo_mitigated") {
			t.Fatalf("strict receipt on verified simulation: %v %s", err, output)
		}
	} else if err == nil || strings.Contains(string(output), "strict receipt PASS:") {
		t.Fatal("strict receipt accepted an unverified simulation outcome")
	}

}
