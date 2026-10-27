package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"syscall"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// crashTransport persists the fake API server independently of the controller
// process. The production executor enforces retained request payload equality,
// Job/result identity and completion on both sides of a real SIGKILL.
type crashAPIState struct {
	Maps corev1.ConfigMapList
	Jobs batchv1.JobList
}
type crashTransport struct {
	t                    *testing.T
	root, statePath, cut string
	registry             *worker_registry.Registry
	worker               *tracingRefreshWorker
}

func crashAt(path, selected, point string) {
	if selected != point {
		return
	}
	if err := os.WriteFile(path+".cut", []byte(point), 0600); err != nil {
		panic(err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		panic(err)
	}
	select {}
}
func (w crashTransport) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	client := fake.NewSimpleClientset()
	if data, err := os.ReadFile(w.statePath); err == nil {
		var state crashAPIState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		for i := range state.Maps.Items {
			if err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), &state.Maps.Items[i], "demo"); err != nil {
				return nil, err
			}
		}
		for i := range state.Jobs.Items {
			if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), &state.Jobs.Items[i], "demo"); err != nil {
				return nil, err
			}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	save := func() {
		maps, err := client.Tracker().List(corev1.SchemeGroupVersion.WithResource("configmaps"), corev1.SchemeGroupVersion.WithKind("ConfigMap"), "demo")
		if err != nil {
			w.t.Fatal(err)
		}
		jobs, err := client.Tracker().List(batchv1.SchemeGroupVersion.WithResource("jobs"), batchv1.SchemeGroupVersion.WithKind("Job"), "demo")
		if err != nil {
			w.t.Fatal(err)
		}
		state := crashAPIState{Maps: *maps.(*corev1.ConfigMapList), Jobs: *jobs.(*batchv1.JobList)}
		data, err := json.Marshal(state)
		if err != nil {
			w.t.Fatal(err)
		}
		if err := os.WriteFile(w.statePath, data, 0600); err != nil {
			w.t.Fatal(err)
		}
	}
	client.PrependReactor("create", "configmaps", func(action ktesting.Action) (bool, k8sruntime.Object, error) {
		cm := action.(ktesting.CreateAction).GetObject().(*corev1.ConfigMap)
		err := client.Tracker().Create(corev1.SchemeGroupVersion.WithResource("configmaps"), cm, "demo")
		if err == nil {
			save()
			if strings.HasSuffix(cm.Name, "-request") {
				crashAt(w.statePath, w.cut, "request")
			}
		}
		return true, cm, err
	})
	executor := worker_registry.KubernetesExecutor{Client: client, Namespace: "demo", Registry: w.registry, RepositoryPVC: "repo", RepositoryMountPath: filepath.Dir(filepath.Dir(w.root)), PollInterval: time.Millisecond}
	executor.OnPhase = func(ctx context.Context, phase string) error {
		if phase != "reflection_job_created" {
			return nil
		}
		save()
		crashAt(w.statePath, w.cut, "job")
		resultName := worker_registry.JobName(task.TaskId) + "-result"
		if _, err := client.CoreV1().ConfigMaps("demo").Get(ctx, resultName, metav1.GetOptions{}); err == nil {
			return nil
		} else if !apierrors.IsNotFound(err) {
			return err
		}
		result, err := w.worker.Execute(ctx, task)
		if err != nil {
			return err
		}
		data, err := wire.Canonical(result)
		if err != nil {
			return err
		}
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: resultName, Namespace: "demo"}, Data: map[string]string{worker_registry.ResultDataKey: string(data)}}
		if _, err := client.CoreV1().ConfigMaps("demo").Create(ctx, cm, metav1.CreateOptions{}); err != nil {
			return err
		}
		job, err := client.BatchV1().Jobs("demo").Get(ctx, worker_registry.JobName(task.TaskId), metav1.GetOptions{})
		if err != nil {
			return err
		}
		job.Status.Succeeded = 1
		if _, err := client.BatchV1().Jobs("demo").UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
			return err
		}
		save()
		crashAt(w.statePath, w.cut, "result")
		return nil
	}
	return executor.Execute(ctx, task)
}

type crashProposalBroker struct {
	lifecycle.ProposalBroker
	path, cut string
}

func (b crashProposalBroker) CreateProposalForPhase(ctx context.Context, id string, phase broker.Phase) (broker.Proposal, error) {
	proposal, err := b.ProposalBroker.CreateProposalForPhase(ctx, id, phase)
	if err == nil {
		crashAt(b.path, b.cut, "proposal")
	}
	return proposal, err
}
func (b crashProposalBroker) Publish(ctx context.Context, id string, actor broker.Actor, phase broker.Phase) (broker.Receipt, error) {
	crashAt(b.path, b.cut, "before-accept")
	receipt, err := b.ProposalBroker.Publish(ctx, id, actor, phase)
	if err == nil {
		crashAt(b.path, b.cut, "after-accept")
	}
	return receipt, err
}
func crashCoordinator(t *testing.T, root, proposals, state, cut string) (lifecycle.Coordinator, *tracingRefreshWorker) {
	t.Helper()
	repository, err := broker.New(root, proposals, func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	registry := registrytest.BuiltIn(t)
	worker := &tracingRefreshWorker{t: t, registry: registry}
	return lifecycle.Coordinator{Broker: crashProposalBroker{ProposalBroker: repository, path: state, cut: cut}, Bootstrap: artifacts.BootstrapStore{}, Registry: registry, Workers: crashTransport{t: t, root: root, statePath: state, cut: cut, registry: registry, worker: worker}}, worker
}

// crashStateStore retains the controller's actual durable contract across child
// processes. Cuts bracket the Save that makes obsolete evidence terminal.
type crashStateStore struct {
	path string
}

func (s crashStateStore) Load(context.Context) (*contracts.ControllerState, string, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	state := &contracts.ControllerState{}
	if err := wire.UnmarshalDurable(data, state); err != nil {
		return nil, "", err
	}
	return state, fmt.Sprintf("%x", sha256.Sum256(data)), nil
}
func (s crashStateStore) Save(ctx context.Context, state *contracts.ControllerState, revision string) (string, error) {
	_, current, err := s.Load(ctx)
	if err != nil {
		return "", err
	}
	if current != revision {
		return "", ErrStateConflict
	}
	data, err := wire.Canonical(state)
	if err != nil {
		return "", err
	}
	if err := os.WriteFile(s.path+".tmp", data, 0600); err != nil {
		return "", err
	}
	if err := os.Rename(s.path+".tmp", s.path); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func crashController(t *testing.T, root, compiled, state, cut string) *Controller {
	t.Helper()
	cfg := testControllerConfig()
	cfg.SourceCommit = compiled
	detector := controllerDetector("health", time.Second, sdk.Finding{})
	detector.spec.Class, detector.spec.Owner = sdk.DetectorClassHealth, sdk.DetectorOwnerHealthJudge
	detector.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
	detector.spec.Batching = sdk.BatchingPolicy{Severity: sdk.SeverityCritical}
	detector.spec.OriginatingCommit = compiled
	c, err := NewController(cfg, []sdk.Detector{detector}, staticProvider{}, &recordingDispatcher{}, time.Unix(1000, 0))
	if err != nil {
		t.Fatal(err)
	}
	rv6Guard(c, root, compiled)
	if err := c.AttachStateStore(context.Background(), crashStateStore{path: state + ".controller"}); err != nil {
		t.Fatal(err)
	}
	c.discardRestoredHealthObservations(time.Unix(1000, 0))
	return c
}

func TestSR2dCrashProcess(t *testing.T) {
	root := os.Getenv("SR2D_CRASH_ROOT")
	if root == "" {
		t.Skip("subprocess only")
	}
	state, cut := os.Getenv("SR2D_CRASH_STATE"), os.Getenv("SR2D_CRASH_CUT")
	c, _ := crashCoordinator(t, root, os.Getenv("SR2D_CRASH_PROPOSALS"), state, cut)
	revision, err := memory.AcceptedHealthRevision(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := c.RefreshHealth(context.Background(), lifecycle.Config{Application: "tracing", Namespace: "demo"}, revision)
	if err != nil {
		t.Fatal(err)
	}
	crashAt(state, cut, "before-adoption")
	controller := crashController(t, root, receipt.Commit, state, cut)
	crashAt(state, cut, "after-adoption")
	if err := executeStartupEffects(context.Background(), controller, true); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash boundary was not reached")
}

func TestSimRV6HealthRefreshCrashSchedules(t *testing.T) {
	cuts := []string{"proposal", "request", "job", "result", "before-accept", "after-accept", "before-adoption", "after-adoption"}
	// The accepted baseline is immutable; each schedule gets a separate clone
	// and proposal/API/state stores. Avoid republishing it twenty times.
	baselineRoot, _, _, _, baseline := tracingFixture(t)
	old := tracingRefresh(t, baselineRoot, baseline)
	covered := map[string]bool{}
	executed := 0
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			executed++
			random := rand.New(rand.NewSource(seed))
			cut := cuts[(seed-1)%int64(len(cuts))]
			root := filepath.Join(t.TempDir(), "accepted")
			testutil.CloneGit(t, baselineRoot, root, "--shared")
			nativeGit(t, root, "config", "user.name", "Test")
			nativeGit(t, root, "config", "user.email", "test@example.com")
			proposals := filepath.Join(t.TempDir(), "proposals")
			if random.Intn(2) == 0 {
				tracingSwap(t, root)
			} else {
				tracingWrite(t, root, "README.md", "source documentation change\n")
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "source docs")
			}
			state := filepath.Join(filepath.Dir(root), "api.json")
			now := time.Unix(1000, 0)
			cfg := testControllerConfig()
			cfg.SourceCommit = old.Commit
			controller, err := NewController(cfg, nil, staticProvider{}, &recordingDispatcher{}, now)
			if err != nil {
				t.Fatal(err)
			}
			request := controller.incidentRequest(now, []sdk.Finding{stateFinding("fault")})
			request.SourceCommit = old.Commit
			request.DetectorHistory = testDetectorHistory(now)
			switch seed % 3 {
			case 0:
				controller.incidentOpen = true
				controller.currentIncidentRequest = &request
			default:
				controller.pendingClosure = &IncidentClosure{Request: request, Result: cloneIncidentResultPointer(completedResult(request.IncidentID)), FinalDetectorStates: []DetectorEvaluation{{DetectorID: "health", Status: DetectorEvaluationClear}}, DetectedAt: now, DispatchedAt: now, ResponderCompletedAt: now, VerifiedAt: now}
				controller.closureState = contracts.ClosureState_CLOSURE_STATE_PROCESSING
				if seed%3 == 2 {
					controller.closureState = contracts.ClosureState_CLOSURE_STATE_COMMITTED
					controller.closureReceipt = &ClosureReceipt{IncidentID: request.IncidentID, OutcomeCommit: old.Commit, AckToken: "old-token"}
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			// A returned error after the finite parent budget is a permitted
			// terminal outcome. It must never be mistaken for healthy success.
			timedOut := func(err error) bool {
				if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Logf("seed=%d cut=%s terminal=bounded_timeout error=%v", seed, cut, err)
					return true
				}
				return false
			}
			store := crashStateStore{path: state + ".controller"}
			if _, err := store.Save(ctx, exportState(t, controller), ""); err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSR2dCrashProcess$", "-test.timeout=60s")
			command.Env = append(os.Environ(), "SR2D_CRASH_ROOT="+root, "SR2D_CRASH_PROPOSALS="+proposals, "SR2D_CRASH_STATE="+state, "SR2D_CRASH_CUT="+cut)
			output, err := command.CombinedOutput()
			if timedOut(err) {
				return
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) {
				t.Fatalf("child did not die at %s: %v %s", cut, err, output)
			}
			status, ok := exit.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("expected SIGKILL at %s: %v %s", cut, err, output)
			}
			if data, err := os.ReadFile(state + ".cut"); err != nil || string(data) != cut {
				t.Fatalf("wrong crash boundary: %s %v", data, err)
			}
			covered[cut] = true
			if err := acceptedHealthGuard(root, old.Commit)(ctx); !errors.Is(err, memory.ErrHealthRefreshRequired) {
				if timedOut(err) {
					return
				}
				t.Fatalf("old binary allowed after kill: %v", err)
			}
			var retained crashAPIState
			if data, err := os.ReadFile(state); err == nil {
				if err := json.Unmarshal(data, &retained); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			memoryAdvanced := random.Intn(2) == 0
			if memoryAdvanced {
				tracingMemoryAdvance(t, root)
				nativeGit(t, root, "add", ".")
				nativeGit(t, root, "commit", "-qm", "memory while interrupted")
			}
			c, worker := crashCoordinator(t, root, proposals, state, "")
			revision, err := memory.AcceptedHealthRevision(ctx, root)
			if err != nil {
				if timedOut(err) {
					return
				}
				t.Fatal(err)
			}
			receipt, err := c.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, revision)
			if err != nil {
				if timedOut(err) {
					return
				}
				t.Fatalf("restart failed at seed %d/%s: %v", seed, cut, err)
			}
			if err := acceptedHealthGuard(root, receipt.Commit)(ctx); err != nil {
				if timedOut(err) {
					return
				}
				t.Fatalf("fresh adoption: %v", err)
			}
			var resumed crashAPIState
			if data, err := os.ReadFile(state); err == nil {
				if err := json.Unmarshal(data, &resumed); err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			for _, before := range retained.Maps.Items {
				found := false
				for _, after := range resumed.Maps.Items {
					if before.Name == after.Name {
						found = true
						if !reflect.DeepEqual(before.Data, after.Data) {
							t.Fatalf("retained payload changed: %s", before.Name)
						}
					}
				}
				if !found {
					t.Fatalf("retained request/result lost: %s", before.Name)
				}
			}
			if cut == "result" && !memoryAdvanced && worker.calls != 0 {
				t.Fatal("retained completed result dispatched judge again")
			}
			calls := worker.calls
			current, err := memory.AcceptedHealthRevision(ctx, root)
			if err != nil || current.NeedsRefresh {
				if timedOut(err) {
					return
				}
				t.Fatalf("not verified after bounded restart: %+v %v", current, err)
			}
			if _, err := c.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, current); err != nil || calls != worker.calls {
				if timedOut(err) {
					return
				}
				t.Fatalf("accepted work repeated: %v", err)
			}
			// Recover using only state that survived the killed process. The
			// adopted controller must persist a visible failure before effects.
			restarted := crashController(t, root, current.Commit, state, "")
			b := &recordingIncidentBroker{}
			if err := restarted.SetIncidentBroker(b); err != nil {
				t.Fatal(err)
			}
			if effect, ok := restarted.PendingClosureEffect(); ok {
				if err := restarted.ExecuteClosureEffect(ctx, effect); err != nil {
					if timedOut(err) {
						return
					}
					t.Fatalf("adopted controller refused its pending closure: %v", err)
				}
			}
			if effect, ok := restarted.PendingClosureAcknowledgmentEffect(); ok {
				if err := restarted.ExecuteClosureAcknowledgmentEffect(ctx, effect); err != nil {
					if timedOut(err) {
						return
					}
					t.Fatalf("adopted controller refused its acknowledgment: %v", err)
				}
			}
			evaluations := 0
			restarted.OnEvaluation = func([]sdk.Finding) { evaluations++ }
			if err := executeStartupEffects(ctx, restarted, true); err != nil {
				if timedOut(err) {
					return
				}
				t.Fatalf("paused startup blocked recovery: %v", err)
			}
			if evaluations != 0 {
				t.Fatal("paused recovery evaluated application detectors")
			}
			persisted, _, err := store.Load(ctx)
			if err != nil || len(persisted.GetFailedClosures()) != 0 {
				t.Fatalf("adoption retired a closure it must resume: %v %v", persisted, err)
			}
			t.Logf("seed=%d cut=%s verified=%s closure_resumed", seed, cut, current.Commit)
		})
	}
	// A filtered -run subtest selection is a replay, not a coverage sweep.
	if executed != 20 {
		return
	}
	for _, cut := range cuts {
		if !covered[cut] {
			t.Errorf("uncovered cut %s", cut)
		}
	}
}
