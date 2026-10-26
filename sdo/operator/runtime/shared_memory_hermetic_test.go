package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/validate"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/sdktest"
)

type sharedMemoryResponder struct {
	health *chaosDetector
	lane   string
	start  time.Time
}

func (d sharedMemoryResponder) Dispatch(_ context.Context, r IncidentRequest) (IncidentResult, error) {
	if err := os.WriteFile(filepath.Join(r.RepositoryWorktree, d.lane+".txt"), []byte("synthetic accepted source repair\n"), 0644); err != nil {
		return IncidentResult{}, err
	}
	d.health.active.set(false)
	result := completedResult(r.IncidentID)
	result.ResponderSessionID = "session-" + d.lane
	result.Timing = TimingMetrics{StartedAt: d.start.Add(time.Second), CompletedAt: d.start.Add(2 * time.Second)}
	result.RepairActions = []RepairActionReceipt{{ActionID: d.lane + "-repair", Kind: "source", Target: d.lane + ".txt", Summary: "synthetic repair", Details: "scripted source repair", Success: true, StartedAt: result.Timing.StartedAt, CompletedAt: result.Timing.CompletedAt}}
	return result, nil
}

// The cluster edge is synthetic; controller detection, workspaces, registered
// reflection tasks, independent compilation, native ledgers and Git are real.
func TestHermeticSharedMemoryControllers(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("same-playbook=%t", conflict), func(t *testing.T) {
			// Conflict modes own separate repositories and controller/process state.
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			root := nativeRepository(t)
			if err := os.CopyFS(root, os.DirFS("../memory/validate/testdata/good")); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(root, ".sdo/playbooks/base/scripts/fix.sh"), 0755); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"alpha", "beta"} {
				path := filepath.Join(root, ".sdo/playbooks", id, "README.md")
				if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("---\nschema_version: 1\nowner: responder\nfault_class: "+id+"\noriginating_incident: synthetic\n---\nOriginal <RESOURCE_NAME> learning\n"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(root, ".sdo/playbooks/README.md"), []byte("# Playbooks\n\n- [base](base/README.md)\n- [alpha](alpha/README.md)\n- [beta](beta/README.md)\n"), 0644); err != nil {
				t.Fatal(err)
			}
			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "commit", "-qm", "synthetic accepted memory")
			base := nativeGit(t, root, "rev-parse", "HEAD")
			operatorRoot, err := filepath.Abs("..")
			if err != nil {
				t.Fatal(err)
			}
			var compiles atomic.Int32
			compile := func(ctx context.Context, stage string, _ []string) error {
				compiles.Add(1)
				ds, err := validate.CompileCandidate(ctx, validate.Request{Root: stage, OperatorRoot: operatorRoot})
				if err != nil {
					return err
				}
				return validate.Err(ds)
			}
			started := make(chan int, 2)
			releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
			controllers := make([]*Controller, 2)
			natives := make([]NativeIncidentBroker, 2)
			calls := make([]atomic.Int32, 2)
			incidents := make([]string, 2)
			for lane := 0; lane < 2; lane++ {
				name := fmt.Sprintf("lane-%d", lane)
				authority, err := broker.New(root, t.TempDir(), compile)
				if err != nil {
					t.Fatal(err)
				}
				executor := reflectionTaskExecutorFunc(func(_ context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
					attempt := calls[lane].Add(1)
					request := new(contracts.ReflectionRequest)
					if err := task.Request.UnmarshalTo(request); err != nil {
						return nil, err
					}
					if request.GetMemorySnapshot().GetAcceptedCommit() != task.GetWorkspace().GetBaseCommit() {
						return nil, fmt.Errorf("snapshot not bound to proposal")
					}
					if request.GetSessionMode() != "resume" || request.GetClosure().GetResult().GetResponderSessionId() != "session-"+name {
						return nil, fmt.Errorf("retry lost responder session")
					}
					book := []string{"alpha", "beta"}[lane]
					if conflict {
						book = "base"
					}
					path := filepath.Join(task.Workspace.CheckoutPath, ".sdo/playbooks", book, "README.md")
					data, err := os.ReadFile(path)
					if err != nil {
						return nil, err
					}
					if attempt == 1 {
						// Both edit the same original line to force a real three-way conflict.
						if conflict {
							data = []byte(strings.ReplaceAll(string(data), "Repair <RESOURCE_NAME>. Run fix.sh to restart it.", "Repair <RESOURCE_NAME> with "+name+" intent. Run fix.sh to restart it."))
						} else {
							data = append(data, []byte(name+" learning\n")...)
						}
						if err := os.WriteFile(path, data, 0644); err != nil {
							return nil, err
						}
						started <- lane
						select {
						case <-releases[lane]:
						case <-ctx.Done():
							return nil, ctx.Err()
						}
					} else {
						if !conflict || lane != 1 || attempt != 2 || len(request.GetPreviousDiagnostics()) != 1 {
							return nil, fmt.Errorf("unexpected retry %d lane %d: %v", attempt, lane, request.GetPreviousDiagnostics())
						}
						detail := request.GetPreviousDiagnostics()[0].GetMemoryConflict()
						if detail == nil || len(detail.GetAcceptedChanges()) == 0 || detail.GetAcceptedChanges()[0].GetIncidentId() != incidents[0] {
							return nil, fmt.Errorf("typed conflict lost accepted incident provenance")
						}
						if !strings.Contains(string(data), "lane-0 intent") {
							return nil, fmt.Errorf("retry lost peer learning")
						}
						if err := os.WriteFile(path, append(data, []byte("Preserve lane-1 intent for <RESOURCE_NAME>.\n")...), 0644); err != nil {
							return nil, err
						}
					}
					response, err := anypb.New(&contracts.ReflectionResult{LearningDecision: "updated", ProposedArtifactIds: []string{book}})
					return &contracts.AgentResult{SchemaVersion: task.SchemaVersion, TaskId: task.TaskId, ProposalId: task.Workspace.ProposalId, Status: "completed", Response: response}, err
				})
				worker := RegisteredReflectionWorker{Executor: executor, AgentID: "reflector", TaskType: reflectionTaskType(t), Outcomes: memory.OpenReader(root), Catalog: memory.OpenCatalogReader(root), Timeout: time.Minute, SessionMode: "resume", SessionExecutor: func(string) (ReflectionTaskExecutor, error) { return executor, nil }}
				native := NativeIncidentBroker{Repository: authority, LedgerRoot: t.TempDir(), Deriver: ProtoOutcomeDeriver{}, Reflector: worker, ResponderBackend: "scripted", ResponderModel: "none", ForceReflection: true}
				natives[lane] = native
				at := time.Unix(1000+int64(lane)*100, 0)
				now := at
				health := newChaosHealthDetector("health")
				health.spec.Persistence = sdk.PersistencePolicy{Firing: 1, Clearing: 1}
				health.active.set(true)
				cfg := testControllerConfig()
				cfg.SourceCommit, cfg.DeployedCommit = base, base
				cfg.RepositoryWorktree = root
				cfg.BatchDebounce = 0
				cfg.ClearThreshold = 1
				cfg.FiringThreshold = 1
				c, err := NewController(cfg, []sdk.Detector{health}, staticProvider{snapshot: sdktest.Snapshot{NamespaceName: "demo"}}, sharedMemoryResponder{health: health, lane: name, start: at}, at)
				if err != nil {
					t.Fatal(err)
				}
				c.now = func() time.Time { return now }
				if err := c.SetIncidentBroker(native); err != nil {
					t.Fatal(err)
				}
				controllers[lane] = c
				// Exercise actual detection and workspace/dispatch effects, then stop at
				// the durable closure boundary so both incidents overlap in reflection.
				for tick := 0; tick < 12; tick++ {
					now = at.Add(time.Duration(tick) * time.Second)
					if err := c.Step(ctx, now, nil); err != nil {
						t.Fatal(err)
					}
					if effect, ok := c.PendingWorkspaceEffect(); ok {
						if err := c.ExecuteWorkspaceEffect(ctx, effect); err != nil {
							t.Fatal(err)
						}
						fastloopGateWait(t, 10*time.Second, func() bool { return len(c.workspaceResults) > 0 })
						c.processBrokerCompletions()
					}
					if effect, ok := c.PendingDispatchEffect(); ok {
						if err := c.ExecuteDispatchEffect(ctx, effect); err != nil {
							t.Fatal(err)
						}
						awaitDispatchCompletionQueued(t, c)
					}
					if _, ok := c.PendingClosureEffect(); ok {
						break
					}
				}
				effect, ok := c.PendingClosureEffect()
				if !ok {
					t.Fatal("controller never reached independently verified closure")
				}
				incidents[lane] = effect.Closure.Request.IncidentID
				if err := c.ExecuteClosureEffect(ctx, effect); err != nil {
					t.Fatal(err)
				}
				select {
				case got := <-started:
					if got != lane {
						t.Fatalf("unexpected started lane %d", got)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			close(releases[0])
			fastloopGateWait(t, 30*time.Second, func() bool { return len(controllers[0].closureResults) > 0 })
			controllers[0].processBrokerCompletions()
			if controllers[0].closureReceipt == nil {
				t.Fatalf("first closure failed: %+v", controllers[0].closureFailure)
			}
			close(releases[1])
			fastloopGateWait(t, 30*time.Second, func() bool { return len(controllers[1].closureResults) > 0 })
			controllers[1].processBrokerCompletions()
			if controllers[1].closureReceipt == nil {
				t.Fatalf("second closure failed: %+v", controllers[1].closureFailure)
			}
			for lane, c := range controllers {
				if c.closureReceipt.ReflectionCommit == "" {
					t.Fatal("reflection learning was not accepted")
				}
				ledger, err := natives[lane].readLedger(incidents[lane])
				if err != nil {
					t.Fatal(err)
				}
				want := int32(1)
				if conflict && lane == 1 {
					want = 2
				}
				if calls[lane].Load() != want || ledger.GetReflectionRejected() {
					t.Fatalf("lane %d calls=%d ledger=%v", lane, calls[lane].Load(), ledger)
				}
				nativeGit(t, root, "merge-base", "--is-ancestor", c.closureReceipt.ReflectionCommit, "HEAD")
			}
			for lane := 0; lane < 2; lane++ {
				book := []string{"alpha", "beta"}[lane]
				needle := fmt.Sprintf("lane-%d learning", lane)
				if conflict {
					book = "base"
					needle = fmt.Sprintf("lane-%d intent", lane)
				}
				data, err := os.ReadFile(filepath.Join(root, ".sdo/playbooks", book, "README.md"))
				if err != nil || !strings.Contains(string(data), needle) {
					t.Fatalf("lost accepted learning %q: %s %v", needle, data, err)
				}
			}
			if compiles.Load() < 4 {
				t.Fatal("independent compiler was bypassed")
			}
		})
	}
}

func TestIncidentDispatchSnapshotPinsBaseAfterPeerPublication(t *testing.T) {
	ctx := context.Background()
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	authority, err := broker.New(root, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := authority.CreateProposal(ctx, "snapshot-race")
	if err != nil {
		t.Fatal(err)
	}
	// The peer moves accepted HEAD after preparation but before dispatch.
	if _, err := authority.RecordOutcome(ctx, "peer", &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: "peer", Classification: "failed"}); err != nil {
		t.Fatal(err)
	}
	request := goldenRequest(t)
	request.RepositoryWorktree, request.RepositoryBaseCommit = proposal.Path, proposal.BaseCommit
	dispatcher := KubernetesJobDispatcher{Registry: testIncidentRegistry(t, "responder:test"), AgentID: testIncidentAgentID(t), Catalog: memory.OpenCatalogReader(root), RequireMemorySnapshot: true}
	prepared, err := dispatcher.PrepareDispatch(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	task := new(contracts.AgentTask)
	if err := wire.Unmarshal([]byte(prepared.PreparedAgentTask), task); err != nil {
		t.Fatal(err)
	}
	typed := new(contracts.IncidentRequest)
	if err := task.GetRequest().UnmarshalTo(typed); err != nil {
		t.Fatal(err)
	}
	if typed.GetMemorySnapshot().GetAcceptedCommit() != proposal.BaseCommit {
		t.Fatal("dispatch snapshot advanced to peer publication")
	}
}

func TestNativeSharedMemoryConflictsTerminateWithLastDiagnostic(t *testing.T) {
	ctx := context.Background()
	var h *rejectionHarness
	var peer *broker.Broker
	var accepted broker.Receipt
	h = newRejectionHarness(t, "conflict-loser", func(call int, w IncidentWorkspace) (ReflectionDecision, error) {
		decision, err := writeGoodPlaybook(w)
		if err != nil {
			return decision, err
		}
		id := fmt.Sprintf("peer-%d", call)
		if _, err := peer.RecordOutcome(ctx, id, &contracts.OutcomeRecord{SchemaVersion: 1, IncidentId: id, Classification: "success"}); err != nil {
			return decision, err
		}
		proposal, err := peer.CreateProposalForPhase(ctx, id, broker.ReflectionPhase)
		if err != nil {
			return decision, err
		}
		body := fmt.Sprintf("---\nschema_version: 1\nowner: responder\nfault_class: base\noriginating_incident: synthetic\n---\nPeer learning for <RESOURCE_NAME> revision %d\n", call)
		if err := os.WriteFile(filepath.Join(proposal.Path, ".sdo/playbooks/base/README.md"), []byte(body), 0644); err != nil {
			return decision, err
		}
		accepted, err = peer.PublishReflection(ctx, id, &contracts.ReflectionDecision{LearningDecision: "updated", PlaybookIds: []string{"base"}})
		return decision, err
	})
	var err error
	peer, err = broker.New(h.target, t.TempDir(), func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := h.native.ProcessClosure(ctx, h.closure)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := h.native.readLedger("conflict-loser")
	if err != nil {
		t.Fatal(err)
	}
	if len(h.calls) != 3 || !ledger.GetReflectionRejected() || !ledger.GetReflectionProcessed() || receipt.ReflectionCommit != "" {
		t.Fatalf("conflict did not fail visibly at bound: %v", ledger)
	}
	last := ledger.GetReflectionRejections()[2].GetDiagnostics()[0].GetMemoryConflict()
	if last.GetAcceptedCommit() != accepted.Commit || last.GetAcceptedChanges()[0].GetIncidentId() != "peer-3" {
		t.Fatalf("terminal conflict diagnostic lost: %v", last)
	}
	if nativeGit(t, h.target, "rev-parse", "HEAD") != accepted.Commit {
		t.Fatal("rejected learning reached accepted state")
	}
	// Receipt replay cannot resume a terminal losing reflection after restart.
	restored := h.native
	restored.Reflector = ReflectionWorkerFunc(func(context.Context, IncidentClosure, IncidentWorkspace, *contracts.OutcomeRecord, string, string, ReflectionFeedback) (ReflectionDecision, error) {
		return ReflectionDecision{}, fmt.Errorf("terminal reflection redispatched")
	})
	again, err := restored.ProcessClosure(ctx, h.closure)
	if err != nil || again != receipt {
		t.Fatalf("terminal conflict not durable across restart: %v", err)
	}
}
