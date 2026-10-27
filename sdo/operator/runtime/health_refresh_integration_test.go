package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/anypb"
	"sdo.dev/operator/artifacts"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/worker_registry"
	"sdo.dev/operator/worker_registry/registrytest"
)

// Reduced from SP1/AF1 stage 2: accepted source replaces Jaeger with a collector,
// while architecture and learned memory still describe Jaeger. Only a scripted
// judge runs; detector compilation, publication and provenance are real.
type tracingRefreshWorker struct {
	t         *testing.T
	registry  *worker_registry.Registry
	calls     int
	interrupt bool
}

func (w *tracingRefreshWorker) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	w.calls++
	if w.interrupt {
		w.interrupt = false
		return nil, context.Canceled
	}
	request := &contracts.HealthJudgeRequest{}
	if err := task.GetRequest().UnmarshalTo(request); err != nil {
		return nil, err
	}
	if request.GetArchitectureSummaryMarkdown() == "" || request.GetHealthObjective() == "" {
		return nil, errors.New("missing judge context")
	}
	names := []string{}
	for _, resource := range request.GetSourceResources() {
		if resource.GetKind() == "Deployment" {
			names = append(names, fmt.Sprintf("%q", resource.GetName()))
		}
	}
	if len(names) != 1 {
		return nil, fmt.Errorf("expected current tracing inventory: %v", names)
	}
	base := nativePathWorkers{t: w.t, registry: w.registry}
	result, err := base.Execute(ctx, task)
	if err != nil {
		return nil, err
	}
	tracingWrite(w.t, task.GetWorkspace().GetCheckoutPath(), ".sdo/playbooks/README.md", "# Playbooks\n\n- [base](base/README.md)\n- [health-objective](health-objective/README.md)\n")
	source := fmt.Sprintf(`package objective
import("context";"time";"sdo.dev/operator/sdk";"sdo.dev/operator/sdk/servicehealth")
type detector struct{}
func New() sdk.Detector { return detector{} }
func (detector) Spec() sdk.DetectorSpec {return sdk.DetectorSpec{ID:"health-objective",Class:sdk.DetectorClassHealth,Owner:sdk.DetectorOwnerHealthJudge,Watches:servicehealth.EndpointWatches(),Interval:30*time.Second,Persistence:sdk.PersistencePolicy{Firing:2,Clearing:2},Batching:sdk.BatchingPolicy{Severity:sdk.SeverityCritical,Debounce:500*time.Millisecond},Playbooks:[]string{"health-objective"},OriginatingCommit:"lifecycle-bootstrap"}}
func(detector) Detect(_ context.Context, ctx sdk.DetectionContext)([]sdk.Finding,error){
 for _,name:=range []string{%s} {found:=false;for _,d:=range ctx.Deployments(){if d.Name==name {found=true}}
 if !found{return []sdk.Finding{{DetectorID:"health-objective",RuleID:"deployment-missing",Status:sdk.FindingActive,Severity:sdk.SeverityCritical,PrimaryResource:sdk.ObjectRef{APIVersion:"apps/v1",Kind:"Deployment",Namespace:ctx.Namespace(),Name:name},Summary:"required tracing workload absent"}},nil}}
 return nil,nil
}
`, strings.Join(names, ","))
	path := filepath.Join(task.GetWorkspace().GetCheckoutPath(), ".sdo/diagnostics/detectors/health/health-objective/detector.go")
	if err := os.WriteFile(path, formattedNativePathSource(w.t, source), 0644); err != nil {
		return nil, err
	}
	response, _ := anypb.New(&contracts.HealthJudgeResult{Authored: true, AgentSessionId: "scripted-judge", ObjectiveDigest: fmt.Sprintf("%x", sha256.Sum256([]byte(request.GetHealthObjective()))), DetectorIds: []string{"health-objective"}})
	result.Response = response
	return result, nil
}
func tracingWrite(t *testing.T, root, path, content string) {
	t.Helper()
	target := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}
func tracingFixture(t *testing.T) (string, string, *broker.Broker, *tracingRefreshWorker, lifecycle.Coordinator) {
	t.Helper()
	root := nativeRepository(t)
	nativeMemoryFixture(t, root)
	if err := os.RemoveAll(filepath.Join(root, ".sdo/diagnostics/detectors/health")); err != nil {
		t.Fatal(err)
	}
	if err := memory.WriteSchema(root); err != nil {
		t.Fatal(err)
	}
	if err := memory.WriteGoal(root, "tracing", "Keep intended tracing healthy."); err != nil {
		t.Fatal(err)
	}
	tracingWrite(t, root, ".sdo/arch.md", "---\nschema_version: 1\napplication: tracing\n---\n# Architecture\nJaeger receives tracing.\n")
	tracingWrite(t, root, "deploy/tracing.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: jaeger\nspec: {}\n")
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "tracing seed")
	proposals := filepath.Join(t.TempDir(), "proposals")
	repository, err := broker.New(root, proposals, func(context.Context, string, []string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	registry, err := worker_registry.LoadDirectory(registrytest.Directory())
	if err != nil {
		t.Fatal(err)
	}
	worker := &tracingRefreshWorker{t: t, registry: registry}
	coordinator := lifecycle.Coordinator{Broker: repository, Bootstrap: artifacts.BootstrapStore{}, Registry: registry, Workers: worker}
	return root, proposals, repository, worker, coordinator
}
func tracingRefresh(t *testing.T, root string, c lifecycle.Coordinator) memory.HealthRevision {
	t.Helper()
	state, err := memory.AcceptedHealthRevision(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.RefreshHealth(context.Background(), lifecycle.Config{Application: "tracing", Namespace: "demo"}, state); err != nil {
		t.Fatal(err)
	}
	accepted, err := memory.AcceptedHealthRevision(context.Background(), root)
	if err != nil || accepted.NeedsRefresh {
		t.Fatalf("health not accepted: %+v %v", accepted, err)
	}
	return accepted
}
func tracingSwap(t *testing.T, root string) {
	t.Helper()
	tracingWrite(t, root, "deploy/tracing.yaml", "apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: collector\nspec: {}\n")
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "developer replaces Jaeger with collector")
}
func TestTracingSwapRefreshPublishesAndBuildsCurrentRequirements(t *testing.T) {
	ctx := context.Background()
	root, _, _, worker, coordinator := tracingFixture(t)
	old := tracingRefresh(t, root, coordinator)
	tracingProbe(t, root, "jaeger", false)
	tracingSwap(t, root)
	guard := acceptedHealthGuard(root, old.Commit)
	if err := guard(ctx); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("warm source advance allowed: %v", err)
	}
	tracingProbe(t, root, "collector", true) // old detector reproduces the false Jaeger alarm
	architecture := nativeGit(t, root, "rev-parse", "HEAD:.sdo/arch.md")
	current := tracingRefresh(t, root, coordinator)
	if err := guard(ctx); !errors.Is(err, memory.ErrHealthRefreshRequired) {
		t.Fatalf("old binary adopted new policy: %v", err)
	}
	if err := acceptedHealthGuard(root, current.Commit)(ctx); err != nil {
		t.Fatalf("new binary did not adopt policy: %v", err)
	}
	tracingProbe(t, root, "collector", false)
	tracingProbe(t, root, "jaeger", true) // collector coverage still required
	if nativeGit(t, root, "rev-parse", "HEAD:.sdo/arch.md") != architecture {
		t.Fatal("refresh changed architecture ownership")
	}
	if worker.calls != 2 {
		t.Fatalf("refresh performed extra worker turns: %d", worker.calls)
	}
	tracingWrite(t, root, ".sdo/outcomes/memory.json", "{}")
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "memory-only")
	tracingRefresh(t, root, coordinator)
	if worker.calls != 2 {
		t.Fatal("memory-only commit dispatched judge")
	}
}

// Compile the accepted detector bundle and execute its generated registration
// against an in-memory capture. No Kubernetes client or model backend is used.
func tracingProbe(t *testing.T, root, live string, wantFiring bool) {
	t.Helper()
	ctx := context.Background()
	projected, err := memory.AcceptedDiagnostics(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := builder.BuildWorkspace(ctx, builder.Config{ApplicationRoot: root, DiagnosticsRoot: projected.Directory, OperatorRoot: ".."})
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Close()
	path := filepath.Join(workspace.Directory, "generated", "tracing_refresh_test.go")
	source := fmt.Sprintf(`package generated
import("context";"testing";appsv1 "k8s.io/api/apps/v1";metav1 "k8s.io/apimachinery/pkg/apis/meta/v1";"sdo.dev/operator/sdk/sdktest")
func TestTracingCapture(t *testing.T){snapshot:=sdktest.Snapshot{NamespaceName:"demo",DeploymentList:[]appsv1.Deployment{{ObjectMeta:metav1.ObjectMeta{Name:%q,Namespace:"demo"}}}}
 count:=0; for _,d:=range All(){if d.Spec().ID!="health-objective"{continue};count++;findings,err:=d.Detect(context.Background(),snapshot);if err!=nil || (len(findings)>0)!=%t {t.Fatalf("unexpected tracing health: %%+v %%v",findings,err)}};if count!=1 {t.Fatalf("executed %%d health-objective detectors, want 1",count)}}
`, live, wantFiring)
	if err := os.WriteFile(path, []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := workspace.ValidateAndBuild(ctx); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, "go", "test", "./generated", "-run", "TestTracingCapture", "-count=1")
	cmd.Dir = workspace.Directory
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("compiled tracing capture: %v: %s", err, out)
	}
}
func TestSimHealthRefreshInterruptedAndAdopted(t *testing.T) {
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			root, proposals, _, worker, c := tracingFixture(t)
			old := tracingRefresh(t, root, c)
			tracingSwap(t, root)
			state, err := memory.AcceptedHealthRevision(ctx, root)
			if err != nil {
				t.Fatal(err)
			}
			interruptedBefore := rand.New(rand.NewSource(seed)).Intn(2) == 0
			if interruptedBefore {
				worker.interrupt = true
				if _, err := c.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, state); err == nil {
					t.Fatal("interrupted refresh succeeded")
				}
				if err := acceptedHealthGuard(root, old.Commit)(ctx); !errors.Is(err, memory.ErrHealthRefreshRequired) {
					t.Fatalf("failure authorized stale policy: %v", err)
				}
			} else {
				tracingRefresh(t, root, c)
			}
			// Reopen durable broker as a restarted controller/builder would. A committed
			// refresh must be reused even if the previous process never adopted it.
			repository, err := broker.New(root, proposals, func(context.Context, string, []string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			c.Broker = repository
			before := worker.calls
			receipt, err := c.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, state)
			if err != nil {
				t.Fatal(err)
			}
			if !interruptedBefore && worker.calls != before {
				t.Fatal("accepted refresh re-dispatched after restart")
			}
			if err := acceptedHealthGuard(root, receipt.Commit)(ctx); err != nil {
				t.Fatalf("adoption failed: %v", err)
			}
			if err := acceptedHealthGuard(root, old.Commit)(ctx); !errors.Is(err, memory.ErrHealthRefreshRequired) {
				t.Fatalf("old binary trusted after restart: %v", err)
			}
		})
	}
}
