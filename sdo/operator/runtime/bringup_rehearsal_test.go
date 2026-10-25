package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sdo.dev/operator/broker"
	"sdo.dev/operator/builder"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/core"
	"sdo.dev/operator/install"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/onboarding"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/worker_registry"
)

// A scripted judge represents the interrupted previous install. Its exact
// completed transport result survives reset; the real executor must recover it
// without creating another worker. Proposal validation/publication remain real.
type rehearsalJudge struct {
	client     *fake.Clientset
	scripted   *tracingRefreshWorker
	captured   *contracts.AgentTask
	recovering bool
}

func (w *rehearsalJudge) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if !w.recovering {
		if w.captured != nil {
			return nil, errors.New("unexpected second aborted worker")
		}
		result, err := w.scripted.Execute(ctx, task)
		if err != nil {
			return nil, err
		}
		w.captured = proto.Clone(task).(*contracts.AgentTask)
		name := worker_registry.JobName(task.TaskId)
		for key, payload := range map[string]proto.Message{"request": task, "result": result} {
			data, err := wire.Canonical(payload)
			if err != nil {
				return nil, err
			}
			dataKey := worker_registry.TaskDataKey
			if key == "result" {
				dataKey = worker_registry.ResultDataKey
			}
			_, err = w.client.CoreV1().ConfigMaps("demo-sdo").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name + "-" + key, Namespace: "demo-sdo", CreationTimestamp: metav1.Now()}, Data: map[string]string{dataKey: string(data)}}, metav1.CreateOptions{})
			if err != nil {
				return nil, err
			}
		}
		return nil, context.Canceled
	}
	if !proto.Equal(task, w.captured) {
		return nil, errors.New("restart replaced frozen judge task")
	}
	executor := worker_registry.KubernetesExecutor{Client: w.client, Namespace: "demo-sdo", Registry: w.scripted.registry, RepositoryPVC: "accepted", RepositoryMountPath: filepath.Dir(filepath.Dir(task.Workspace.CheckoutPath)), PollInterval: time.Millisecond}
	return executor.Execute(ctx, task)
}

// Rehearse the native bring-up, including the benchmark's actual HY restore,
// against one shared fake cluster. Simulated probe time avoids a minute sleep.
func TestHermeticBringupPersistsPendingAndRestarts(t *testing.T) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	root, proposals, _, scripted, coordinator := tracingFixture(t)
	operator, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reachabilityApp(8083)
	snapshot.NamespaceName = "demo"
	snapshot.ServiceList[0].Namespace = "demo"
	snapshot.PodList[0].Namespace = "demo"
	snapshot.ServiceList[0].ResourceVersion = "1"
	snapshot.PodList[0].Spec.Containers[0].Image = "fixture:test"
	for _, port := range []int32{8084, 8085} {
		snapshot.ServiceList[0].Spec.Ports = append(snapshot.ServiceList[0].Spec.Ports, corev1.ServicePort{Name: "never-" + strconv.Itoa(int(port)), Port: port, TargetPort: intstr.FromInt32(port)})
	}
	one := int32(1)
	deployment := &appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "jaeger", Namespace: "demo", ResourceVersion: "1"}, Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "geo"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "geo"}}, Spec: snapshot.PodList[0].Spec}}, Status: appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1, AvailableReplicas: 1, UpdatedReplicas: 1}}
	snapshot.ServiceList[0].TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}
	snapshot.PodList[0].TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	baseline := []kruntime.Object{&snapshot.ServiceList[0], deployment, &snapshot.PodList[0]}
	var manifests bytes.Buffer
	for _, obj := range baseline {
		raw, e := json.Marshal(obj)
		if e != nil {
			t.Fatal(e)
		}
		manifests.Write(raw)
		manifests.WriteString("\n---\n")
	}
	tracingWrite(t, root, "deploy/tracing.yaml", manifests.String())
	nativeGit(t, root, "add", ".")
	nativeGit(t, root, "commit", "-qm", "unverified P35-shaped adopted seed")
	installed := nativeGit(t, root, "rev-parse", "HEAD")
	installed = seedRenderedLock(t, GitPlanSource{Repository: root, ManifestPaths: []string{"deploy/tracing.yaml"}}, installed, "demo")
	client := fake.NewSimpleClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo-sdo"}}, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}, snapshot.ServiceList[0].DeepCopy(), deployment.DeepCopy(), snapshot.PodList[0].DeepCopy())
	// Prior install authority and a completed worker result are both leftovers.
	if err := onboarding.Stage(ctx, client, "demo-sdo", "demo", strings.Repeat("a", 40), nil, time.Unix(50, 0)); err != nil {
		t.Fatal(err)
	}
	old, err := onboarding.Pending(ctx, client, "demo-sdo", "demo")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := onboarding.Complete(ctx, client, "demo-sdo", old); err != nil {
		t.Fatal(err)
	}
	judge := &rehearsalJudge{client: client, scripted: scripted}
	coordinator.Workers = judge
	revision, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil || !revision.NeedsRefresh {
		t.Fatalf("seed must need real authority: %+v %v", revision, err)
	}
	if _, err := coordinator.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, revision); !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted prior attempt: %v", err)
	}
	oldResultName := worker_registry.JobName(judge.captured.TaskId) + "-result"
	oldResult, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, oldResultName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// HY restores a real targetPort fault using kubectl against this same tracker.
	broken, err := client.CoreV1().Services("demo").Get(ctx, "geo", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	broken.Spec.Ports[0].TargetPort = intstr.FromInt32(9999)
	if _, err := client.CoreV1().Services("demo").Update(ctx, broken, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	source := rendered.Source{Repository: root, ManifestPaths: []string{"deploy/tracing.yaml"}}
	declared, err := source.RawManifests(ctx, installed, "demo")
	if err != nil {
		t.Fatal(err)
	}
	// Native prepared installation uses its trusted lifecycle hook to freeze the
	// accepted install inventory, exactly before starting the controller Job.
	installer := install.Installer{Client: client, BeforeControllerStart: func(ctx context.Context, _ install.Config) error {
		return onboarding.Stage(ctx, client, "demo-sdo", "demo", installed, declared, time.Unix(100, 0))
	}}
	config := install.Config{Namespace: "demo", ControlNamespace: "demo-sdo", Application: "tracing", AcceptedCommit: installed, Model: "scripted-no-model", TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}"), ControllerImage: "controller:test", ResponderImage: "responder:test", ValidatorImage: "validator:test", RepositoryPVC: "accepted", CredentialsSecret: "credentials"}

	var installs atomic.Int32
	kubeconfig := bringupRehearsalAPI(t, client, func() error {
		if record, err := onboarding.Load(ctx, client, "demo-sdo", "demo"); err != nil || record != nil {
			return errors.New("fresh-stream install ran before real reset")
		}
		installs.Add(1)
		_, err := installer.InstallPrepared(ctx, config, root)
		return err
	})
	runDir := t.TempDir()
	baselinePath := filepath.Join(runDir, "baseline.json")
	raw, _ := json.Marshal(baseline)
	if err := os.WriteFile(baselinePath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(runDir, "sdo-operator")
	build := exec.CommandContext(ctx, "go", "build", "-o", launcher, "./cmd/sdo-operator")
	build.Dir = operator
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build native reset launcher: %v\n%s", err, out)
	}
	command := exec.CommandContext(ctx, "uv", "run", "--extra", "test", "python", "tests/fixtures/sdo/bringup_rehearsal/restore.py", runDir, kubeconfig, root, installed, baselinePath)
	command.Dir = filepath.Clean(filepath.Join(operator, "../.."))
	command.Env = append(os.Environ(), "SDO_OPERATOR_BIN="+launcher, "KUBECONFIG="+kubeconfig)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("HY restore + CLI fresh-stream start: %v\n%s", err, out)
	} else {
		t.Log(string(out))
	}
	if installs.Load() != 1 {
		t.Fatalf("native prepared installs=%d", installs.Load())
	}
	restored, err := client.CoreV1().Services("demo").Get(ctx, "geo", metav1.GetOptions{})
	if err != nil || restored.Spec.Ports[0].TargetPort.IntVal != 8083 {
		t.Fatalf("HY did not restore actual cluster: %v %v", restored, err)
	}
	pending, err := onboarding.Pending(ctx, client, "demo-sdo", "demo")
	if err != nil || pending == nil || pending.AcceptedCommit != installed {
		t.Fatalf("native pending: %v %v", pending, err)
	}
	assertNoAttempt := func() {
		t.Helper()
		if _, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, onboarding.AttemptConfigMapName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("unconfirmed window consumed attempt: %v", err)
		}
	}
	assertNoAttempt()
	// The broker's compile transport executes the real builder acceptance check.
	validations := 0
	repository, err := broker.New(root, proposals, func(ctx context.Context, candidate string, _ []string) error {
		validations++
		workspace, err := builder.BuildWorkspace(ctx, builder.Config{ApplicationRoot: candidate, DiagnosticsRoot: filepath.Join(candidate, ".sdo/diagnostics"), OperatorRoot: operator})
		if err != nil {
			return err
		}
		defer workspace.Close()
		return workspace.Check(ctx)
	})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.Broker = repository
	judge.recovering = true
	client.PrependReactor("create", "jobs", func(ktesting.Action) (bool, kruntime.Object, error) {
		return true, nil, errors.New("completed prior judge attempted a new worker Job")
	})
	beforeJobs, err := client.BatchV1().Jobs("demo-sdo").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.RefreshHealth(ctx, lifecycle.Config{Application: "tracing", Namespace: "demo"}, revision); err != nil {
		t.Fatal(err)
	}
	afterJobs, err := client.BatchV1().Jobs("demo-sdo").List(ctx, metav1.ListOptions{})
	if err != nil || len(afterJobs.Items) != len(beforeJobs.Items) {
		t.Fatalf("old completed result reran a worker: %d -> %d %v", len(beforeJobs.Items), len(afterJobs.Items), err)
	}
	if scripted.calls != 1 || validations != 1 {
		t.Fatalf("setup work: judge=%d validator=%d", scripted.calls, validations)
	}
	afterResult, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, oldResultName, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(afterResult.Data, oldResult.Data) {
		t.Fatal("reset/recovery overwrote old result evidence")
	}
	accepted, err := memory.AcceptedHealthRevision(ctx, root)
	if err != nil || accepted.NeedsRefresh {
		t.Fatalf("refresh authority missing: %+v %v", accepted, err)
	}
	assertNoAttempt()
	goal := inventoryHealth().(*sequenceDetector)
	goal.spec.ID = "health-objective"
	reach := servicehealth.NewReachabilityDetector(sdk.DetectorSpec{ID: "service-reachability", OriginatingCommit: "lifecycle-bootstrap", Playbooks: []string{"health-objective"}, Watches: servicehealth.ReachabilityWatches(), Class: sdk.DetectorClassHealth, Owner: sdk.DetectorOwnerHealthJudge, Interval: 10 * time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1}, Batching: sdk.BatchingPolicy{Severity: sdk.SeverityCritical}})
	provider := &mutableSnapshots{snapshot: snapshot}
	dispatcher := &recordingDispatcher{requests: make(chan IncidentRequest, 10)}
	controllerConfig := testControllerConfig()
	controllerConfig.Namespace = "demo"
	controllerConfig.RepositoryWorktree = root
	controllerConfig.SourceCommit = accepted.Commit
	newController := func(at time.Time) *Controller {
		t.Helper()
		c, err := NewController(controllerConfig, []sdk.Detector{goal, reach}, provider, dispatcher, at)
		if err != nil {
			t.Fatal(err)
		}
		c.CheckHealth = acceptedHealthGuard(root, accepted.Commit)
		if err := c.AttachStateStore(ctx, lifecycleio.NewConfigMapStateStore(client, "demo-sdo", "sdo-controller-state")); err != nil {
			t.Fatal(err)
		}
		if err := c.AttachPendingOnboarding(ctx, client, "demo-sdo"); err != nil {
			t.Fatal(err)
		}
		return c
	}
	c := newController(time.Unix(100, 0))
	window := &servicehealth.ReachabilityWindow{Failures: 3, Interval: 10 * time.Second}
	for _, port := range snapshot.ServiceList[0].Spec.Ports {
		window.Targets = append(window.Targets, servicehealth.TargetObservations{Target: servicehealth.ReachabilityTarget{Service: "geo", Port: port.Port, PortName: port.Name}})
	}
	probe := func(round int) {
		t.Helper()
		at := time.Unix(int64(101+round*10), 0)
		window.ObservedAt = at
		for i := range window.Targets {
			sample := servicehealth.DialSample{At: at, OK: i == 0}
			if sample.OK {
				window.Targets[i].LastSuccess = &at
			} else {
				sample.Class = servicehealth.DialRefused
			}
			window.Targets[i].Samples = append(window.Targets[i].Samples, sample)
		}
		snapshot.Reachability = window
		provider.set(snapshot)
		if err := c.EvaluateAll(ctx, at); err != nil {
			t.Fatal(err)
		}
		if err := c.PersistState(ctx); err != nil {
			t.Fatalf("real controller checkpoint round %d: %v", round, err)
		}
		if c.IncidentOpen() {
			t.Fatal("never-served declaration opened incident")
		}
		select {
		case request := <-dispatcher.requests:
			t.Fatalf("unsolicited responder: %s", request.IncidentID)
		default:
		}
	}
	for round := 0; round < 3; round++ {
		probe(round)
		if round < 2 {
			assertNoAttempt()
			state, _, err := lifecycleio.NewConfigMapStateStore(client, "demo-sdo", "sdo-controller-state").Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if state.History[len(state.History)-1].Status != "pending" {
				t.Fatal("unconfirmed probe persisted as clear")
			}
		}
	}
	final, err := onboarding.Load(ctx, client, "demo-sdo", "demo")
	if err != nil || !proto.Equal(final, pending) {
		t.Fatalf("final changed frozen install: %v %v", final, err)
	}
	reference, err := onboarding.LoadReachability(ctx, client, "demo-sdo", "demo")
	if err != nil || reference == nil || len(reference.Exclusions) != 2 {
		t.Fatalf("never-served reference: %+v %v", reference, err)
	}
	recordBefore, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, onboarding.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the actual read-only source evaluator, with native acknowledgement.
	ack, err := wire.Canonical(final)
	if err != nil {
		t.Fatal(err)
	}
	ackPath := filepath.Join(runDir, "onboarding.json")
	if err := os.WriteFile(ackPath, ack, 0600); err != nil {
		t.Fatal(err)
	}
	reportPath := filepath.Join(runDir, "source-report.json")
	client.ClearActions()
	if err := RunWithOptions(ctx, nil, RuntimeOptions{Args: []string{"--evaluate-once", "--namespace", "demo", "--app-root", root, "--deployed-manifest-path", "deploy/tracing.yaml", "--source-drift-acknowledgement", ackPath, "--source-drift-report", reportPath}, Provider: &core.KubernetesSnapshotProvider{Client: client, Namespace: "demo"}, Stdout: &bytes.Buffer{}}); err != nil {
		t.Fatal(err)
	}
	report, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var evaluated struct {
		Healthy  bool              `json:"healthy"`
		Findings []json.RawMessage `json:"findings"`
	}
	if err := json.Unmarshal(report, &evaluated); err != nil || !evaluated.Healthy || len(evaluated.Findings) != 0 {
		t.Fatalf("read-only evaluator: %s %v", report, err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" && action.GetVerb() != "watch" {
			t.Fatalf("evaluator wrote cluster: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
	for round := 3; round < 9; round++ {
		probe(round)
	}
	c = newController(time.Unix(200, 0))
	probe(10)
	recordAfter, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, onboarding.ConfigMapName, metav1.GetOptions{})
	if err != nil || !reflect.DeepEqual(recordBefore.Data, recordAfter.Data) {
		t.Fatal("restart recaptured native authority")
	}
	state, _, err := lifecycleio.NewConfigMapStateStore(client, "demo-sdo", "sdo-controller-state").Load(ctx)
	if err != nil || state.NamespaceOnboarding == nil {
		t.Fatalf("restart lost durable authority: %v", err)
	}
	if !time.Unix(100, 0).Equal(final.EstablishedAt.AsTime()) {
		t.Fatal("refresh rewrote install time")
	}
	t.Logf("full native bring-up + real HY restore + recovered judge + validated pending checkpoint + six quiet cycles + restart passed in %s", time.Since(started))
}
