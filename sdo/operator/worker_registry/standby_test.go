package worker_registry

import (
	"context"
	"encoding/json"
	"fmt"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sdo.dev/operator/memory"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/client-go/kubernetes/fake"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func standbyFixture(t *testing.T, command ...string) (*StandbyPool, KubernetesExecutor, *contracts.AgentTask) {
	t.Helper()
	if len(command) == 0 {
		command = []string{"python3", "-m", "benchmark.recording_worker", "responder"}
	}
	root := t.TempDir()
	reg, err := New(&contracts.AgentRegistration{SchemaVersion: SchemaVersion, AgentId: "responder", Capabilities: []*contracts.AgentCapability{{TaskType: "incident", RequestTypeUrl: TypeURL(&contracts.IncidentRequest{}), ResultTypeUrl: TypeURL(&contracts.IncidentResult{})}}, Launch: &contracts.AgentRegistration_KubernetesJob{KubernetesJob: &contracts.KubernetesJobLaunch{Image: "worker", Command: command}}})
	if err != nil {
		t.Fatal(err)
	}
	request, _ := anypb.New(&contracts.IncidentRequest{IncidentId: "incident", RepositoryBaseCommit: "accepted", RepositoryWorktree: root + "/proposal"})
	task := &contracts.AgentTask{SchemaVersion: SchemaVersion, TaskId: "incident", AgentId: "responder", TaskType: "incident", Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "p", CheckoutPath: root + "/proposal", BaseCommit: "accepted"}}
	for _, dir := range []string{root + "/proposal", root + "/session/api-history", root + "/session/corpus"} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(root+"/proposal/source", []byte("accepted"), 0600); err != nil {
		t.Fatal(err)
	}
	e := KubernetesExecutor{Client: fake.NewSimpleClientset(), Namespace: "control", Registry: reg, RepositoryPVC: "repo", RepositoryMountPath: root, SessionPVCSubPath: "session", ServiceAccount: "sdo-responder", CredentialsSecret: "same", PollInterval: time.Millisecond, Environment: map[string]string{}}
	for _, args := range [][]string{{"init", "-q"}, {"add", "source"}, {"-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "-qm", "accepted"}} {
		if output, err := exec.Command("git", append([]string{"-C", task.Workspace.CheckoutPath}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %s %v", args, output, err)
		}
	}
	head, err := exec.Command("git", "-C", task.Workspace.CheckoutPath, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	task.Workspace.BaseCommit = strings.TrimSpace(string(head))
	typed := new(contracts.IncidentRequest)
	_ = task.Request.UnmarshalTo(typed)
	typed.RepositoryBaseCommit = task.Workspace.BaseCommit
	task.Request, _ = anypb.New(typed)
	pool := NewStandbyPool(e, "responder", "app")
	return pool, e, task
}

func TestStandbyNotReadyFallsBackWithoutWaiting(t *testing.T) {
	pool, e, task := standbyFixture(t)
	start := time.Now()
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding != nil {
		t.Fatal("cold fallback failed", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("waited for standby readiness")
	}
}

func TestStandbyClaimIsSingleUseAndReplenishes(t *testing.T) {
	pool, e, task := standbyFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pool.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	first := pool.current
	if first == nil {
		t.Fatal("no standby Job")
	}
	if err := os.WriteFile(filepath.Join(first.Directory, "ready/agent"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(first.Directory, "ready/history"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	bound, err := pool.TryBind(ctx, e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatalf("claim failed: %v", err)
	}
	next, err := pool.TryBind(ctx, e, task)
	if err != nil || next.WarmBinding == nil || next.WarmBinding.JobName != bound.WarmBinding.JobName {
		t.Fatal("retry did not rejoin same claim", err)
	}
	if err := pool.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if pool.current == nil || pool.current.JobName == first.JobName {
		t.Fatal("standby not replenished")
	}
	if data, err := os.ReadFile(task.Workspace.CheckoutPath + "/source"); err != nil || string(data) != "accepted" {
		t.Fatal("binding lost source", err)
	}
}

func readyStandby(t *testing.T, pool *StandbyPool) *WarmBinding {
	t.Helper()
	if err := pool.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	slot := pool.current
	for _, name := range []string{"agent", "history"} {
		if err := os.WriteFile(filepath.Join(slot.Directory, "ready", name), []byte("waiting"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return slot
}

func TestStandbyBindingRetainsMountedInodesAndDurableSession(t *testing.T) {
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	checkout, err := os.OpenRoot(filepath.Join(slot.Directory, "checkout"))
	if err != nil {
		t.Fatal(err)
	}
	defer checkout.Close()
	session, err := os.OpenRoot(filepath.Join(slot.Directory, "session"))
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for _, name := range []string{"codex/session", "claude/session", "corpus/open.json"} {
		file := filepath.Join(e.RepositoryMountPath, "session", name)
		if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte("durable"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal(err)
	}
	if data, err := checkout.ReadFile("source"); err != nil || string(data) != "accepted" {
		t.Fatal("mounted checkout inode lost", err)
	}
	for _, name := range []string{"codex/session", "claude/session", "corpus/open.json"} {
		if data, err := session.ReadFile(name); err != nil || string(data) != "durable" {
			t.Fatal("mounted session lost", name, err)
		}
	}
	if err := session.WriteFile("codex/reflection-session", []byte("same-provider-session"), 0600); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(e.RepositoryMountPath, "session/codex/reflection-session")); err != nil || string(data) != "same-provider-session" {
		t.Fatal("reflection cannot see session", err)
	}
}

func TestStandbyBusyCaptureAndFailedClaimFallBack(t *testing.T) {
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	release := memory.SessionStorageUse()
	bound, err := pool.TryBind(context.Background(), e, task)
	release()
	if err != nil || bound.WarmBinding != nil {
		t.Fatal("busy capture did not abstain", err)
	}
	if err := os.RemoveAll(task.Workspace.CheckoutPath + "/.git"); err != nil {
		t.Fatal(err)
	}
	bound, err = pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding != nil {
		t.Fatal("bad checkout did not use cold path", err)
	}
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", TaskDataKey)); !os.IsNotExist(err) {
		t.Fatal("failed claim published a task")
	}
}

func TestStandbyRestartRejoinsClaimAndRetiresStaleIdle(t *testing.T) {
	pool, e, task := standbyFixture(t)
	first := readyStandby(t, pool)
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal(err)
	}
	if err := bound.publishWarmTask(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if err := pool.Ensure(context.Background()); err != nil {
		t.Fatal(err)
	}
	idle := pool.current
	restarted := NewStandbyPool(e, "responder", "app")
	if err := restarted.recoverUnclaimed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), idle.JobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("stale idle Job survived", err)
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), first.JobName, metav1.GetOptions{}); err != nil {
		t.Fatal("claimed Job retired", err)
	}
	joined, err := restarted.TryBind(context.Background(), e, task)
	if err != nil || joined.WarmBinding == nil || joined.WarmBinding.JobName != first.JobName {
		t.Fatal("restart changed claimant", err)
	}
	if err := joined.publishWarmTask(context.Background(), task); err != nil {
		t.Fatal("idempotent publication", err)
	}
}

func TestStandbyJobPreservesSecurityAndConfiguredCommand(t *testing.T) {
	pool, e, _ := standbyFixture(t)
	slot := readyStandby(t, pool)
	job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(context.Background(), slot.JobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod := job.Spec.Template.Spec
	agent := pod.Containers[0]
	if pod.ServiceAccountName != e.ServiceAccount || !*pod.SecurityContext.RunAsNonRoot || *agent.SecurityContext.AllowPrivilegeEscalation || !*agent.SecurityContext.ReadOnlyRootFilesystem {
		t.Fatal("standby broadened pod authority")
	}
	if len(agent.EnvFrom) != 1 || agent.EnvFrom[0].SecretRef.Name != "same" {
		t.Fatal("standby credentials changed")
	}
	var command []string
	if err := json.Unmarshal([]byte(agent.Command[3]), &command); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(command, []string{"python3", "-m", "benchmark.recording_worker", "responder"}) {
		t.Fatalf("registration rewritten: %v", command)
	}
	for _, container := range append(pod.InitContainers, agent) {
		for _, mount := range container.VolumeMounts {
			if mount.Name == "repository" && (mount.SubPath == "" || mount.MountPath == e.RepositoryMountPath) {
				t.Fatalf("standby exposes PVC root: %+v", mount)
			}
		}
	}
	for _, mount := range agent.VolumeMounts {
		if mount.MountPath == "/sdo/request" || mount.MountPath == "/sdo/session/corpus" || mount.MountPath == "/sdo/session/api-history" || mount.MountPath == "/sdo/session/tools" {
			if !mount.ReadOnly {
				t.Fatalf("controller boundary writable: %+v", mount)
			}
		}
	}
}

func TestStandbyPublicationFailureUsesColdJobAndPreservesCanonicalDirectories(t *testing.T) {
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(slot.Directory, "control/environment.json"), 0700); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go func() { _, err := bound.Execute(ctx, task); done <- err }()
	deadline := time.Now().Add(time.Second)
	cold := false
	for time.Now().Before(deadline) {
		if _, err := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, JobName(task.TaskId), metav1.GetOptions{}); err == nil {
			cold = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if !cold {
		t.Fatal("unpublished claim did not launch cold Job")
	}
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", TaskDataKey)); !os.IsNotExist(err) {
		t.Fatal("cold fallback also published warm task")
	}
	if data, err := os.ReadFile(task.Workspace.CheckoutPath + "/source"); err != nil || string(data) != "accepted" {
		t.Fatal("cold fallback lost canonical proposal", err)
	}
	if err := pool.recoverUnclaimed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(task.Workspace.CheckoutPath + "/source"); err != nil || string(data) != "accepted" {
		t.Fatal("restart rolled back active cold proposal", err)
	}
}

func TestStandbyRealDaemonClaimResultAndReplenishment(t *testing.T) {
	const script = `import os,json,datetime,pathlib,subprocess
ready=int(os.environ.pop('SDO_STANDBY_READY_FD'));claim=int(os.environ.pop('SDO_STANDBY_CLAIM_FD'));inbox=pathlib.Path(os.environ.pop('SDO_STANDBY_INBOX'))
os.write(ready,b'R');os.close(ready)
assert os.read(claim,1)==b'C';os.close(claim)
os.environ.update(json.loads((inbox/'environment.json').read_text()));os.environ['SDO_TASK_PATH']=str(inbox/'agent-task.json')
wire=json.loads(pathlib.Path(os.environ['SDO_TASK_PATH']).read_text())
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head == wire['workspace']['base_commit']
pathlib.Path('source').write_text('repaired')
request=wire['request']
result={'schema_version':wire['schema_version'],'task_id':wire['task_id'],'status':'completed','proposal_id':wire['workspace']['proposal_id'],'response':{'@type':'type.googleapis.com/sdodev.contracts.v1alpha1.IncidentResult','incident_id':request['incident_id'],'status':'completed'}}
pathlib.Path(os.environ['TEST_SESSION'],'codex').mkdir(exist_ok=True)
pathlib.Path(os.environ['TEST_SESSION'],'codex/session').write_text('durable-session')
pathlib.Path(os.environ['TEST_RESULT']).write_text(json.dumps(result))
`
	pool, e, task := standbyFixture(t, "python3", "-c", script)
	e.Environment["TEST_RESULT"] = filepath.Join(e.RepositoryMountPath, "session/result.json")
	e.Environment["TEST_SESSION"] = filepath.Join(e.RepositoryMountPath, "session")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	slot := pool.current
	if err := os.WriteFile(filepath.Join(slot.Directory, "ready/history"), []byte("waiting"), 0600); err != nil {
		t.Fatal(err)
	}
	job, err := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, slot.JobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var registered []string
	if err := json.Unmarshal([]byte(job.Spec.Template.Spec.Containers[0].Command[3]), &registered); err != nil {
		t.Fatal(err)
	}
	mountedDirectory, err := os.Open(filepath.Join(slot.Directory, "checkout"))
	if err != nil {
		t.Fatal(err)
	}
	defer mountedDirectory.Close()
	mountedCheckout := fmt.Sprintf("/proc/%d/fd/%d", os.Getpid(), mountedDirectory.Fd())
	daemonDone := make(chan error, 1)
	go func() {
		daemonDone <- RunStandbyDaemon(ctx, StandbyDaemonOptions{Inbox: filepath.Join(slot.Directory, "control"), Ready: filepath.Join(slot.Directory, "ready/agent"), Checkout: mountedCheckout, SessionDirectory: filepath.Join(e.RepositoryMountPath, "session"), Command: registered})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(slot.Directory, "ready/agent")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon never advertised imported worker readiness")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(e.Environment["TEST_RESULT"]); !os.IsNotExist(err) {
		t.Fatal("standby executed before receiving an incident")
	}
	bound, err := pool.TryBind(ctx, e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal(err)
	}
	resultDone := make(chan error, 1)
	go func() {
		result, err := bound.Execute(ctx, task)
		if err == nil && result.TaskId != task.TaskId {
			err = fmt.Errorf("result identity changed")
		}
		resultDone <- err
	}()
	if err := <-daemonDone; err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(bound.WarmBinding.ClaimedAt)
	if elapsed >= 3*time.Second {
		t.Fatalf("scripted claim through process/result took %s", elapsed)
	}
	data, err := os.ReadFile(e.Environment["TEST_RESULT"])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: slot.JobName + "-result"}, Data: map[string]string{ResultDataKey: string(data)}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	job.Status.Succeeded = 1
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).UpdateStatus(ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := <-resultDone; err != nil {
		t.Fatal(err)
	}
	if err := pool.Ensure(ctx); err != nil {
		t.Fatal(err)
	}
	if pool.current.JobName == slot.JobName {
		t.Fatal("claim did not replenish")
	}
	if data, err := os.ReadFile(filepath.Join(e.RepositoryMountPath, "session/codex/session")); err != nil || string(data) != "durable-session" {
		t.Fatal("session did not survive worker exit", err)
	}
	if data, err := os.ReadFile(filepath.Join(task.Workspace.CheckoutPath, "source")); err != nil || string(data) != "repaired" {
		t.Fatal("warm source edits missed canonical broker checkout", err)
	}
	t.Logf("scripted warm claim to real worker result: %s", elapsed)
}

func TestStandbyRejectsEscapingCheckoutBeforeAnyBinding(t *testing.T) {
	pool, e, task := standbyFixture(t)
	readyStandby(t, pool)
	task.Workspace.CheckoutPath = filepath.Join(t.TempDir(), "outside")
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding != nil {
		t.Fatal("escaping checkout was bound", err)
	}
	if _, err := os.Stat(filepath.Join(pool.current.Directory, "reservation")); !os.IsNotExist(err) {
		t.Fatal("escaping checkout consumed standby")
	}
}

func TestStandbyInterruptedBindingRestoresCanonicalDirectories(t *testing.T) {
	for _, cut := range []int{0, 1, 2, 3, 4} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			pool, e, task := standbyFixture(t)
			slot := readyStandby(t, pool)
			moves := []standbyMove{{task.Workspace.CheckoutPath, filepath.Join(slot.Directory, "checkout"), filepath.Join(slot.Directory, "backup-checkout")}, {filepath.Join(e.RepositoryMountPath, "session"), filepath.Join(slot.Directory, "session"), filepath.Join(slot.Directory, "backup-session")}}
			j := standbyJournal{Binding: *slot, Moves: moves}
			if err := writeStandbyJSON(filepath.Join(slot.Directory, "binding-journal.json"), j); err != nil {
				t.Fatal(err)
			}
			operations := 0
			for _, move := range moves {
				for _, pair := range [][2]string{{move.Original, move.Backup}, {move.Staged, move.Original}} {
					if operations < cut {
						if err := os.Rename(pair[0], pair[1]); err != nil {
							t.Fatal(err)
						}
					}
					operations++
				}
			}
			if err := pool.recoverUnclaimed(context.Background()); err != nil {
				t.Fatal(err)
			}
			if data, err := os.ReadFile(task.Workspace.CheckoutPath + "/source"); err != nil || string(data) != "accepted" {
				t.Fatalf("cut %d lost canonical proposal: %v", cut, err)
			}
			if info, err := os.Stat(filepath.Join(e.RepositoryMountPath, "session")); err != nil || !info.IsDir() {
				t.Fatalf("cut %d lost session: %v", cut, err)
			}
		})
	}
}

func TestStandbyExecutorRejoinsDurableBindingWithoutPool(t *testing.T) {
	pool, e, task := standbyFixture(t)
	slot := readyStandby(t, pool)
	bound, err := pool.TryBind(context.Background(), e, task)
	if err != nil || bound.WarmBinding == nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, _ = e.Execute(ctx, task)
	if _, err := os.Stat(filepath.Join(slot.Directory, "control", TaskDataKey)); err != nil {
		t.Fatal("restarted executor did not publish its durable claim without the pool", err)
	}
	data, err := os.ReadFile(filepath.Join(e.RepositoryMountPath, "session/startup/startup-dispatch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var timing DispatchStartup
	if err := json.Unmarshal(data, &timing); err != nil {
		t.Fatal(err)
	}
	if timing.Mode != "warm" || timing.ClaimedAt == nil {
		t.Fatal("restart hid warm claim as cold", string(data))
	}
}
