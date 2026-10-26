package operatorcli

import (
	"context"
	"google.golang.org/protobuf/types/known/anypb"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/worker_registry"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/install"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestIncidentEvidenceUsesRunningControllerPodWithoutCreatingSyncPod(t *testing.T) {
	dir := t.TempDir()
	log := filepath.Join(dir, "kubectl.log")
	script := filepath.Join(dir, "kubectl")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SDO_TEST_KUBECTL_LOG\"\ncase \"$*\" in *' -- tar '*) exec /usr/bin/tar -C \"$SDO_TEST_SOURCE\" -cf - .;; esac\nprintf '{\"incident_id\":\"incident-1\"}'\n"), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SDO_TEST_KUBECTL_LOG", log)
	source := filepath.Join(dir, "source")
	repository := filepath.Join(dir, "repository")
	for _, path := range []string{source, repository} {
		if err := os.Mkdir(path, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "accepted.txt"), []byte("accepted"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SDO_TEST_SOURCE", source)
	for _, args := range [][]string{{"init", "-q"}, {"-c", "user.name=Test", "-c", "user.email=test@example.com", "add", "accepted.txt"}, {"-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-qm", "accepted"}} {
		if output, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	if output, err := exec.Command("git", "-C", repository, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: install.ControllerJobName, Namespace: "sdo", UID: "controller-job"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "controller-1", Namespace: "sdo", OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: install.ControllerJobName, UID: job.UID}}, Labels: map[string]string{"job-name": install.ControllerJobName}}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	warm := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-standby-completed", Namespace: "sdo"}, Status: batchv1.JobStatus{Succeeded: 1}}
	request, _ := anypb.New(&contracts.IncidentRequest{IncidentId: "incident-1", RepositoryBaseCommit: "accepted", RepositoryWorktree: "/workspace/proposal"})
	task := &contracts.AgentTask{SchemaVersion: "sdo.dev/v1alpha1", TaskId: "incident-1", AgentId: "responder", TaskType: "incident", Request: request, Workspace: &contracts.ProposalWorkspace{ProposalId: "proposal", CheckoutPath: "/workspace/proposal", BaseCommit: "accepted"}}
	payload, encodeErr := wire.Canonical(task)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: warm.Name + "-request", Namespace: "sdo"}, Data: map[string]string{worker_registry.TaskDataKey: string(payload)}}
	client := fake.NewSimpleClientset(job, pod, warm, cm)

	afterStarted := time.Now()
	result, err := runIncidentEvidence(context.Background(), client, IncidentEvidenceConfig{Sync: install.SyncConfig{ControlNamespace: "sdo", RepositoryPVC: "repo", ControllerImage: "controller:test"}, IncidentID: "incident-1", Repository: repository}, func(_ context.Context, namespace, pod string, command []string, stdout, stderr io.Writer) error {
		if namespace != "sdo" || pod != "controller-1" || len(command) != 2 || command[0] != "cat" {
			t.Fatalf("wrong evidence exec: %s %s %v", namespace, pod, command)
		}
		_, err := io.WriteString(stdout, `{"incident_id":"incident-1"}`)
		return err
	})
	after := time.Since(afterStarted)
	if err != nil {
		t.Fatal(err)
	}
	if result.ResponderJobName != "sdo-standby-completed" {
		t.Fatalf("wrong receipt identity %q", result.ResponderJobName)
	}
	if result.IncidentID != "incident-1" {
		t.Fatalf("incident ID = %q", result.IncidentID)
	}
	accepted, err := os.ReadFile(filepath.Join(repository, "accepted.txt"))
	if err != nil || string(accepted) != "accepted" {
		t.Fatalf("repository sync: %q, %v", accepted, err)
	}
	calls, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(calls), " -- cat") {
		t.Fatalf("ledger spawned kubectl: %s", calls)
	}
	pods, err := client.CoreV1().Pods("sdo").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("temporary sync Pod created: %v", pods.Items)
	}

	oldClient := fake.NewSimpleClientset()
	// Report readiness on the real tracked Pod, including actual deletion.
	oldClient.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		object, err := oldClient.Tracker().Get(corev1.SchemeGroupVersion.WithResource("pods"), "sdo", install.RepositorySyncPodName)
		if err != nil {
			return true, nil, err
		}
		pod := object.(*corev1.Pod).DeepCopy()
		pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		return true, pod, nil
	})
	old := install.Installer{Client: oldClient}
	beforeStarted := time.Now()
	if err := old.WithSyncPod(context.Background(), install.SyncConfig{ControlNamespace: "sdo", RepositoryPVC: "repo", ControllerImage: "controller:test"}, func(ctx context.Context, cfg install.Config) error {
		if err := install.KubectlTarSynchronizerWithKubeconfig(repository, "")(ctx, cfg); err != nil {
			return err
		}
		_, err := exec.CommandContext(ctx, "kubectl", "--namespace", "sdo", "exec", install.RepositorySyncPodName, "--", "cat", "/workspace/worktrees/native-ledger/incident.json").Output()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	before := time.Since(beforeStarted)
	oldCreates, newCreates := 0, 0
	for _, action := range oldClient.Actions() {
		if action.Matches("create", "pods") {
			oldCreates++
		}
	}
	for _, action := range client.Actions() {
		if action.Matches("create", "pods") {
			newCreates++
		}
	}
	if oldCreates != 1 || newCreates != 0 {
		t.Fatalf("sync Pod creations before=%d after=%d", oldCreates, newCreates)
	}
	t.Logf("hermetic transport before=%s (%d Pod), after=%s (%d Pod)", before, oldCreates, after, newCreates)
}

func TestParseIncidentEvidenceLaunch(t *testing.T) {
	cfg, err := ParseIncidentEvidenceLaunch([]string{"incident-evidence", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--incident-id", "incident-1", "--kubeconfig", "/tmp/kube"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IncidentID != "incident-1" || cfg.Sync.ControlNamespace != "sdo" || cfg.Sync.RepositoryPVC != "repo" {
		t.Fatalf("lost evidence identity: %+v", cfg)
	}
	if _, err := ParseIncidentEvidenceLaunch([]string{"incident-evidence", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test"}); err == nil {
		t.Fatal("missing incident accepted")
	}
	withSync, err := ParseIncidentEvidenceLaunch([]string{"incident-evidence", "--control-namespace", "sdo", "--repository-pvc", "repo", "--controller-image", "controller:test", "--incident-id", "incident-1", "--repository", "/tmp/accepted"})
	if err != nil || withSync.Repository != "/tmp/accepted" {
		t.Fatalf("single-pod repository sync unavailable: %+v, %v", withSync, err)
	}
}
