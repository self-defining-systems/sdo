package operatorcli

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/install"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	client := fake.NewSimpleClientset(job, pod)

	afterStarted := time.Now()
	result, err := runIncidentEvidence(context.Background(), client, IncidentEvidenceConfig{Sync: install.SyncConfig{ControlNamespace: "sdo", RepositoryPVC: "repo", ControllerImage: "controller:test"}, IncidentID: "incident-1", Repository: repository})
	after := time.Since(afterStarted)
	if err != nil {
		t.Fatal(err)
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
	if !strings.Contains(string(calls), "exec controller-1 -- cat") {
		t.Fatalf("did not read from controller: %s", calls)
	}
	pods, err := client.CoreV1().Pods("sdo").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("temporary sync Pod created: %v", pods.Items)
	}

	oldClient := fake.NewSimpleClientset()
	created := false
	oldClient.PrependReactor("create", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		created = true
		return false, nil, nil
	})
	oldClient.PrependReactor("get", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !created {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, install.RepositorySyncPodName)
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: install.RepositorySyncPodName, Namespace: "sdo"}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
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
