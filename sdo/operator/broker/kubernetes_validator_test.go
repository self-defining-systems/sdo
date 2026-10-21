package broker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
)

func TestKubernetesValidatorFailureIncludesBoundedPodLogs(t *testing.T) {
	const jobName = "sdo-validator-failed"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/batch/v1/namespaces/sdo/jobs/" + jobName:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(batchv1.Job{Status: batchv1.JobStatus{Failed: 1}})
		case "/api/v1/namespaces/sdo/pods":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: jobName + "-pod"}}}})
		case "/api/v1/namespaces/sdo/pods/" + jobName + "-pod/log":
			_, _ = w.Write([]byte("validation error: " + strings.Repeat("x", 20000)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", PollInterval: time.Millisecond}
	err = launcher.waitJob(context.Background(), jobName)
	if err == nil || !strings.Contains(err.Error(), "validation error:") {
		t.Fatalf("failure omitted validator Pod logs: %v", err)
	}
	if len(err.Error()) > 9000 {
		t.Fatalf("validator failure logs are unbounded: %d bytes", len(err.Error()))
	}
}

func TestKubernetesValidatorReportsCompletedJobPhases(t *testing.T) {
	finished := metav1.Now()
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "sdo-validator-complete", Namespace: "sdo"}, Status: batchv1.JobStatus{Succeeded: 1, CompletionTime: &finished}})
	var phases []string
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", PollInterval: time.Millisecond, OnPhase: func(_ context.Context, phase string) error {
		phases = append(phases, phase)
		return nil
	}}
	if err := launcher.waitJob(context.Background(), "sdo-validator-complete"); err != nil {
		t.Fatal(err)
	}
	if strings.Join(phases, ",") != "validator_job_started,program_build_compile,corpus_replay_validation,validator_job_finished" {
		t.Fatalf("validator completion phases: %v", phases)
	}
}

func TestKubernetesValidatorTreeCheckUsesScratchWithReadOnlyCandidate(t *testing.T) {
	repository := t.TempDir()
	scratch := t.TempDir()
	for _, args := range [][]string{{"init", "-q", repository}, {"-C", repository, "config", "user.email", "test@example.invalid"}} {
		if output, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git setup %v: %v: %s", args, err, output)
		}
	}
	if err := os.WriteFile(filepath.Join(repository, "demo.txt"), []byte("candidate"), 0644); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", repository, "add", "demo.txt").CombinedOutput(); err != nil {
		t.Fatalf("git add: %v: %s", err, output)
	}
	tree, err := exec.Command("git", "-C", repository, "write-tree").Output()
	if err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(repository, ".git")
	if err := os.Chmod(gitDir, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(gitDir, 0755) })
	command := validatorTreeCheckCommand(repository, scratch)
	check := exec.Command("/bin/sh", "-eu", "-c", command)
	check.Env = append(os.Environ(), "SDO_CANDIDATE_TREE="+strings.TrimSpace(string(tree)))
	if output, err := check.CombinedOutput(); err != nil {
		t.Fatalf("read-only candidate tree check failed: %v: %s", err, output)
	}
	if _, err := os.Stat(filepath.Join(gitDir, "index.lock")); !os.IsNotExist(err) {
		t.Fatalf("tree check wrote to candidate Git directory: %v", err)
	}
	wrong := exec.Command("/bin/sh", "-eu", "-c", command)
	wrong.Env = append(os.Environ(), "SDO_CANDIDATE_TREE="+strings.Repeat("0", 40))
	if output, err := wrong.CombinedOutput(); err == nil {
		t.Fatalf("tree check accepted wrong candidate digest: %s", output)
	}
}

func TestKubernetesValidatorAcceptsOnlyKnownJobAdmissionDefaults(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		admitted := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		kubescheme.Scheme.Default(admitted)
		admitted.Spec.Template.Labels["batch.kubernetes.io/controller-uid"] = "job-uid"
		admitted.Spec.Template.Labels["batch.kubernetes.io/job-name"] = admitted.Name
		admitted.Spec.Template.Labels["controller-uid"] = "job-uid"
		admitted.Spec.Template.Labels["job-name"] = admitted.Name
		return true, admitted, nil
	})
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test"}
	job := launcher.canaryJob("sdo-validator-allow", "sdo-validator", false)
	if err := launcher.createJob(context.Background(), job); err != nil {
		t.Fatalf("known API Job defaults rejected: %v", err)
	}
}

func TestKubernetesValidatorCanaryUsesGoOperator(t *testing.T) {
	launcher := KubernetesValidationJobClient{Namespace: "sdo", Image: "validator:test"}
	for _, denied := range []bool{false, true} {
		job := launcher.canaryJob("sdo-validator-canary", "sdo-validator", denied)
		command := job.Spec.Template.Spec.Containers[0].Command
		expected := "allow"
		if denied {
			expected = "deny"
		}
		if len(command) != 3 || command[0] != "/usr/local/bin/sdo-operator" || command[1] != "network-canary" || command[2] != expected {
			t.Fatalf("network canary requires Go operator, got %v", command)
		}
	}
}

func TestKubernetesValidatorProjectsHealthyBaselineOnlyForIncidentGate(t *testing.T) {
	launcher := KubernetesValidationJobClient{RepositoryPVC: "repository", HealthyBaseline: true}
	job := launcher.validationJob("sdo-validator-test", "worktrees/candidate", strings.Repeat("a", 40), true)
	container := job.Spec.Template.Spec.Containers[0]
	if !strings.Contains(container.Command[3], "--healthy-baseline /sdo/healthy-baseline") {
		t.Fatalf("incident validator did not run baseline gate: %v", container.Command)
	}
	if len(container.VolumeMounts) != 3 || container.VolumeMounts[2].SubPath != ".sdo-baseline/healthy" || !container.VolumeMounts[2].ReadOnly {
		t.Fatalf("baseline should be an isolated read-only projection: %+v", container.VolumeMounts)
	}
	job = launcher.validationJob("sdo-validator-test", "worktrees/candidate", strings.Repeat("a", 40), false)
	if len(job.Spec.Template.Spec.Containers[0].VolumeMounts) != 2 {
		t.Fatal("bootstrap validation mounted a baseline before capture")
	}
}

func TestKubernetesValidatorCompilesProgramsOffline(t *testing.T) {
	launcher := KubernetesValidationJobClient{RepositoryPVC: "repository"}
	job := launcher.validationJob("sdo-validator-test", "worktrees/candidate", strings.Repeat("a", 40), false)
	env := map[string]string{}
	for _, entry := range job.Spec.Template.Spec.Containers[0].Env {
		env[entry.Name] = entry.Value
	}
	for name, want := range map[string]string{
		"GOMODCACHE": "/go/pkg/mod",
		"GOPROXY":    "off",
		"GOFLAGS":    "-mod=mod -p=2",
	} {
		if env[name] != want {
			t.Errorf("%s = %q, want %q", name, env[name], want)
		}
	}
}

func TestHealthyBaselineGateWaitsForAnIncidentDetectorProposal(t *testing.T) {
	candidate := t.TempDir()
	manifest := filepath.Join(candidate, ".sdo", "diagnostics", "detectors", "incidents", "fault", "manifest.yaml")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte(detectorYAML("incident", "fault", 1, "base")), 0o644); err != nil {
		t.Fatal(err)
	}
	if needed, err := baselineGateRequired(candidate, []string{".sdo/diagnostics/detectors/health/objective/detector.go"}); err != nil || needed {
		t.Fatalf("bootstrap health detector required a pre-fault snapshot: %v %v", needed, err)
	}
	if needed, err := baselineGateRequired(candidate, []string{".sdo/diagnostics/detectors/incidents/fault/detector.go"}); err != nil || !needed {
		t.Fatalf("incident detector skipped healthy replay: %v %v", needed, err)
	}
}

func TestKubernetesValidatorRejectsCredentialInjectionAtAdmission(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		admitted := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		kubescheme.Scheme.Default(admitted)
		allow := true
		admitted.Spec.Template.Spec.AutomountServiceAccountToken = &allow
		return true, admitted, nil
	})
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test"}
	if err := launcher.createJob(context.Background(), launcher.canaryJob("sdo-validator-allow", "sdo-validator", false)); err == nil {
		t.Fatal("validator accepted service account token injection")
	}
}

func TestKubernetesValidatorRunsNetworkControlsBeforeReadOnlyBuild(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	var jobs []*batchv1.Job
	var policy *networkingv1.NetworkPolicy
	client.PrependReactor("create", "networkpolicies", func(action ktesting.Action) (bool, runtime.Object, error) {
		policy = action.(ktesting.CreateAction).GetObject().(*networkingv1.NetworkPolicy).DeepCopy()
		return false, nil, nil
	})
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		jobs = append(jobs, job.DeepCopy())
		if len(jobs) == 3 && policy == nil {
			t.Fatal("validator ran before network policy")
		}
		now := metav1.Now()
		job.Status.Succeeded = 1
		job.Status.CompletionTime = &now
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
			t.Fatal(err)
		}
		return true, job, nil
	})
	launcher := KubernetesValidationJobClient{
		Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root),
		SchemaIdentity: "sdo.dev/v1alpha1:sha256:test", PollInterval: time.Millisecond,
	}
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 || policy == nil {
		t.Fatalf("expected positive and negative controls then validation, got %d jobs", len(jobs))
	}
	if jobs[0].Spec.Template.Labels["sdo.dev/validator-isolation"] != "" || jobs[1].Spec.Template.Labels["sdo.dev/validator-isolation"] != policy.Name {
		t.Fatal("network controls do not distinguish allowed and denied traffic")
	}
	if policy.Spec.PodSelector.MatchLabels["sdo.dev/validator-isolation"] != policy.Name || len(policy.Spec.Egress) != 0 || len(policy.Spec.Ingress) != 0 || len(policy.Spec.PolicyTypes) != 2 {
		t.Fatalf("validator policy is not default-deny: %+v", policy.Spec)
	}
	job := jobs[2]
	pod := job.Spec.Template.Spec
	container := pod.Containers[0]
	if pod.AutomountServiceAccountToken == nil || *pod.AutomountServiceAccountToken || pod.EnableServiceLinks == nil || *pod.EnableServiceLinks || pod.ServiceAccountName != "" || pod.HostNetwork || len(container.EnvFrom) != 0 {
		t.Fatalf("validator pod has publishing credentials or network privileges: %+v", pod)
	}
	if job.Spec.Template.Labels["sdo.dev/validator-isolation"] != policy.Name || len(pod.Volumes) != 2 || pod.Volumes[0].PersistentVolumeClaim == nil || !pod.Volumes[0].PersistentVolumeClaim.ReadOnly {
		t.Fatal("validator is not isolated by the policy and read-only PVC")
	}
	if mount := container.VolumeMounts[0]; mount.MountPath != "/workspace" || mount.SubPath != "accepted" || !mount.ReadOnly {
		t.Fatalf("validator received the wrong candidate mount: %+v", mount)
	}
	if container.SecurityContext == nil || container.SecurityContext.ReadOnlyRootFilesystem == nil || !*container.SecurityContext.ReadOnlyRootFilesystem || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
		t.Fatal("validator container is not restricted")
	}
	if len(container.Command) != 4 || !strings.Contains(container.Command[3], "GIT_INDEX_FILE=") || !strings.Contains(container.Command[3], "GIT_OBJECT_DIRECTORY=") || !strings.Contains(container.Command[3], "sdo-operator validate --app /workspace") {
		t.Fatalf("validator does not bind tree and compile detectors: %v", container.Command)
	}
	if !hasEnv(container.Env, "SDO_CANDIDATE_TREE") || !hasEnv(container.Env, "SDO_SCHEMA_IDENTITY") {
		t.Fatal("validator lacks tree or schema binding")
	}
}

func TestKubernetesValidatorFailsClosedWhenNetworkControlFails(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	var jobs int
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		jobs++
		job.Status.Failed = 1
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
			t.Fatal(err)
		}
		return true, job, nil
	})
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root), SchemaIdentity: "schema", PollInterval: time.Millisecond}
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err == nil {
		t.Fatal("failed network positive control accepted")
	}
	if jobs != 1 {
		t.Fatalf("detector ran after a failed network control: %d jobs", jobs)
	}
}

func TestKubernetesValidatorRejectsCandidateOutsidePVC(t *testing.T) {
	root := repository(t)
	launcher := KubernetesValidationJobClient{Client: fake.NewSimpleClientset(), Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Join(t.TempDir(), "other"), SchemaIdentity: "schema"}
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err == nil {
		t.Fatal("candidate outside PVC accepted")
	}
}

func hasEnv(env []corev1.EnvVar, name string) bool {
	for _, variable := range env {
		if variable.Name == name && variable.Value != "" {
			return true
		}
	}
	return false
}

func TestKubernetesValidatorReturnsTypedContentFailure(t *testing.T) {
	for _, rule := range []string{"detector.compile", "detector.test"} {
		t.Run(rule, func(t *testing.T) {
			const jobName = "sdo-validator-content"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/apis/batch/v1/namespaces/sdo/jobs/" + jobName:
					_ = json.NewEncoder(w).Encode(batchv1.Job{Status: batchv1.JobStatus{Failed: 1}})
				case "/api/v1/namespaces/sdo/pods":
					_ = json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: jobName + "-pod"}}}})
				case "/api/v1/namespaces/sdo/pods/" + jobName + "-pod/log":
					_, _ = w.Write([]byte(`SDO_MEMORY_REPORT:{"diagnostics":[{"rule":"` + rule + `","path":".sdo/diagnostics/detectors/incidents/cache/detector.go","line":4,"message":"undefined: missing"}]}` + "\n"))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", PollInterval: time.Millisecond}
			err = launcher.waitJob(context.Background(), jobName)
			ds, typed := RejectionDiagnostics(err)
			if !typed || len(ds) != 1 || ds[0].Rule != rule || ds[0].Line != 4 {
				t.Fatalf("lost typed failure: %v %v %v", typed, ds, err)
			}
		})
	}
}
