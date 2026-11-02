package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sdo.dev/operator/internal/testutil"
	"strings"
	"testing"
	"time"

	"sdo.dev/operator/memory/validate"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	if strings.Join(phases, ",") != "validator_job_started,validator_job_finished" {
		t.Fatalf("validator completion phases: %v", phases)
	}
}

func TestKubernetesValidatorLogsToolchainStageTimings(t *testing.T) {
	var out bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, nil)))
	defer slog.SetDefault(previous)
	logs := "noise\n" + validate.StageReportPrefix + "build ./...\t4200\n" + validate.StageReportPrefix + "test ./...\t31000\n" + validate.StageReportPrefix + "malformed\n"
	logStages("sdo-validator-x", logs)
	for _, want := range []string{
		"msg=validator_stage job=sdo-validator-x stage=\"build ./...\" duration_ms=4200",
		"msg=validator_stage job=sdo-validator-x stage=\"test ./...\" duration_ms=31000",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("stage log lacks %q:\n%s", want, out.String())
		}
	}
	if strings.Count(out.String(), "validator_stage") != 2 {
		t.Fatalf("malformed stage line was logged:\n%s", out.String())
	}
}

func TestKubernetesValidatorTreeCheckUsesScratchWithReadOnlyCandidate(t *testing.T) {
	repository := t.TempDir()
	scratch := t.TempDir()
	testutil.InitGit(t, repository)
	for _, args := range [][]string{{"-C", repository, "config", "user.email", "test@example.invalid"}} {
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
		// The apiserver defaults nested init containers; client-go's Job
		// defaulter does not. These are the observed v1.34 defaults.
		for i := range admitted.Spec.Template.Spec.InitContainers {
			admitted.Spec.Template.Spec.InitContainers[i].TerminationMessagePath = "/dev/termination-log"
			admitted.Spec.Template.Spec.InitContainers[i].TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		admitted.Spec.Template.Labels["batch.kubernetes.io/controller-uid"] = "job-uid"
		admitted.Spec.Template.Labels["batch.kubernetes.io/job-name"] = admitted.Name
		admitted.Spec.Template.Labels["controller-uid"] = "job-uid"
		admitted.Spec.Template.Labels["job-name"] = admitted.Name
		return true, admitted, nil
	})
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", ToolchainImage: "controller:compiler", RepositoryPVC: "repository"}
	job := launcher.canaryJob("sdo-validator-allow", false)
	if err := launcher.createJob(context.Background(), job); err != nil {
		t.Fatalf("known API Job defaults rejected: %v", err)
	}
	job = launcher.validationJob("sdo-validator-compile", "worktrees/candidate", strings.Repeat("a", 40), false)
	if err := launcher.createJob(context.Background(), job); err != nil {
		t.Fatalf("known compiler init defaults rejected: %v", err)
	}
}

func TestKubernetesValidatorCanaryUsesGoOperator(t *testing.T) {
	launcher := KubernetesValidationJobClient{Namespace: "sdo", Image: "validator:test"}
	for _, denied := range []bool{false, true} {
		job := launcher.canaryJob("sdo-validator-canary", denied)
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

// Canary process execution has its own bounded network attempts. Its Job must
// allow the enclosing validation budget for scheduling and status reconciliation.
func TestKubernetesValidatorCanaryJobCannotExpireBeforeValidationBudget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		budget  time.Duration
	}{
		{"default", 0, 10 * time.Minute},
		{"negative-default", -time.Second, 10 * time.Minute},
		{"custom", 2 * time.Minute, 2 * time.Minute},
		{"fractional", 45*time.Second + time.Nanosecond, 45*time.Second + time.Nanosecond},
		{"subsecond", time.Nanosecond, time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			launcher := KubernetesValidationJobClient{Timeout: tc.timeout}
			for _, denied := range []bool{false, true} {
				job := launcher.canaryJob("sdo-validator-canary", denied)
				if job.Spec.ActiveDeadlineSeconds == nil {
					t.Fatal("canary Job must remain bounded after its owner exits")
				}
				lifetime := time.Duration(*job.Spec.ActiveDeadlineSeconds) * time.Second
				if lifetime < tc.budget || lifetime-tc.budget >= time.Second {
					t.Fatalf("canary Job lifetime %v preempts validation budget %v or exceeds its rounding allowance", lifetime, tc.budget)
				}
			}
		})
	}
}

// Replay the lane-50 failure class with virtual cluster time: the probe exits
// at 16s but the Job controller observes terminal Pod status only at 45s.
func TestSimScheduleValidatorCanaryTerminalStatusLag(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	var jobs []string
	reads := map[string]int{}
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		jobs = append(jobs, action.(ktesting.CreateAction).GetObject().(*batchv1.Job).Name)
		return false, nil, nil
	})
	client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		name := action.(ktesting.GetAction).GetName()
		object, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), "sdo", name)
		if err != nil {
			return true, nil, err
		}
		job := object.(*batchv1.Job).DeepCopy()
		reads[name]++
		isCanary := job.Spec.Template.Labels["sdo.dev/validator-canary"] != ""
		if isCanary && reads[name] < 3 {
			job.Status.Active = 1
			return true, job, nil
		}
		completed := metav1.NewTime(time.Unix(45, 0))
		if isCanary && *job.Spec.ActiveDeadlineSeconds <= 45 {
			job.Status.Failed = 1
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}
		} else {
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &completed
		}
		return true, job, nil
	})
	proof := &IsolationProof{}
	launcher := KubernetesValidationJobClient{
		Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root),
		SchemaIdentity: "schema", PollInterval: time.Millisecond, IsolationProof: proof,
	}
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err != nil {
		t.Fatalf("successful canary rejected during delayed Job reconciliation: %v", err)
	}
	if len(jobs) != 3 || reads[jobs[0]] != 3 || reads[jobs[1]] != 3 || !proof.valid("validator:test", time.Minute, time.Now()) {
		t.Fatalf("both terminal canary proofs must precede validator execution: jobs=%v reads=%v", jobs, reads)
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
	launcher := KubernetesValidationJobClient{RepositoryPVC: "repository", ToolchainImage: "controller:compiler"}
	job := launcher.validationJob("sdo-validator-test", "worktrees/candidate", strings.Repeat("a", 40), false)
	if len(job.Spec.Template.Spec.InitContainers) != 0 {
		t.Fatal("validator must use its immutable image compiler without startup copying")
	}
	env := map[string]string{}
	for _, entry := range job.Spec.Template.Spec.Containers[0].Env {
		env[entry.Name] = entry.Value
	}
	for name, want := range map[string]string{
		"GOMODCACHE":  "/go/pkg/mod",
		"GOCACHEPROG": "/usr/local/bin/sdo-go-cache",
		"GOPROXY":     "off",
		"GOFLAGS":     "-mod=mod -p=2",
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
	if err := launcher.createJob(context.Background(), launcher.canaryJob("sdo-validator-allow", false)); err == nil {
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
	for _, failing := range []string{"allow", "deny"} {
		t.Run(failing, func(t *testing.T) {
			root := repository(t)
			client := fake.NewSimpleClientset()
			var validators int
			client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
				job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
				canary := job.Spec.Template.Labels["sdo.dev/validator-canary"]
				if canary == "" {
					validators++
				}
				if canary == failing {
					job.Status.Failed = 1
				} else {
					now := metav1.Now()
					job.Status.Succeeded, job.Status.CompletionTime = 1, &now
				}
				if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
					t.Fatal(err)
				}
				return true, job, nil
			})
			launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root), SchemaIdentity: "schema", PollInterval: time.Millisecond}
			if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err == nil {
				t.Fatalf("failed %s network control accepted", failing)
			}
			if validators != 0 {
				t.Fatalf("detector ran after a failed %s network control", failing)
			}
		})
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

func TestJobValidatorSkipsTheJobWhenNothingExecutableChanged(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	jobs := 0
	client.PrependReactor("create", "jobs", func(ktesting.Action) (bool, runtime.Object, error) {
		jobs++
		return true, nil, errors.New("no Job may run")
	})
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root), SchemaIdentity: "schema", PollInterval: time.Millisecond}
	if err := JobValidator(launcher)(context.Background(), root, []string{".sdo/arch.md", ".sdo/playbooks/cache/README.md"}); err != nil {
		t.Fatalf("prose-only candidate: %v", err)
	}
	if jobs != 0 {
		t.Fatalf("prose-only candidate created %d Jobs", jobs)
	}
	if err := JobValidator(nil)(context.Background(), root, []string{".sdo/arch.md"}); err == nil {
		t.Fatal("a missing launcher must still fail closed for prose-only candidates")
	}
	if err := JobValidator(launcher)(context.Background(), root, []string{".sdo/corpus/cache/open.json"}); err == nil || jobs == 0 {
		t.Fatalf("a corpus change must run the Job: err=%v jobs=%d", err, jobs)
	}
}

// succeedingJobs completes every created Job and records its name.
func succeedingJobs(t *testing.T, client *fake.Clientset) *[]string {
	t.Helper()
	var created []string
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		created = append(created, job.Name)
		now := metav1.Now()
		job.Status.Succeeded = 1
		job.Status.CompletionTime = &now
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
			t.Fatal(err)
		}
		return true, job, nil
	})
	return &created
}

func TestKubernetesValidatorProvesNetworkIsolationOncePerImageUntilExpiry(t *testing.T) {
	root := repository(t)
	newClient := func(image string, ttl time.Duration, proof *IsolationProof, client *fake.Clientset) Validator {
		return JobValidator(KubernetesValidationJobClient{
			Client: client, Namespace: "sdo", Image: image, RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root),
			SchemaIdentity: "schema", PollInterval: time.Millisecond, IsolationProof: proof, IsolationProofTTL: ttl,
		})
	}
	paths := []string{"app.txt"}
	client := fake.NewSimpleClientset()
	created := succeedingJobs(t, client)
	proof := &IsolationProof{}
	validate := newClient("validator:a", time.Hour, proof, client)
	for run := 0; run < 2; run++ {
		if err := validate(context.Background(), root, paths); err != nil {
			t.Fatal(err)
		}
	}
	canaries := 0
	for _, name := range *created {
		if strings.HasSuffix(name, "-allow") || strings.HasSuffix(name, "-deny") {
			canaries++
		}
	}
	if len(*created) != 4 || canaries != 2 {
		t.Fatalf("second validation reran the canaries: %v", *created)
	}

	// A different validator image is a new thing to prove.
	*created = nil
	if err := newClient("validator:b", time.Hour, proof, client)(context.Background(), root, paths); err != nil || len(*created) != 3 {
		t.Fatalf("new image skipped the proof: err=%v jobs=%v", err, *created)
	}

	// An expired proof is rerun against the existing policy.
	*created = nil
	if err := newClient("validator:b", time.Nanosecond, proof, client)(context.Background(), root, paths); err != nil || len(*created) != 3 {
		t.Fatalf("expired proof was reused: err=%v jobs=%v", err, *created)
	}

	// A policy removed behind the proof is noticed on the next validation.
	if err := client.NetworkingV1().NetworkPolicies("sdo").Delete(context.Background(), validatorIsolationPolicy, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	*created = nil
	if err := newClient("validator:b", time.Hour, proof, client)(context.Background(), root, paths); err != nil || len(*created) != 3 {
		t.Fatalf("missing policy was not re-proven: err=%v jobs=%v", err, *created)
	}
	if _, err := client.NetworkingV1().NetworkPolicies("sdo").Get(context.Background(), validatorIsolationPolicy, metav1.GetOptions{}); err != nil {
		t.Fatalf("policy not restored: %v", err)
	}
}

func TestKubernetesValidatorDoesNotRecordAFailedProof(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		job.Status.Failed = 1
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
			t.Fatal(err)
		}
		return true, job, nil
	})
	proof := &IsolationProof{}
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root), SchemaIdentity: "schema", PollInterval: time.Millisecond, IsolationProof: proof}
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err == nil {
		t.Fatal("failed canary accepted")
	}
	if proof.valid("validator:test", time.Hour, time.Now()) {
		t.Fatal("a failed canary was recorded as proof")
	}
}

func TestKubernetesValidatorRepairsADriftedPolicyInPlaceAndFailsClosedWithoutOne(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	created := succeedingJobs(t, client)
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root), SchemaIdentity: "schema", PollInterval: time.Millisecond}
	drifted := validatorPolicy("sdo")
	drifted.Spec.PolicyTypes = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
	if _, err := client.NetworkingV1().NetworkPolicies("sdo").Create(context.Background(), drifted, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	deletes := 0
	client.PrependReactor("delete", "networkpolicies", func(ktesting.Action) (bool, runtime.Object, error) {
		deletes++
		return false, nil, nil
	})
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err != nil {
		t.Fatal(err)
	}
	current, err := client.NetworkingV1().NetworkPolicies("sdo").Get(context.Background(), validatorIsolationPolicy, metav1.GetOptions{})
	if err != nil || len(current.Spec.PolicyTypes) != 2 || deletes != 0 {
		t.Fatalf("drifted policy was not updated in place: %+v err=%v deletes=%d", current, err, deletes)
	}
	if len(*created) != 3 {
		t.Fatalf("jobs = %v", *created)
	}

	// The validator never starts when the policy disappears after the proof.
	removed := fake.NewSimpleClientset()
	succeedingJobs(t, removed)
	proof := &IsolationProof{}
	proof.record("validator:test", time.Now())
	launcher.Client, launcher.IsolationProof = removed, proof
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err != nil {
		t.Fatalf("missing policy should be re-proven, not accepted blindly: %v", err)
	}
	removed.PrependReactor("get", "networkpolicies", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(networkingv1.Resource("networkpolicies"), validatorIsolationPolicy)
	})
	if err := launcher.requirePolicy(context.Background()); err == nil {
		t.Fatal("validator may start without the default-deny policy")
	}
}

func TestKubernetesValidatorUsesFrozenControllerBaselineWithoutLegacyOptIn(t *testing.T) {
	root := repository(t)
	baseline := filepath.Join(filepath.Dir(root), ".sdo-runtime", "sessions", "fixture", "corpus", "healthy")
	if err := os.MkdirAll(baseline, 0700); err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset()
	var validated *batchv1.Job
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		job := action.(ktesting.CreateAction).GetObject().(*batchv1.Job).DeepCopy()
		if strings.HasPrefix(job.Name, "sdo-validator-") && !strings.Contains(job.Name, "canary") {
			validated = job.DeepCopy()
		}
		now := metav1.Now()
		job.Status.Succeeded = 1
		job.Status.CompletionTime = &now
		if err := client.Tracker().Create(batchv1.SchemeGroupVersion.WithResource("jobs"), job, "sdo"); err != nil {
			t.Fatal(err)
		}
		return true, job, nil
	})
	launcher := KubernetesValidationJobClient{Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root), RepositoryPVCSubPath: "tenant", SchemaIdentity: "schema", PollInterval: time.Millisecond}
	ctx := context.WithValue(context.Background(), compileBaselineKey{}, baseline)
	if err := JobValidator(launcher)(ctx, root, []string{"app.txt"}); err != nil {
		t.Fatal(err)
	}
	if validated == nil {
		t.Fatal("validator Job missing")
	}
	found := false
	for _, mount := range validated.Spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.MountPath == "/sdo/healthy-baseline" {
			found = true
			if mount.Name != "application" || !mount.ReadOnly || mount.SubPath != "tenant/.sdo-runtime/sessions/fixture/corpus/healthy" {
				t.Fatalf("wrong baseline projection: %+v", mount)
			}
		}
	}
	if !found {
		t.Fatal("frozen controller baseline not mounted")
	}
	candidate := validated.Spec.Template.Spec.Containers[0].VolumeMounts[0]
	if candidate.Name != "application" || !candidate.ReadOnly || !strings.HasPrefix(candidate.SubPath, "tenant/") || candidate.MountPath != "/workspace" || candidate.SubPath == "tenant/.sdo-runtime/sessions/fixture/corpus/healthy" {
		t.Fatalf("baseline override changed the candidate projection: %+v", candidate)
	}
}

// The allow and deny canaries are independent controls under one long-lived
// policy; each costs a Pod start, so they run together, and the timeline marks
// when they start so earlier closure work is never charged to them.
func TestKubernetesValidatorRunsCanariesConcurrentlyBetweenPhaseMarks(t *testing.T) {
	root := repository(t)
	client := fake.NewSimpleClientset()
	var created []string
	client.PrependReactor("create", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		created = append(created, action.(ktesting.CreateAction).GetObject().(*batchv1.Job).Name)
		return false, nil, nil
	})
	client.PrependReactor("get", "jobs", func(action ktesting.Action) (bool, runtime.Object, error) {
		name := action.(ktesting.GetAction).GetName()
		object, err := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("jobs"), "sdo", name)
		if err != nil {
			return true, nil, err
		}
		job := object.(*batchv1.Job).DeepCopy()
		// A canary completes only once both canaries exist at the same time.
		if job.Spec.Template.Labels["sdo.dev/validator-canary"] != "" && len(created) < 2 {
			job.Status.Active = 1
			return true, job, nil
		}
		now := metav1.Now()
		job.Status.Succeeded, job.Status.CompletionTime = 1, &now
		return true, job, nil
	})
	var phases []string
	launcher := KubernetesValidationJobClient{
		Client: client, Namespace: "sdo", Image: "validator:test", RepositoryPVC: "repository", RepositoryMountPath: filepath.Dir(root),
		SchemaIdentity: "schema", PollInterval: time.Millisecond, Timeout: 2 * time.Second,
		OnPhase: func(_ context.Context, phase string) error {
			phases = append(phases, phase)
			return nil
		},
	}
	if err := JobValidator(launcher)(context.Background(), root, []string{"app.txt"}); err != nil {
		t.Fatalf("canaries did not run concurrently: %v", err)
	}
	if len(phases) < 3 || phases[0] != "network_canaries_started" || phases[1] != "network_canaries_passed" || phases[2] != "validator_job_created" {
		t.Fatalf("canary phases are not marked at start and end: %v", phases)
	}
}
