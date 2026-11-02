package install

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func testConfig() Config {
	return Config{Namespace: "demo", ControlNamespace: "demo-sdo", Application: "demo", ControllerImage: "controller:test", ResponderImage: "responder:test", RepositoryPVC: "repository", CredentialsSecret: "credentials", Model: "gpt-test", AcceptedCommit: strings.Repeat("a", 40), TimeoutSeconds: 60, NativeCommand: []string{"/usr/local/bin/sdo-operator"}, BuildKitImage: "moby/buildkit@sha256:" + strings.Repeat("a", 64), RegistryPushSecret: "registry-push", RegistryPushDockerConfig: []byte("{}")}
}

func TestDevControllerUsesMountedSource(t *testing.T) {
	t.Setenv("SDO_DEV_MOUNT", "1")
	plan, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	container := plan.Job.Spec.Template.Spec.Containers[0]
	foundSource := map[string]bool{"/opt/sdo/sdo": false, "/opt/sdo/libs": false}
	foundBinary := false
	for _, mount := range container.VolumeMounts {
		if strings.Contains(mount.MountPath, "benchmarks") || strings.Contains(mount.MountPath, "scripted") {
			t.Fatalf("benchmark sources mounted in controller: %+v", mount)
		}
		if _, exists := foundSource[mount.MountPath]; exists {
			foundSource[mount.MountPath] = mount.ReadOnly
		}
		foundBinary = foundBinary || mount.MountPath == "/usr/local/bin/sdo-operator" && mount.ReadOnly
	}
	if !foundSource["/opt/sdo/sdo"] || !foundSource["/opt/sdo/libs"] || !foundBinary {
		t.Fatalf("dev controller mounts=%v env=%v", container.VolumeMounts, container.Env)
	}
}

func TestNativeJobSelectsBrokerAndProposalMountTogether(t *testing.T) {
	plan, err := Resources(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	job := plan.Job
	if job.Namespace != "demo-sdo" {
		t.Fatalf("controller namespace = %q", job.Namespace)
	}
	container := job.Spec.Template.Spec.Containers[0]
	if !contains(container.Args, "--broker-mode=native") || !contains(container.Args, "--worker-mount-mode=proposal-only") {
		t.Fatalf("native modes missing: %v", container.Args)
	}
	if valueAfter(container.Args, "--model") != "gpt-test" || valueAfter(container.Args, "--responder-backend") != "codex" {
		t.Fatalf("native validator/model missing: %v", container.Args)
	}
	if contains(container.Args, "--broker-arg=-m") || contains(container.Args, "--worker-mount-mode=linked") {
		t.Fatalf("legacy mode leaked: %v", container.Args)
	}
	if len(container.VolumeMounts) == 0 || container.VolumeMounts[0].MountPath != "/workspace" {
		t.Fatal("broker does not own accepted checkout")
	}
}

func TestNativeRequiresEntrypointAndRejectsMixedArguments(t *testing.T) {
	cfg := testConfig()
	cfg.NativeCommand = nil
	if _, err := Resources(cfg); err == nil {
		t.Fatal("native mode accepted without entrypoint")
	}
	cfg.NativeCommand = []string{"/usr/local/bin/sdo-operator"}
	cfg.NativeArgs = []string{"--broker-mode=legacy"}
	if _, err := Resources(cfg); err == nil {
		t.Fatal("native mode accepted conflicting broker flag")
	}
}

func TestAcceptedCommitChangesInstallFingerprint(t *testing.T) {
	cfg := testConfig()
	first, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AcceptedCommit = strings.Repeat("b", 40)
	second, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if first.Job.Annotations[InstallFingerprintAnnotation] == second.Job.Annotations[InstallFingerprintAnnotation] {
		t.Fatal("new accepted commit reused stale controller")
	}
	cfg.AcceptedCommit = ""
	if _, err := Resources(cfg); err == nil {
		t.Fatal("native install accepted missing source provenance")
	}
}

func TestMaintenanceRequiresGenerationAndRecordsState(t *testing.T) {
	client := fake.NewSimpleClientset()
	if err := SetMaintenance(context.Background(), client, "demo-sdo", true, ""); err == nil {
		t.Fatal("accepted empty maintenance generation")
	}
	if err := SetMaintenance(context.Background(), client, "demo-sdo", true, "rollout-1"); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("demo-sdo").Get(context.Background(), MaintenanceConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Labels["app.kubernetes.io/managed-by"] != "sdo" {
		t.Fatal("maintenance record lacks SDO ownership")
	}
	if cm.Data["state"] != "paused" || cm.Data["generation"] != "rollout-1" {
		t.Fatalf("maintenance ConfigMap=%v", cm.Data)
	}
	if err := SetMaintenance(context.Background(), client, "demo-sdo", false, "rollout-2"); err != nil {
		t.Fatal(err)
	}
	cm, err = client.CoreV1().ConfigMaps("demo-sdo").Get(context.Background(), MaintenanceConfigMap, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if cm.Data["state"] != "active" || cm.Data["generation"] != "rollout-2" {
		t.Fatalf("resumed ConfigMap=%v", cm.Data)
	}
}

func TestMaintenancePreservesBaselineOnlyWhenExplicitlyRequested(t *testing.T) {
	ctx := context.Background()
	client := fake.NewSimpleClientset()
	if err := SetMaintenanceWithOptions(ctx, client, "demo-sdo", true, "barrier", MaintenanceOptions{PreserveBaseline: true}); err != nil {
		t.Fatal(err)
	}
	cm, err := client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, MaintenanceConfigMap, metav1.GetOptions{})
	if err != nil || cm.Data["preserve_baseline"] != "true" {
		t.Fatalf("observation barrier not retained: %v %v", cm, err)
	}
	if err := SetMaintenance(ctx, client, "demo-sdo", true, "redeploy"); err != nil {
		t.Fatal(err)
	}
	cm, err = client.CoreV1().ConfigMaps("demo-sdo").Get(ctx, MaintenanceConfigMap, metav1.GetOptions{})
	if err != nil || cm.Data["preserve_baseline"] != "" {
		t.Fatalf("redeploy inherited preservation: %v %v", cm, err)
	}
}

func TestPrepareSeedsInitialCheckoutWithoutStartingController(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	seeded := false
	i := Installer{Client: client, PollInterval: time.Millisecond}
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	err := i.Prepare(context.Background(), cfg, func(ctx context.Context, got Config) error {
		seeded = true
		if got.AcceptedCommit != cfg.AcceptedCommit {
			t.Fatal("prepare lost initial commit")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !seeded {
		t.Fatal("initial checkout was not seeded")
	}
	jobs, err := client.BatchV1().Jobs(cfg.ControlNamespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatal("prepare started controller before lifecycle accepted source")
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(cfg.ControlNamespace).Get(context.Background(), cfg.RepositoryPVC, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pvc.Annotations[PreparedCommitAnnotation] != cfg.AcceptedCommit {
		t.Fatalf("prepared PVC commit=%q", pvc.Annotations[PreparedCommitAnnotation])
	}
	if _, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(context.Background(), OperateLockConfigMap, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("prepare left PVC claim behind: %v", err)
	}
}

func TestPrepareDoesNotResetPVCUsedByActiveOperateJob(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.ControlNamespace}, Data: map[string]string{"operation_id": "operation-1"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: OperateJobName("operation-1"), Namespace: cfg.ControlNamespace}, Status: batchv1.JobStatus{Active: 1}},
	)
	i := Installer{Client: client}
	if err := i.Prepare(context.Background(), cfg, func(context.Context, Config) error {
		t.Fatal("prepare reset an active operate Job's PVC")
		return nil
	}); err == nil || !strings.Contains(err.Error(), "native operate PVC is already used") {
		t.Fatalf("prepare did not reject active operate Job: %v", err)
	}
}

func TestOperateJobUsesPreparedPVCAcceptedMountAndRegistrationConfigMap(t *testing.T) {
	cfg := testConfig()
	cfg.RegistrationConfigMap = "sdo-agents"
	cfg.RegistrationDirectory = "/sdo/registrations"
	job, err := OperateJob(cfg, "pods stay healthy", "operation-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if job.Spec.Template.Spec.ServiceAccountName != "sdo-operate" {
		t.Fatal("operate Job lacks its own deployment service account")
	}
	container := job.Spec.Template.Spec.Containers[0]
	if valueAfter(container.Args, "--health-objective") != "pods stay healthy" || valueAfter(container.Args, "--operation-id") != "operation-1" {
		t.Fatalf("operate args=%v", container.Args)
	}
	if valueAfter(container.Args, "--agent-registration-directory") != "/sdo/registrations" {
		t.Fatal("registration directory not passed")
	}
	if !containsMount(container.VolumeMounts, "repository", "/workspace") || !containsMount(container.VolumeMounts, "registrations", "/sdo/registrations") {
		t.Fatalf("operate mounts=%v", container.VolumeMounts)
	}
	if len(job.Spec.Template.Spec.Volumes) < 3 {
		t.Fatalf("operate volumes=%v", job.Spec.Template.Spec.Volumes)
	}
	plan, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	controller := plan.Job.Spec.Template.Spec.Containers[0]
	if valueAfter(controller.Args, "--agent-registration-directory") != "/sdo/registrations" || !containsMount(controller.VolumeMounts, "registrations", "/sdo/registrations") {
		t.Fatal("controller lost the same registration directory")
	}
}

func TestStartOperateRequiresPreparedCommitAndCreatesJob(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset(&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cfg.RepositoryPVC, Namespace: cfg.ControlNamespace}})
	i := Installer{Client: client}
	if _, err := i.StartOperate(context.Background(), cfg, "healthy", "operation-1", 2); err == nil {
		t.Fatal("operate started on unprepared PVC")
	}
	pvc, err := client.CoreV1().PersistentVolumeClaims(cfg.ControlNamespace).Get(context.Background(), cfg.RepositoryPVC, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pvc.Annotations = map[string]string{PreparedCommitAnnotation: cfg.AcceptedCommit}
	if _, err := client.CoreV1().PersistentVolumeClaims(cfg.ControlNamespace).Update(context.Background(), pvc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := i.StartOperate(context.Background(), cfg, "healthy", "operation-1", 2); err == nil {
		t.Fatal("operate started without registry push Secret")
	}
	_, err = client.CoreV1().Secrets(cfg.ControlNamespace).Create(context.Background(), &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.RegistryPushSecret, Namespace: cfg.ControlNamespace}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte("{}")}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	name, err := i.StartOperate(context.Background(), cfg, "healthy", "operation-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if name != OperateJobName("operation-1") {
		t.Fatalf("Job name=%q", name)
	}
	if _, err := client.BatchV1().Jobs(cfg.ControlNamespace).Get(context.Background(), name, metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := i.StartOperate(context.Background(), cfg, "healthy", "operation-2", 2); err == nil {
		t.Fatal("second operate Job started on the same repository PVC")
	}
}

func TestStartOperateReclaimsExpiredClaimAfterJobTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		client := fake.NewSimpleClientset(
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: cfg.RepositoryPVC, Namespace: cfg.ControlNamespace, Annotations: map[string]string{PreparedCommitAnnotation: cfg.AcceptedCommit}}},
			&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.RegistryPushSecret, Namespace: cfg.ControlNamespace}, Type: corev1.SecretTypeDockerConfigJson, Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte("{}")}},
			&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.ControlNamespace, CreationTimestamp: metav1.NewTime(time.Now().Add(-20 * time.Minute))}, Data: map[string]string{"operation_id": "expired-operation"}},
		)
		i := Installer{Client: client}
		if _, err := i.StartOperate(context.Background(), cfg, "healthy", "operation-2", 2); err != nil {
			t.Fatalf("expired claim prevented new operation: %v", err)
		}
	})
}

func TestWaitOperateReleasesClaimAfterFailedJob(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset(
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.ControlNamespace}, Data: map[string]string{"operation_id": "failed-operation"}},
		&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: OperateJobName("failed-operation"), Namespace: cfg.ControlNamespace}, Status: batchv1.JobStatus{Failed: 1}},
	)
	i := Installer{Client: client}
	if _, err := i.WaitOperate(context.Background(), cfg, "failed-operation"); err == nil {
		t.Fatal("failed operate Job reported success")
	}
	if _, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(context.Background(), OperateLockConfigMap, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed operate Job kept PVC claim: %v", err)
	}
}

func TestBuilderIdentityAndRegistryCredentialsStaySeparate(t *testing.T) {
	cfg := testConfig()
	plan, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !hasServiceAccount(plan.ServiceAccounts, "sdo-image-builder") {
		t.Fatal("missing unprivileged image builder identity")
	}
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	i := Installer{Client: client, PollInterval: time.Millisecond}
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	if err := i.Prepare(context.Background(), cfg, func(context.Context, Config) error { return nil }); err != nil {
		t.Fatal(err)
	}
	secret, err := client.CoreV1().Secrets(cfg.ControlNamespace).Get(context.Background(), cfg.RegistryPushSecret, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if secret.Type != corev1.SecretTypeDockerConfigJson || string(secret.Data[corev1.DockerConfigJsonKey]) != "{}" {
		t.Fatal("registry Secret lacks dedicated Docker credentials")
	}
	if secret.Name == cfg.CredentialsSecret {
		t.Fatal("registry and agent credentials share a Secret")
	}
}

func hasServiceAccount(accounts []*corev1.ServiceAccount, name string) bool {
	for _, account := range accounts {
		if account.Name == name {
			return true
		}
	}
	return false
}
func TestReuseRequiresMatchingActiveJobAndDoesNotSeed(t *testing.T) {
	cfg := testConfig()
	cfg.ReuseExisting = true
	plan, err := Resources(cfg)
	if err != nil {
		t.Fatal(err)
	}
	job := plan.Job.DeepCopy()
	job.Status.Active = 1
	client := fake.NewSimpleClientset(job, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	seeded := 0
	i := Installer{Client: client, SeedRepository: func(context.Context, Config) error { seeded++; return nil }}
	result, err := i.Install(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Reused || seeded != 0 {
		t.Fatalf("result=%+v seeded=%d", result, seeded)
	}
}

func TestSyncExistingRepositoryUsesTemporaryPodWithoutReplacingController(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset()
	synced := false
	installer := Installer{Client: client, PollInterval: time.Millisecond, SyncTimeout: time.Second, SyncRepository: func(context.Context, Config) error {
		synced = true
		pod, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(context.Background(), RepositorySyncPodName, metav1.GetOptions{})
		if err != nil || pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != cfg.RepositoryPVC {
			t.Fatalf("sync callback did not receive repository PVC Pod: %v %#v", err, pod)
		}
		return nil
	}}
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	if err := installer.SyncExistingRepository(context.Background(), SyncConfig{ControlNamespace: cfg.ControlNamespace, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}); err != nil {
		t.Fatal(err)
	}
	if !synced {
		t.Fatal("repository synchronization did not run")
	}
	if _, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(context.Background(), RepositorySyncPodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("temporary sync Pod was not deleted: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetResource().Resource == "jobs" {
			t.Fatalf("sync changed controller Job: %s", action.GetVerb())
		}
	}
}

func TestWithSyncPodCleansUpAfterReadFailure(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset()
	installer := Installer{Client: client, PollInterval: time.Millisecond, SyncTimeout: time.Second}
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	want := errors.New("read failed")
	err := installer.WithSyncPod(context.Background(), SyncConfig{ControlNamespace: cfg.ControlNamespace, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}, func(context.Context, Config) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("read error was lost: %v", err)
	}
	if _, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(context.Background(), RepositorySyncPodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("sync Pod survived failed read: %v", err)
	}
}

func TestFreshInstallRequiresSeederBeforeControllerJob(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	i := Installer{Client: client}
	if _, err := i.Install(context.Background(), cfg); err == nil {
		t.Fatal("fresh install accepted without repository seeder")
	}
	if jobs, _ := client.BatchV1().Jobs(cfg.ControlNamespace).List(context.Background(), metav1.ListOptions{}); len(jobs.Items) != 0 {
		t.Fatal("job created before repository seeding")
	}
}

func TestFreshInstallSeedsBeforeStartingJob(t *testing.T) {
	cfg := testConfig()
	client := fake.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	seeded := false
	i := Installer{Client: client, PollInterval: time.Millisecond, SeedRepository: func(ctx context.Context, cfg Config) error {
		seeded = true
		jobs, err := client.BatchV1().Jobs(cfg.ControlNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		if len(jobs.Items) != 0 {
			t.Fatal("controller Job started before accepted checkout was seeded")
		}
		return nil
	}}
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	result, err := i.Install(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Reused || !seeded {
		t.Fatalf("result=%+v seeded=%v", result, seeded)
	}
	job, err := client.BatchV1().Jobs(cfg.ControlNamespace).Get(context.Background(), ControllerJobName, metav1.GetOptions{})
	if err != nil || job.Annotations[InstallFingerprintAnnotation] == "" {
		t.Fatalf("controller Job missing: %v", err)
	}
}

func TestReplacementStopsOldControllerBeforeReseedingPVC(t *testing.T) {
	cfg := testConfig()
	old := cfg
	old.AcceptedCommit = strings.Repeat("b", 40)
	oldPlan, err := Resources(old)
	if err != nil {
		t.Fatal(err)
	}
	oldPlan.Job.Status.Active = 1
	client := fake.NewSimpleClientset(oldPlan.Job, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: cfg.CredentialsSecret, Namespace: cfg.ControlNamespace}})
	i := Installer{Client: client, PollInterval: time.Millisecond, SeedRepository: func(ctx context.Context, cfg Config) error {
		if _, err := client.BatchV1().Jobs(cfg.ControlNamespace).Get(ctx, ControllerJobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatal("old controller still owns PVC during seed")
		}
		return nil
	}}
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	if _, err := i.Install(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
}

// The fake must not report readiness before activation releases the scheduling
// gate: a concurrent status write can otherwise be lost by the fake tracker.
func TestSyncPodReadinessFollowsActivation(t *testing.T) {
	cfg := testConfig()
	ctx := context.Background()
	pod := syncPod(cfg)
	pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "sdo.dev/pvc-claim"}}
	client := fake.NewSimpleClientset(pod)
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	current, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if podReady(current.Status.Conditions) {
		t.Fatal("fake made a gated sync Pod ready before activation")
	}
	current.Spec.SchedulingGates = nil
	if _, err := client.CoreV1().Pods(cfg.ControlNamespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	current, err = client.CoreV1().Pods(cfg.ControlNamespace).Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil || !podReady(current.Status.Conditions) {
		t.Fatalf("activated sync Pod is not ready: %v %+v", err, current)
	}
}

func TestSyncPodReadinessIgnoresUnrelatedUpdates(t *testing.T) {
	cfg := testConfig()
	for _, tc := range []struct {
		name, namespace, podName string
		gated, statusUpdate      bool
	}{
		{name: "other-namespace", namespace: "other", podName: RepositorySyncPodName},
		{name: "other-pod", namespace: cfg.ControlNamespace, podName: "unrelated"},
		{name: "still-gated", namespace: cfg.ControlNamespace, podName: RepositorySyncPodName, gated: true},
		{name: "status-only", namespace: cfg.ControlNamespace, podName: RepositorySyncPodName, statusUpdate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: tc.namespace, Name: tc.podName}}
			if tc.gated {
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "sdo.dev/pvc-claim"}}
			}
			client := fake.NewSimpleClientset(pod)
			readySyncPodOnActivation(client, cfg.ControlNamespace)
			pods := client.CoreV1().Pods(tc.namespace)
			var updated *corev1.Pod
			var err error
			if tc.statusUpdate {
				updated, err = pods.UpdateStatus(context.Background(), pod, metav1.UpdateOptions{})
			} else {
				updated, err = pods.Update(context.Background(), pod, metav1.UpdateOptions{})
			}
			if err != nil {
				t.Fatal(err)
			}
			if podReady(updated.Status.Conditions) {
				t.Fatal("fake granted readiness to an update unrelated to sync activation")
			}
		})
	}
}

// readySyncPodOnActivation models scheduling only after the claim gate is
// removed. React synchronously to activation so the fake tracker cannot lose a
// goroutine's status write to an older activation snapshot. Do not call client
// methods from a reactor: client-go holds its fake lock while invoking it.
func readySyncPodOnActivation(client *fake.Clientset, namespace string) {
	client.PrependReactor("update", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		pod := action.(ktesting.UpdateAction).GetObject().(*corev1.Pod)
		if action.GetSubresource() == "" && action.GetNamespace() == namespace && pod.Name == RepositorySyncPodName && len(pod.Spec.SchedulingGates) == 0 {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
		return false, nil, nil
	})
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func valueAfter(values []string, flag string) string {
	for i, value := range values {
		if value == flag && i+1 < len(values) {
			return values[i+1]
		}
	}
	return ""
}
func containsMount(mounts []corev1.VolumeMount, name, path string) bool {
	for _, mount := range mounts {
		if mount.Name == name && mount.MountPath == path {
			return true
		}
	}
	return false
}

func TestSeederRejectsCommitDifferentFromInstallFingerprint(t *testing.T) {
	cfg := testConfig()
	seeder := KubectlTarSeeder("/tmp/repository", strings.Repeat("b", 40))
	if err := seeder(context.Background(), cfg); err == nil {
		t.Fatal("seeder accepted a commit different from install config")
	}
}

// The export Pod is created ungated, so the fake marks it ready at creation.
func readyExportPodOnCreate(client *fake.Clientset, namespace string) {
	client.PrependReactor("create", "pods", func(action ktesting.Action) (bool, kruntime.Object, error) {
		pod := action.(ktesting.CreateAction).GetObject().(*corev1.Pod)
		if action.GetNamespace() == namespace && pod.Name == RuntimeExportPodName {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
		return false, nil, nil
	})
}

func TestRuntimeExportPodCoexistsWithRepositorySync(t *testing.T) {
	cfg := testConfig()
	stale := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: RuntimeExportPodName, Namespace: cfg.ControlNamespace, UID: "crashed-delivery"}}
	client := fake.NewSimpleClientset(stale)
	readySyncPodOnActivation(client, cfg.ControlNamespace)
	readyExportPodOnCreate(client, cfg.ControlNamespace)
	installer := Installer{Client: client, PollInterval: time.Millisecond, SyncTimeout: time.Second}
	sync := SyncConfig{ControlNamespace: cfg.ControlNamespace, RepositoryPVC: cfg.RepositoryPVC, ControllerImage: cfg.ControllerImage}
	synced := false
	err := installer.WithRuntimeExportPod(context.Background(), sync, func(ctx context.Context, _ Config) error {
		pod, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(ctx, RuntimeExportPodName, metav1.GetOptions{})
		if err != nil || pod.UID == stale.UID || pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != cfg.RepositoryPVC {
			t.Fatalf("export callback did not receive a fresh repository PVC Pod: %v %#v", err, pod)
		}
		// The next row's accepted-memory synchronization must not wait for delivery.
		return installer.WithSyncPod(ctx, sync, func(context.Context, Config) error { synced = true; return nil })
	})
	if err != nil || !synced {
		t.Fatalf("repository sync blocked behind evidence delivery: synced=%v err=%v", synced, err)
	}
	if _, err := client.CoreV1().Pods(cfg.ControlNamespace).Get(context.Background(), RuntimeExportPodName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("export Pod survived delivery: %v", err)
	}
	if _, err := client.CoreV1().ConfigMaps(cfg.ControlNamespace).Get(context.Background(), OperateLockConfigMap, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("evidence delivery left the operate lock held: %v", err)
	}
}
