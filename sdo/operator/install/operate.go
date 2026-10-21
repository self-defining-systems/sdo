package install

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const PreparedCommitAnnotation = "sdo.dev/prepared-commit"
const OperateLockConfigMap = "sdo-operate-lock"

// Prepare creates the shared control resources and seeds the initial accepted
// repository before lifecycle workers need proposal checkouts on the PVC.
// It never starts a controller and refuses to reset an active controller's PVC.
func (i Installer) Prepare(ctx context.Context, cfg Config, seed RepositorySeeder) error {
	if i.Client == nil {
		return errors.New("Kubernetes client is required")
	}
	plan, err := Resources(cfg)
	if err != nil {
		return err
	}
	if seed == nil {
		return errors.New("prepare requires an initial accepted-repository seeder")
	}
	if prior, err := i.Client.BatchV1().Jobs(cfg.controlNamespace()).Get(ctx, ControllerJobName, metav1.GetOptions{}); err == nil && prior.Status.Active > 0 {
		return errors.New("cannot prepare repository while controller Job is active")
	} else if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("inspect controller Job: %w", err)
	}
	if err := i.ensureBase(ctx, cfg, plan); err != nil {
		return err
	}
	prepareOwner := "prepare-" + cfg.AcceptedCommit
	if err := i.acquireOperateLock(ctx, cfg, prepareOwner); err != nil {
		return err
	}
	defer func() { _ = i.releaseOperateLock(context.Background(), cfg, prepareOwner) }()
	if i.Extension != nil {
		if err := i.Extension.InstallResources(ctx, cfg, i.Client); err != nil {
			return fmt.Errorf("install transport resources: %w", err)
		}
		defer func() { _ = i.Extension.Cleanup(context.Background(), cfg) }()
		if err := i.Extension.WaitUntilReady(ctx, cfg); err != nil {
			return fmt.Errorf("wait for transport: %w", err)
		}
	}
	if err := i.ensureSyncPod(ctx, plan.SyncPod); err != nil {
		return err
	}
	defer func() {
		_ = i.Client.CoreV1().Pods(cfg.controlNamespace()).Delete(context.Background(), RepositorySyncPodName, metav1.DeleteOptions{})
	}()
	if err := i.waitSync(ctx, cfg.controlNamespace()); err != nil {
		return err
	}
	if err := seed(ctx, cfg); err != nil {
		return fmt.Errorf("seed initial accepted repository: %w", err)
	}
	return i.markPreparedCommit(ctx, cfg)
}

func (i Installer) markPreparedCommit(ctx context.Context, cfg Config) error {
	pvc, err := i.Client.CoreV1().PersistentVolumeClaims(cfg.controlNamespace()).Get(ctx, cfg.RepositoryPVC, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("inspect prepared repository PVC: %w", err)
	}
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[PreparedCommitAnnotation] = cfg.AcceptedCommit
	if _, err := i.Client.CoreV1().PersistentVolumeClaims(cfg.controlNamespace()).Update(ctx, pvc, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("record prepared repository commit: %w", err)
	}
	return nil
}

func OperateJobName(operationID string) string {
	hash := sha256.Sum256([]byte(operationID))
	return "sdo-operate-" + hex.EncodeToString(hash[:8])
}

// OperateJob runs the full native lifecycle inside the shared repository PVC.
// The Job owns the accepted checkout; delegated workers receive proposal mounts.
func OperateJob(cfg Config, healthObjective, operationID string, maxAttempts int) (*batchv1.Job, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.BuildKitImage == "" || cfg.RegistryPushSecret == "" {
		return nil, errors.New("native operate requires BuildKit image and dedicated registry push Secret")
	}
	if strings.TrimSpace(healthObjective) == "" || strings.TrimSpace(operationID) == "" || maxAttempts <= 0 {
		return nil, errors.New("operate requires objective, operation ID, and positive max attempts")
	}
	args := []string{
		"operate", "--app", "/workspace/application", "--namespace", cfg.Namespace,
		"--control-namespace", cfg.controlNamespace(), "--application", cfg.Application,
		"--health-objective", healthObjective, "--operation-id", operationID,
		"--max-attempts", fmt.Sprint(maxAttempts), "--controller-image", cfg.ControllerImage,
		"--responder-image", cfg.ResponderImage, "--validator-image", cfg.ValidatorImage,
		"--repository-pvc", cfg.RepositoryPVC, "--repository-mount-path", "/workspace",
		"--credentials-secret", cfg.CredentialsSecret, "--model", cfg.Model,
		"--agent-provider", provider(cfg), "--worktree-root", "/workspace/worktrees",
		"--buildkit-image", cfg.BuildKitImage, "--registry-push-secret", cfg.RegistryPushSecret,
		"--repair-policy", defaultRepairPolicy(cfg), "--response-timeout", fmt.Sprintf("%ds", cfg.TimeoutSeconds),
		"--verification-timeout", fmt.Sprintf("%ds", verificationTimeout(cfg)),
		"--reflection-session", reflectionSession(cfg),
		"--reflection-guidance", reflectionGuidance(cfg),
	}
	if cfg.ForceReflection {
		args = append(args, "--force-reflection")
	}
	if cfg.MaxFollowUps > 0 {
		args = append(args, "--max-follow-ups", fmt.Sprint(cfg.MaxFollowUps), "--follow-up-cooldown", fmt.Sprintf("%ds", followUpCooldown(cfg)))
	}
	if cfg.MaxResponseLoops > 0 {
		args = append(args, "--max-response-loops", fmt.Sprint(cfg.MaxResponseLoops))
	}
	if cfg.RegistryHost != "" {
		args = append(args, "--registry-host", cfg.RegistryHost)
	}
	for _, value := range cfg.ResponderEnv {
		args = append(args, "--responder-env", value)
	}
	if cfg.RegistryInsecure {
		args = append(args, "--registry-insecure")
	}
	if cfg.HealthyBaseline {
		args = append(args, "--healthy-baseline")
	}
	if cfg.ControllerDurationSeconds > 0 {
		args = append(args, "--controller-duration", fmt.Sprintf("%ds", cfg.ControllerDurationSeconds))
	}
	if cfg.ExitAfterClosure {
		args = append(args, "--exit-after-closure")
	}
	if cfg.RegistrationConfigMap != "" {
		args = append(args, "--agent-registration-configmap", cfg.RegistrationConfigMap, "--agent-registration-directory", cfg.registrationDirectory())
	}
	zero := int32(0)
	ttl := int32(600)
	deadline := int64(cfg.TimeoutSeconds*maxAttempts + 600)
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: OperateJobName(operationID), Namespace: cfg.controlNamespace(), Annotations: map[string]string{PreparedCommitAnnotation: cfg.AcceptedCommit}}, Spec: batchv1.JobSpec{BackoffLimit: &zero, TTLSecondsAfterFinished: &ttl, ActiveDeadlineSeconds: &deadline}}
	job.Spec.Template.Labels = map[string]string{"app.kubernetes.io/name": "sdo-operate", "sdo.dev/operation": OperateJobName(operationID)}
	pod := &job.Spec.Template.Spec
	pod.RestartPolicy = corev1.RestartPolicyNever
	pod.ServiceAccountName = "sdo-operate"
	pod.SecurityContext = podSecurity()
	pod.Volumes = []corev1.Volume{pvcVolume(cfg.RepositoryPVC), {Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: quantity("4Gi")}}}, {Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: cfg.CredentialsSecret}}}}
	if cfg.RegistrationConfigMap != "" {
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "registrations", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cfg.RegistrationConfigMap}}}})
	}
	container := corev1.Container{Name: "operate", Image: cfg.ControllerImage, ImagePullPolicy: corev1.PullIfNotPresent, Command: append([]string{}, cfg.NativeCommand...), Args: args, SecurityContext: containerSecurity(), Resources: limits("250m", "512Mi", "2", "4Gi"), EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: cfg.CredentialsSecret}}}}, Env: []corev1.EnvVar{{Name: "CODEX_HOME", Value: "/workspace/.sdo-runtime/codex"}, {Name: "CLAUDE_CONFIG_DIR", Value: "/workspace/.sdo-runtime/claude"}, {Name: "TMPDIR", Value: "/tmp"}, {Name: "GIT_AUTHOR_NAME", Value: "sdo-controller"}, {Name: "GIT_AUTHOR_EMAIL", Value: "sdo-controller@invalid"}, {Name: "GIT_COMMITTER_NAME", Value: "sdo-controller"}, {Name: "GIT_COMMITTER_EMAIL", Value: "sdo-controller@invalid"}}, VolumeMounts: []corev1.VolumeMount{{Name: "repository", MountPath: "/workspace"}, {Name: "scratch", MountPath: "/tmp"}, {Name: "credentials", MountPath: "/sdo/credentials", ReadOnly: true}}}
	if cfg.RegistrationConfigMap != "" {
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{Name: "registrations", MountPath: cfg.registrationDirectory(), ReadOnly: true})
	}
	pod.Containers = []corev1.Container{container}
	return job, nil
}
func provider(c Config) string {
	if c.AgentProvider != "" {
		return c.AgentProvider
	}
	return "codex"
}
func defaultRepairPolicy(c Config) string {
	if c.RepairPolicy != "" {
		return c.RepairPolicy
	}
	return "commit"
}
func reflectionSession(c Config) string {
	if c.ReflectionSession != "" {
		return c.ReflectionSession
	}
	return "resume"
}
func reflectionGuidance(c Config) string {
	if c.ReflectionGuidance != "" {
		return c.ReflectionGuidance
	}
	return "baseline"
}
func followUpCooldown(c Config) int {
	if c.FollowUpCooldownSeconds > 0 {
		return c.FollowUpCooldownSeconds
	}
	return 30
}

type OperateResult struct {
	OperationID        string `json:"operation_id"`
	GoalCommit         string `json:"goal_commit"`
	SchemaCommit       string `json:"schema_commit"`
	SourceCommit       string `json:"source_commit"`
	ArchitectureCommit string `json:"architecture_commit"`
	JudgeCommit        string `json:"judge_commit"`
	Attempts           int    `json:"attempts"`
	Installed          bool   `json:"installed"`
}

// StartOperate checks the prepared commit and starts one native lifecycle Job.
func (i Installer) StartOperate(ctx context.Context, cfg Config, objective, operationID string, maxAttempts int) (string, error) {
	if i.Client == nil {
		return "", errors.New("Kubernetes client is required")
	}
	job, err := OperateJob(cfg, objective, operationID, maxAttempts)
	if err != nil {
		return "", err
	}
	pvc, err := i.Client.CoreV1().PersistentVolumeClaims(cfg.controlNamespace()).Get(ctx, cfg.RepositoryPVC, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("inspect prepared repository PVC: %w", err)
	}
	if pvc.Annotations[PreparedCommitAnnotation] != cfg.AcceptedCommit {
		return "", errors.New("shared repository PVC is not seeded at the requested initial commit")
	}
	registrySecret, err := i.Client.CoreV1().Secrets(cfg.controlNamespace()).Get(ctx, cfg.RegistryPushSecret, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("inspect registry push Secret: %w", err)
	}
	if registrySecret.Type != corev1.SecretTypeDockerConfigJson || len(registrySecret.Data[corev1.DockerConfigJsonKey]) == 0 {
		return "", errors.New("registry push Secret lacks .dockerconfigjson data")
	}
	if err := i.acquireOperateLock(ctx, cfg, operationID); err != nil {
		return "", err
	}
	if _, err := i.Client.BatchV1().Jobs(cfg.controlNamespace()).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		_ = i.releaseOperateLock(context.Background(), cfg, operationID)
		return "", fmt.Errorf("start native operate Job: %w", err)
	}
	return job.Name, nil
}

// The ConfigMap create is atomic across host launchers. A terminal Job permits
// the next operation to claim the shared PVC; a pending Job still owns it.
func (i Installer) acquireOperateLock(ctx context.Context, cfg Config, operationID string) error {
	configMaps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
	for attempt := 0; attempt < 3; attempt++ {
		_, err := configMaps.Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: OperateLockConfigMap, Namespace: cfg.controlNamespace()}, Data: map[string]string{"operation_id": operationID}}, metav1.CreateOptions{})
		if err == nil {
			return nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("claim native operate PVC: %w", err)
		}
		prior, err := configMaps.Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("inspect native operate PVC claim: %w", err)
		}
		owner := prior.Data["operation_id"]
		if owner == "" {
			return errors.New("native operate PVC claim has no operation ID")
		}
		job, err := i.Client.BatchV1().Jobs(cfg.controlNamespace()).Get(ctx, OperateJobName(owner), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if prior.CreationTimestamp.IsZero() || time.Since(prior.CreationTimestamp.Time) < 15*time.Minute {
				return fmt.Errorf("native operate PVC is claimed by %q but its Job is missing; inspect the claim before retrying", owner)
			}
			uid := prior.UID
			if err := configMaps.Delete(ctx, OperateLockConfigMap, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				return fmt.Errorf("release expired native operate PVC claim: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect native operate PVC owner Job: %w", err)
		}
		if job.Status.Succeeded == 0 && !jobFailed(job) && (job.Status.Failed == 0 || job.Status.Active > 0) {
			return fmt.Errorf("native operate PVC is already used by operation %q", owner)
		}
		uid := prior.UID
		if err := configMaps.Delete(ctx, OperateLockConfigMap, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return fmt.Errorf("release completed native operate PVC claim: %w", err)
		}
	}
	return errors.New("native operate PVC claim changed while starting Job; retry")
}

func (i Installer) releaseOperateLock(ctx context.Context, cfg Config, operationID string) error {
	configMaps := i.Client.CoreV1().ConfigMaps(cfg.controlNamespace())
	claim, err := configMaps.Get(ctx, OperateLockConfigMap, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if claim.Data["operation_id"] != operationID {
		return nil
	}
	uid := claim.UID
	return configMaps.Delete(ctx, OperateLockConfigMap, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}

// WaitOperate requires a complete Job and an attributable success receipt.
func (i Installer) WaitOperate(ctx context.Context, cfg Config, operationID string) (OperateResult, error) {
	if i.Client == nil {
		return OperateResult{}, errors.New("Kubernetes client is required")
	}
	name := OperateJobName(operationID)
	for {
		job, err := i.Client.BatchV1().Jobs(cfg.controlNamespace()).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return OperateResult{}, fmt.Errorf("inspect operate Job: %w", err)
		}
		if job.Status.Succeeded > 0 {
			if err := i.releaseOperateLock(ctx, cfg, operationID); err != nil {
				return OperateResult{}, fmt.Errorf("release completed native operate PVC claim: %w", err)
			}
			break
		}
		if jobFailed(job) || (job.Status.Failed > 0 && job.Status.Active == 0) {
			if err := i.releaseOperateLock(ctx, cfg, operationID); err != nil {
				return OperateResult{}, fmt.Errorf("release failed native operate PVC claim: %w", err)
			}
			return OperateResult{}, errors.New("native operate Job failed")
		}
		if err := i.pause(ctx); err != nil {
			return OperateResult{}, err
		}
	}
	pods, err := i.Client.CoreV1().Pods(cfg.controlNamespace()).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	if err != nil {
		return OperateResult{}, err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}
		raw, err := i.Client.CoreV1().Pods(cfg.controlNamespace()).GetLogs(pod.Name, &corev1.PodLogOptions{}).DoRaw(ctx)
		if err != nil {
			return OperateResult{}, err
		}
		for _, line := range strings.Split(string(raw), "\n") {
			var result OperateResult
			if json.Unmarshal([]byte(line), &result) == nil && result.OperationID == operationID && result.Installed && result.SourceCommit != "" && result.JudgeCommit != "" {
				return result, nil
			}
		}
	}
	return OperateResult{}, errors.New("native operate Job completed without an attributable install receipt")
}

// LaunchOperate starts and awaits the native in-cluster lifecycle.
func (i Installer) LaunchOperate(ctx context.Context, cfg Config, objective, operationID string, maxAttempts int) (OperateResult, error) {
	if _, err := i.StartOperate(ctx, cfg, objective, operationID, maxAttempts); err != nil {
		return OperateResult{}, err
	}
	return i.WaitOperate(ctx, cfg, operationID)
}
