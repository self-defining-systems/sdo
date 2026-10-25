package worker_registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/devmount"
	"sdo.dev/operator/toolchain"
)

const (
	TaskDataKey   = "agent-task.json"
	ResultDataKey = "agent-result.json"
	// MaxConfigMapRequestBytes keeps a ConfigMap request well under the API server's 1 MiB object limit. Larger
	// requests are staged on the repository volume instead.
	MaxConfigMapRequestBytes = 512 << 10
	// MaxStagedRequestBytes is the hard budget for one staged request. Exceeding it fails before any API call.
	MaxStagedRequestBytes = 64 << 20
	stagedRequestsDir     = ".sdo-runtime/requests"
)

// requestSource says where a worker reads /sdo/request/agent-task.json: a ConfigMap, or a repository-volume subPath.
type requestSource struct {
	ConfigMap  string
	PVCSubPath string
}

// RequestTooLargeError names the request components that exhausted the staged-request budget.
type RequestTooLargeError struct {
	TaskID     string
	Bytes      int
	Limit      int
	Components []string
}

func (err *RequestTooLargeError) Error() string {
	return fmt.Sprintf("agent request for task %q is %d bytes, over the %d byte budget; largest components: %s", err.TaskID, err.Bytes, err.Limit, strings.Join(err.Components, ", "))
}

// RequestStagingError reports that a large request could not be written to the repository volume.
type RequestStagingError struct {
	TaskID string
	Err    error
}

func (err *RequestStagingError) Error() string {
	return fmt.Sprintf("stage agent request for task %q on the repository volume: %v", err.TaskID, err.Err)
}
func (err *RequestStagingError) Unwrap() error { return err.Err }

// KubernetesExecutor is the implementation-neutral Job transport. By default,
// a Job sees only its independent proposal checkout.
type KubernetesExecutor struct {
	APIProxyImage          string
	ToolchainImage         string
	TaskPayload            string
	CapturedCorpusReadOnly bool
	RepositoryReadOnly     bool
	OnPhase                func(context.Context, string) error
	Client                 kubernetes.Interface
	Namespace              string
	Registry               *Registry
	ServiceAccount         string
	RepositoryPVC          string
	RepositoryMountPath    string
	RepositoryPVCSubPath   string
	SessionPVCSubPath      string
	HealthyBaseline        bool
	CredentialsSecret      string
	LateFindingsConfigMap  string
	Environment            map[string]string
	PollInterval           time.Duration
}

type JobFailedError struct{ JobName string }

func (err *JobFailedError) Error() string { return fmt.Sprintf("agent Job %q failed", err.JobName) }

// TaskTimeoutError is terminal for this frozen reflection task. Retrying a
// transport may rejoin work, but must never grant a new execution budget.
type TaskTimeoutError struct {
	TaskID   string
	Deadline time.Time
}

func (e *TaskTimeoutError) Error() string {
	return fmt.Sprintf("reflection task %q exceeded frozen deadline %s", e.TaskID, e.Deadline.UTC().Format(time.RFC3339Nano))
}
func (e *TaskTimeoutError) Unwrap() error { return context.DeadlineExceeded }

func frozenReflectionDeadline(task *contracts.AgentTask) (time.Time, bool) {
	if task.GetDeadline() == nil || !task.GetRequest().MessageIs(&contracts.ReflectionRequest{}) {
		return time.Time{}, false
	}
	return task.GetDeadline().AsTime(), true
}

func (e KubernetesExecutor) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if e.Client == nil || e.Registry == nil || e.Namespace == "" || e.RepositoryPVC == "" {
		return nil, fmt.Errorf("Kubernetes client, registry, namespace, and repository PVC are required")
	}
	if err := e.Registry.ValidateTask(task); err != nil {
		return nil, fmt.Errorf("validate agent task: %w", err)
	}
	if result, recovered, err := e.RecoverCompleted(ctx, task); err != nil {
		return nil, err
	} else if recovered {
		return result, nil
	}
	deadline, bounded := frozenReflectionDeadline(task)
	if bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		if !time.Now().Before(deadline) {
			return nil, &TaskTimeoutError{TaskID: task.GetTaskId(), Deadline: deadline}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	registration, err := e.Registry.Resolve(task.GetAgentId(), task.GetTaskType())
	if err != nil {
		return nil, err
	}
	launch := registration.GetKubernetesJob()
	if launch == nil {
		return nil, fmt.Errorf("agent %q is not registered as a Kubernetes Job", task.GetAgentId())
	}
	if e.RepositoryMountPath == "" {
		e.RepositoryMountPath = "/workspace"
	}
	checkout := path.Clean(task.GetWorkspace().GetCheckoutPath())
	mountRoot := strings.TrimSuffix(path.Clean(e.RepositoryMountPath), "/")
	if !path.IsAbs(checkout) || !path.IsAbs(mountRoot) || !strings.HasPrefix(checkout, mountRoot+"/") {
		return nil, fmt.Errorf("proposal checkout %q must be inside shared repository mount %q", checkout, mountRoot)
	}
	proposalSubPath := path.Join(e.RepositoryPVCSubPath, strings.TrimPrefix(checkout, mountRoot+"/"))
	if e.SessionPVCSubPath != "" {
		clean := path.Clean(e.SessionPVCSubPath)
		if clean != e.SessionPVCSubPath || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || path.IsAbs(clean) || strings.Contains(clean, "\\") || clean == proposalSubPath || strings.HasPrefix(clean, proposalSubPath+"/") || strings.HasPrefix(proposalSubPath, clean+"/") {
			return nil, errors.New("agent session PVC subPath must be a separate narrow directory")
		}
	}
	if e.PollInterval <= 0 {
		e.PollInterval = time.Second
	}
	payload, err := wire.Canonical(task)
	if err != nil {
		return nil, fmt.Errorf("encode agent task: %w", err)
	}
	if e.TaskPayload != "" {
		prepared := new(contracts.AgentTask)
		if err := protojson.Unmarshal([]byte(e.TaskPayload), prepared); err != nil || !proto.Equal(prepared, task) {
			return nil, errors.New("prepared agent task differs from validated task")
		}
		payload = []byte(e.TaskPayload)
	}
	jobName := JobName(task.GetTaskId())
	requestName, resultName := jobName+"-request", jobName+"-result"
	request := requestSource{ConfigMap: requestName}
	if len(payload) <= MaxConfigMapRequestBytes {
		if err := e.ensureRequest(ctx, requestName, string(payload)); err != nil {
			return nil, err
		}
	} else {
		staged, err := e.stageRequest(task, jobName, payload)
		if err != nil {
			return nil, err
		}
		request = requestSource{PVCSubPath: staged}
		defer e.unstageRequest(jobName)
	}
	if err := e.ensureJob(ctx, task, launch, jobName, request, resultName, proposalSubPath); err != nil {
		return nil, err
	}
	if e.OnPhase != nil {
		if err := e.OnPhase(ctx, "reflection_job_created"); err != nil {
			return nil, err
		}
	}
	ticker := time.NewTicker(e.PollInterval)
	defer ticker.Stop()
	observed := map[string]bool{}
	for {
		if e.OnPhase != nil && !observed["pod_ready"] {
			pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + jobName})
			if err == nil {
				for _, pod := range pods.Items {
					for _, condition := range pod.Status.Conditions {
						if condition.Type == corev1.PodScheduled && condition.Status == corev1.ConditionTrue && !observed["pod_scheduled"] {
							if err := e.OnPhase(ctx, "pod_scheduled"); err != nil {
								return nil, err
							}
							observed["pod_scheduled"] = true
						}
					}
					if pod.Status.StartTime != nil && !observed["pod_started"] {
						if err := e.OnPhase(ctx, "pod_started"); err != nil {
							return nil, err
						}
						observed["pod_started"] = true
					}
					for _, condition := range pod.Status.Conditions {
						if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue && !observed["pod_ready"] {
							if err := e.OnPhase(ctx, "pod_ready"); err != nil {
								return nil, err
							}
							observed["pod_ready"] = true
						}
					}
				}
			}
		}
		job, jobErr := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, jobName, metav1.GetOptions{})
		if jobErr != nil && !apierrors.IsNotFound(jobErr) {
			return nil, fmt.Errorf("get agent Job: %w", jobErr)
		}
		if jobErr == nil && jobFailed(job) {
			return nil, taskJobFailure(task, job)
		}
		resultMap, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, resultName, metav1.GetOptions{})
		if err == nil {
			if e.OnPhase != nil && !observed["worker_result_written"] {
				if err := e.OnPhase(ctx, "worker_result_written"); err != nil {
					return nil, err
				}
				observed["worker_result_written"] = true
			}
			data, ok := resultMap.Data[ResultDataKey]
			if !ok {
				return nil, fmt.Errorf("result ConfigMap %q has no %s", resultName, ResultDataKey)
			}
			result := &contracts.AgentResult{}
			if err := wire.Unmarshal([]byte(data), result); err != nil {
				return nil, fmt.Errorf("decode agent result: %w", err)
			}
			if err := e.Registry.ValidateResult(task, result); err != nil {
				return nil, fmt.Errorf("validate agent result: %w", err)
			}
			// The worker publishes before its process exits. Hold the incident
			// lock until Kubernetes reports the Job durably completed.
			if jobErr == nil && jobCompleted(job) {
				return result, nil
			}
		}
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get agent result ConfigMap: %w", err)
		}
		select {
		case <-ctx.Done():
			if bounded && !time.Now().Before(deadline) {
				return nil, &TaskTimeoutError{TaskID: task.GetTaskId(), Deadline: deadline}
			}
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// stageRequest writes an oversized request under a per-job directory of the repository volume and returns the PVC
// subPath that holds it. The directory is outside every proposal checkout and session.
func (e KubernetesExecutor) stageRequest(task *contracts.AgentTask, jobName string, payload []byte) (string, error) {
	if len(payload) > MaxStagedRequestBytes {
		return "", &RequestTooLargeError{TaskID: task.GetTaskId(), Bytes: len(payload), Limit: MaxStagedRequestBytes, Components: requestComponents(task)}
	}
	if err := writeStaged(e.RepositoryMountPath, jobName, TaskDataKey, payload); err != nil {
		return "", &RequestStagingError{TaskID: task.GetTaskId(), Err: err}
	}
	return path.Join(e.RepositoryPVCSubPath, stagedRequestsDir, jobName), nil
}

// unstageRequest removes the Job's request copy but not a prepared task the controller state still references.
func (e KubernetesExecutor) unstageRequest(jobName string) {
	directory := filepath.Join(e.RepositoryMountPath, filepath.FromSlash(stagedRequestsDir), jobName)
	_ = os.Remove(filepath.Join(directory, TaskDataKey))
	_ = os.Remove(directory)
}

// requestComponents lists the request's largest populated top-level fields with their encoded sizes.
func requestComponents(task *contracts.AgentTask) []string {
	message, err := task.GetRequest().UnmarshalNew()
	if err != nil {
		return []string{"request"}
	}
	type component struct {
		name string
		size int
	}
	var components []component
	message.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		single := message.ProtoReflect().Type().New()
		single.Set(field, value)
		components = append(components, component{string(field.Name()), proto.Size(single.Interface())})
		return true
	})
	sort.Slice(components, func(i, j int) bool { return components[i].size > components[j].size })
	names := []string{}
	for _, c := range components[:min(len(components), 3)] {
		names = append(names, fmt.Sprintf("%s=%d", c.name, c.size))
	}
	return names
}

func (e KubernetesExecutor) ensureRequest(ctx context.Context, name, payload string) error {
	maps := e.Client.CoreV1().ConfigMaps(e.Namespace)
	_, err := maps.Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: e.Namespace},
		Data:       map[string]string{TaskDataKey: payload},
	}, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create agent request ConfigMap: %w", err)
	}
	existing, err := maps.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get existing agent request ConfigMap: %w", err)
	}
	if existing.Data[TaskDataKey] != payload {
		return fmt.Errorf("existing agent request ConfigMap %q has different payload", name)
	}
	return nil
}

const (
	codexHomesVolume = "codex-homes"
	// CodexHomesMountPath is the private writable directory for per-turn Codex homes. Codex rejects a home under
	// /tmp or the checkout, and the worker's root filesystem is read-only.
	CodexHomesMountPath = "/sdo/codex-homes"
)

func (e KubernetesExecutor) ensureJob(ctx context.Context, task *contracts.AgentTask, launch *contracts.KubernetesJobLaunch, jobName string, request requestSource, resultName, proposalSubPath string) error {
	var activeDeadlineSeconds *int64
	if deadline, bounded := frozenReflectionDeadline(task); bounded {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &TaskTimeoutError{TaskID: task.GetTaskId(), Deadline: deadline}
		}
		seconds := int64(remaining / time.Second)
		if remaining%time.Second > 0 {
			seconds++
		}
		activeDeadlineSeconds = &seconds
	}
	backoffLimit := int32(0)
	ttl := int32(3600)
	nonRoot := true
	uid := int64(65532)
	noEscalation := false
	readOnlyRoot := true
	envFrom := []corev1.EnvFromSource{}
	mounts := []corev1.VolumeMount{
		{Name: "repository", MountPath: task.GetWorkspace().GetCheckoutPath(), SubPath: proposalSubPath, ReadOnly: e.RepositoryReadOnly},
		{Name: "scratch", MountPath: "/tmp"},
		{Name: codexHomesVolume, MountPath: CodexHomesMountPath},
	}
	volumes := []corev1.Volume{
		{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: e.RepositoryPVC}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: codexHomesVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	if request.PVCSubPath != "" {
		// The request outgrew a ConfigMap: the worker reads the same /sdo/request/agent-task.json from the repository volume.
		mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/request", SubPath: request.PVCSubPath, ReadOnly: true})
	} else {
		mounts = append(mounts, corev1.VolumeMount{Name: "request", MountPath: "/sdo/request", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "request", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: request.ConfigMap}}}})
	}
	if e.SessionPVCSubPath != "" {
		// The same PVC is mounted a second time through the repository volume: two volumes backed by one claim
		// leave the kubelet waiting forever for the pod's volumes.
		mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session", SubPath: e.SessionPVCSubPath})
		if e.Environment["SDO_TOOLS_DIR"] != "" {
			// Bind the directories individually so the worker cannot replace
			// server-owned entries through the writable session parent. Replies
			// remain writable because the CLI unlinks them after reading.
			mounts = append(mounts,
				corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/tools", SubPath: path.Join(e.SessionPVCSubPath, "tools"), ReadOnly: true},
				corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/tools/requests", SubPath: path.Join(e.SessionPVCSubPath, "tools/requests")},
				corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/tools/responses", SubPath: path.Join(e.SessionPVCSubPath, "tools/responses")},
			)
		}
		if e.CapturedCorpusReadOnly {
			mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/corpus", SubPath: path.Join(e.SessionPVCSubPath, "corpus"), ReadOnly: true})
		}
	}
	if e.LateFindingsConfigMap != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "late-findings", MountPath: "/sdo/late-findings", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "late-findings", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: e.LateFindingsConfigMap}}}})
	}
	if e.CredentialsSecret != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: e.CredentialsSecret}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "credentials", MountPath: "/sdo/credentials", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: e.CredentialsSecret}}})
	}
	env := []corev1.EnvVar{
		{Name: "SDO_REQUEST_CONFIGMAP", Value: request.ConfigMap},
		{Name: "SDO_RESULT_CONFIGMAP", Value: resultName},
		{Name: "SDO_NAMESPACE", Value: e.Namespace},
		{Name: "SDO_CODEX_HOME_ROOT", Value: CodexHomesMountPath},
	}
	if e.SessionPVCSubPath != "" {
		env = append(env, corev1.EnvVar{Name: "SDO_SESSION_DIR", Value: "/sdo/session"})
	}
	reflection := task.GetRequest().MessageIs(&contracts.ReflectionRequest{})
	judge := task.GetRequest().MessageIs(&contracts.HealthJudgeRequest{})
	memoryAuthor := reflection || judge
	baseline := ""
	if reflection && e.HealthyBaseline {
		baseline = "/sdo/healthy-baseline"
		mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: baseline, SubPath: path.Join(e.RepositoryPVCSubPath, ".sdo-baseline/healthy"), ReadOnly: true})
	}
	if memoryAuthor {
		actor, phase := "responder", "reflection"
		if judge {
			actor, phase = "health_judge", "initial"
		}
		env = append(env, corev1.EnvVar{Name: "SDO_ACTOR", Value: actor}, corev1.EnvVar{Name: "SDO_PHASE", Value: phase}, corev1.EnvVar{Name: "SDO_BASE_COMMIT", Value: task.GetWorkspace().GetBaseCommit()})
	}
	if reflection {
		env = append(env, corev1.EnvVar{Name: "SDO_HEALTHY_BASELINE", Value: baseline})
	}
	reserved := map[string]bool{"SDO_CODEX_HOME_ROOT": true, "SDO_REQUEST_CONFIGMAP": true, "SDO_RESULT_CONFIGMAP": true, "SDO_NAMESPACE": true, "SDO_SESSION_DIR": e.SessionPVCSubPath != "", "SDO_ACTOR": memoryAuthor, "SDO_PHASE": memoryAuthor, "SDO_BASE_COMMIT": memoryAuthor, "SDO_HEALTHY_BASELINE": reflection, "SDO_API_PROXY_URL": true, "SDO_API_HISTORY_DIR": true}
	names := make([]string, 0, len(e.Environment))
	for name := range e.Environment {
		if !reserved[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		env = append(env, corev1.EnvVar{Name: name, Value: e.Environment[name]})
	}
	// Unconfined seccomp: Codex's bubblewrap sandbox needs unprivileged user namespaces, which RuntimeDefault denies
	// (verified: `codex sandbox` fails under RuntimeDefault and runs under Unconfined). The pod stays non-root with
	// every capability dropped and privilege escalation off.
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: e.Namespace},
		Spec: batchv1.JobSpec{BackoffLimit: &backoffLimit, TTLSecondsAfterFinished: &ttl, ActiveDeadlineSeconds: activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/name": "sdo-worker", "sdo.dev/incident": jobName}},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever, ServiceAccountName: e.ServiceAccount,
					SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &nonRoot, RunAsUser: &uid, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}},
					Containers: []corev1.Container{{
						Name: "agent", Image: launch.GetImage(), Command: append([]string(nil), launch.GetCommand()...),
						WorkingDir: task.GetWorkspace().GetCheckoutPath(), EnvFrom: envFrom, Env: env, VolumeMounts: mounts,
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &noEscalation, ReadOnlyRootFilesystem: &readOnlyRoot, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
						Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("512Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}},
					}}, Volumes: volumes,
				},
			},
		},
	}
	e.addAPIHistory(job, task)
	toolchain.Apply(&job.Spec.Template.Spec, e.ToolchainImage)
	if devmount.Enabled() {
		devmount.ApplyWorker(&job.Spec.Template.Spec)
	}
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create agent Job: %w", err)
	}
	return nil
}

func jobFailed(job *batchv1.Job) bool {
	if job.Status.Failed > 0 && job.Status.Active == 0 {
		return true
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
func taskJobFailure(task *contracts.AgentTask, job *batchv1.Job) error {
	if deadline, bounded := frozenReflectionDeadline(task); bounded {
		for _, condition := range job.Status.Conditions {
			if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue && condition.Reason == "DeadlineExceeded" {
				return &TaskTimeoutError{TaskID: task.GetTaskId(), Deadline: deadline}
			}
		}
	}
	return &JobFailedError{JobName: job.Name}
}

func jobCompleted(job *batchv1.Job) bool {
	if job.Status.Succeeded > 0 {
		return true
	}
	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func JobName(taskID string) string {
	hash := sha256.Sum256([]byte(taskID))
	suffix := hex.EncodeToString(hash[:])[:10]
	var builder strings.Builder
	for _, character := range strings.ToLower(taskID) {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			builder.WriteRune(character)
		} else if builder.Len() > 0 && builder.String()[builder.Len()-1] != '-' {
			builder.WriteByte('-')
		}
		if builder.Len() >= 40 {
			break
		}
	}
	prefix := strings.Trim(builder.String(), "-")
	if prefix == "" {
		prefix = "task"
	}
	return "sdo-" + prefix + "-" + suffix
}
