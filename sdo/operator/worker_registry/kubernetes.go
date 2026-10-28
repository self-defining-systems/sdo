package worker_registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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
	// MaxRequestBytes keeps a ConfigMap request well under the API server's 1 MiB object limit. A request over it
	// fails before any API call: requesters degrade a request to fit before freezing it, never add a second transport.
	MaxRequestBytes = 512 << 10
)

// requestSource says where a worker reads /sdo/request/agent-task.json: a ConfigMap, or a repository-volume subPath.
type requestSource struct {
	ConfigMap  string
	PVCSubPath string
}

// RequestTooLargeError names the request components that exhausted the request budget.
type RequestTooLargeError struct {
	TaskID     string
	Bytes      int
	Limit      int
	Components []string
}

// NonRetryable: the frozen request never shrinks, so a retry meets the same limit.
func (err *RequestTooLargeError) NonRetryable() bool { return true }

func (err *RequestTooLargeError) Error() string {
	return fmt.Sprintf("agent request for task %q is %d bytes, over the %d byte budget; largest components: %s", err.TaskID, err.Bytes, err.Limit, strings.Join(err.Components, ", "))
}

// KubernetesExecutor is the implementation-neutral Job transport. By default,
// a Job sees only its independent proposal checkout.
type KubernetesExecutor struct {
	WarmBinding            *WarmBinding
	standbyMode            bool
	standbySubPath         string
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

// JobFailedError is terminal for an immutable task execution. Transport retries
// rejoin that execution; only a new logical task may start another worker.
type JobFailedError struct {
	JobName string
	// RequestedMemoryLimit is the memory limit the Job template asked for.
	RequestedMemoryLimit string
	Conditions           []JobFailureCondition
	Containers           []ContainerFailure
	EvidenceError        string
}

type JobFailureCondition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

type ContainerFailure struct {
	PodName       string
	ContainerName string
	Reason        string
	ExitCode      int32
	Message       string
	// MemoryLimit is the limit the pod was admitted with, which a cluster admission webhook can rewrite.
	MemoryLimit string
}

func (err *JobFailedError) NonRetryable() bool { return true }

func (err *JobFailedError) Error() string {
	parts := []string{fmt.Sprintf("agent Job %q failed", err.JobName)}
	if reason := err.FailureReason(); reason != "" {
		parts = append(parts, fmt.Sprintf("reason %s (Job requested memory limit %s)", reason, err.RequestedMemoryLimit))
	}
	for _, condition := range err.Conditions {
		parts = append(parts, fmt.Sprintf("condition %s=%s reason=%q message=%q", condition.Type, condition.Status, condition.Reason, condition.Message))
	}
	for _, container := range err.Containers {
		parts = append(parts, fmt.Sprintf("Pod %q container %q terminated reason=%q exit_code=%d memory_limit=%q message=%q", container.PodName, container.ContainerName, container.Reason, container.ExitCode, container.MemoryLimit, container.Message))
	}
	if err.EvidenceError != "" {
		parts = append(parts, "Pod termination evidence unavailable: "+err.EvidenceError)
	}
	return strings.Join(parts, "; ")
}

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
	// Executors survive continuation turns; bindings belong to immutable tasks.
	binding, err := e.loadWarmBinding(task)
	if err != nil {
		return nil, err
	}
	e.WarmBinding = binding
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
	if task.GetRequest().MessageIs(&contracts.HealthJudgeRequest{}) {
		// Setup accounting survives publication failure and stays outside the
		// incident session tree. Each immutable request has its own directory.
		relative := path.Join(".sdo-runtime", "setup", taskRequestDigest(task))
		if err := os.MkdirAll(filepath.Join(e.RepositoryMountPath, relative), 0700); err != nil {
			return nil, fmt.Errorf("prepare health judge setup accounting: %w", err)
		}
		e.SessionPVCSubPath = path.Join(e.RepositoryPVCSubPath, relative)
		e.Environment = cloneEnvironment(e.Environment)
		e.Environment["SDO_TURN_USAGE_LOG"] = "/sdo/session/setup-usage.jsonl"
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
		if err := wire.UnmarshalDurable([]byte(e.TaskPayload), prepared); err != nil || !proto.Equal(prepared, task) {
			return nil, errors.New("prepared agent task differs from validated task")
		}
		payload = []byte(e.TaskPayload)
	}
	jobName, err := e.taskJobName(ctx, task)
	if err != nil {
		return nil, err
	}
	requestName, resultName := jobName+"-request", jobName+"-result"
	request := requestSource{ConfigMap: requestName}
	if len(payload) > MaxRequestBytes {
		return nil, &RequestTooLargeError{TaskID: task.GetTaskId(), Bytes: len(payload), Limit: MaxRequestBytes, Components: RequestComponents(task)}
	}
	err = e.ensureRequest(ctx, requestName, string(payload))
	var conflict *RequestPayloadConflictError
	if errors.As(err, &conflict) && jobName != TaskJobName(task) {
		// Another controller may provision a different payload between
		// the read-only recovery check and create-if-absent request write.
		jobName = TaskJobName(task)
		requestName, resultName = jobName+"-request", jobName+"-result"
		request.ConfigMap = requestName
		err = e.ensureRequest(ctx, requestName, string(payload))
	}
	if err != nil {
		return nil, err
	}
	if e.WarmBinding == nil {
		if err := e.ensureIncidentExecution(ctx, task, jobName, ""); err != nil {
			return nil, err
		}
	}
	timing, err := e.prepareDispatchStartup(task, jobName)
	if err != nil {
		return nil, fmt.Errorf("persist responder dispatch timing: %w", err)
	}
	if e.SessionPVCSubPath != "" {
		if relative, err := filepath.Rel(sessionPVCBase(e.RepositoryPVCSubPath), e.SessionPVCSubPath); err == nil {
			session := filepath.Join(e.RepositoryMountPath, relative)
			if info, err := os.Stat(session); err == nil && info.IsDir() {
				if err := os.MkdirAll(filepath.Join(session, "startup-workers", jobName), 0700); err != nil {
					return nil, err
				}
				if err := os.MkdirAll(filepath.Join(session, "startup"), 0700); err != nil {
					return nil, err
				}
				if err := os.MkdirAll(filepath.Join(session, "late-findings"), 0700); err != nil {
					return nil, err
				}
			}
		}
	}
	e.projectLateFindings(ctx, task)
	if e.WarmBinding != nil {
		if err := e.publishWarmTask(ctx, task); err != nil {
			var unpublished *standbyClaimUnpublished
			if errors.As(err, &unpublished) {
				if cleanupErr := e.abandonUnpublished(ctx, task); cleanupErr != nil && !apierrors.IsNotFound(cleanupErr) {
					return nil, cleanupErr
				}
				e.WarmBinding = nil
				return e.Execute(ctx, task)
			}
			return nil, err
		}
	} else if err := e.ensureJob(ctx, task, launch, jobName, request, resultName, proposalSubPath); err != nil {
		return nil, err
	}
	if err := e.recordWorkerDispatch(ctx, task, jobName); err != nil {
		return nil, err
	}
	startup := e.startupRecord(task, jobName)
	if startup != nil {
		startup.TaskBoundAt = &timing.TaskBoundAt
		startup.Mode = timing.Mode
		startup.ClaimedAt = timing.ClaimedAt
	}

	defer e.emitStartup(startup)
	if e.OnPhase != nil {
		if err := e.OnPhase(ctx, "reflection_job_created"); err != nil {
			return nil, err
		}
	}
	ticker := time.NewTicker(e.PollInterval)
	defer ticker.Stop()
	observed := map[string]bool{}
	for {
		e.projectLateFindings(ctx, task)
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
		if apierrors.IsNotFound(jobErr) {
			if result, recovered, err := e.RecoverCompleted(ctx, task); err != nil {
				return nil, err
			} else if recovered {
				return result, nil
			}
			return nil, &JobVanishedError{TaskID: task.GetTaskId(), JobName: jobName}
		}
		if startup != nil && jobErr == nil {
			pods, err := e.Client.CoreV1().Pods(e.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + jobName})
			if err == nil {
				startup.observe(job, pods.Items)
			}
		}
		if jobErr == nil && jobFailed(job) {
			return nil, e.taskJobFailure(ctx, task, job)
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

// RequestComponents lists the request's largest populated top-level fields with their encoded sizes.
func RequestComponents(task *contracts.AgentTask) []string {
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
	prior, next := new(contracts.AgentTask), new(contracts.AgentTask)
	if wire.UnmarshalDurable([]byte(existing.Data[TaskDataKey]), prior) != nil || wire.UnmarshalDurable([]byte(payload), next) != nil || !proto.Equal(prior, next) {
		return &RequestPayloadConflictError{ConfigMap: name}
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
	reflection := task.GetRequest().MessageIs(&contracts.ReflectionRequest{})
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
		if task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
			mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/startup", SubPath: path.Join(e.SessionPVCSubPath, "startup"), ReadOnly: true})
		}
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
		if e.CapturedCorpusReadOnly || reflection {
			mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/corpus", SubPath: path.Join(e.SessionPVCSubPath, "corpus"), ReadOnly: true})
		}
	}
	if e.SessionPVCSubPath != "" && task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
		// The controller projects the report into this directory (see
		// projectLateFindings); both paths are read-only for the worker.
		lateFindings := path.Join(e.SessionPVCSubPath, "late-findings")
		mounts = append(mounts,
			corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session/late-findings", SubPath: lateFindings, ReadOnly: true},
			corev1.VolumeMount{Name: "repository", MountPath: "/sdo/late-findings", SubPath: lateFindings, ReadOnly: true})
	}
	if e.CredentialsSecret != "" {
		envFrom = append(envFrom, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: e.CredentialsSecret}}})
		mounts = append(mounts, corev1.VolumeMount{Name: "credentials", MountPath: "/sdo/credentials", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: e.CredentialsSecret}}})
	}
	env := workerIdentityEnvironment()
	env = append(env, []corev1.EnvVar{
		{Name: "SDO_REQUEST_CONFIGMAP", Value: request.ConfigMap},
		{Name: "SDO_RESULT_CONFIGMAP", Value: resultName},
		{Name: "SDO_NAMESPACE", Value: e.Namespace},
		{Name: "SDO_REQUEST_SHA256", Value: taskRequestDigest(task)},
		{Name: "SDO_CODEX_HOME_ROOT", Value: CodexHomesMountPath},
	}...)
	if e.SessionPVCSubPath != "" {
		env = append(env, corev1.EnvVar{Name: "SDO_SESSION_DIR", Value: "/sdo/session"})
		if task.GetRequest().MessageIs(&contracts.IncidentRequest{}) {
			env = append(env, corev1.EnvVar{Name: "SDO_STARTUP_TIMING_PATH", Value: "/sdo/session/startup-workers/" + jobName + "/startup-worker.json"})
		}
	}
	judge := task.GetRequest().MessageIs(&contracts.HealthJudgeRequest{})
	memoryAuthor := reflection || judge
	baseline := ""
	captureDirectory := ""
	if reflection && e.SessionPVCSubPath != "" {
		captureDirectory = "/sdo/session/corpus"
		baseline = path.Join(captureDirectory, "healthy")
	}
	if memoryAuthor {
		actor, phase := "responder", "reflection"
		if judge {
			actor, phase = "health_judge", "initial"
		}
		env = append(env, corev1.EnvVar{Name: "SDO_ACTOR", Value: actor}, corev1.EnvVar{Name: "SDO_PHASE", Value: phase}, corev1.EnvVar{Name: "SDO_BASE_COMMIT", Value: task.GetWorkspace().GetBaseCommit()})
	}
	if reflection {
		env = append(env, corev1.EnvVar{Name: "SDO_HEALTHY_BASELINE", Value: baseline}, corev1.EnvVar{Name: "SDO_CAPTURED_CORPUS_DIR", Value: captureDirectory})
	}
	reserved := map[string]bool{"SDO_REQUEST_SHA256": true, "SDO_CODEX_HOME_ROOT": true, "SDO_REQUEST_CONFIGMAP": true, "SDO_RESULT_CONFIGMAP": true, "SDO_NAMESPACE": true, "SDO_SESSION_DIR": e.SessionPVCSubPath != "", "SDO_ACTOR": memoryAuthor, "SDO_PHASE": memoryAuthor, "SDO_BASE_COMMIT": memoryAuthor, "SDO_HEALTHY_BASELINE": reflection, "SDO_CAPTURED_CORPUS_DIR": reflection, "SDO_API_PROXY_URL": true, "SDO_API_HISTORY_DIR": true, "SDO_STARTUP_TIMING_PATH": true}
	for _, identity := range workerIdentityEnvironment() {
		reserved[identity.Name] = true
	}
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
						// The controller reads no logs: a failing worker writes one curated line to /dev/termination-log.
						SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &noEscalation, ReadOnlyRootFilesystem: &readOnlyRoot, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
						Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse(WorkerMemoryRequest)}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse(WorkerMemoryLimit)}},
					}}, Volumes: volumes,
				},
			},
		},
	}
	e.addAPIHistory(job, task)
	if e.standbyMode {
		e.configureStandby(job, launch)
	}
	// Incident dispatch always runs after the controller seeded the toolchain.
	// Other Jobs (reflection, health refresh) mount the seed when it exists and
	// copy only when this process's repository volume has none.
	if task.GetRequest().MessageIs(&contracts.IncidentRequest{}) || toolchain.Seeded(e.RepositoryMountPath, e.ToolchainImage) {
		toolchain.ApplyShared(&job.Spec.Template.Spec, e.ToolchainImage, e.RepositoryPVCSubPath)
	} else {
		toolchain.Apply(&job.Spec.Template.Spec, e.ToolchainImage)
	}
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

func cloneEnvironment(source map[string]string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for key, value := range source {
		result[key] = value
	}
	return result
}

// RequestPayloadConflictError retains the conflicting immutable request; the
// caller may select its deterministic request-bound execution identity.
type RequestPayloadConflictError struct{ ConfigMap string }

func (e *RequestPayloadConflictError) Error() string {
	return fmt.Sprintf("existing agent request ConfigMap %q has different payload", e.ConfigMap)
}

// The Pod itself captures its immutable identity before publishing a provider session.
func workerIdentityEnvironment() []corev1.EnvVar {
	result := []corev1.EnvVar{{Name: "SDO_WORKER_CONTAINER", Value: "agent"}}
	for _, entry := range []struct{ name, field string }{
		{"SDO_WORKER_NAMESPACE", "metadata.namespace"},
		{"SDO_WORKER_POD_NAME", "metadata.name"},
		{"SDO_WORKER_POD_UID", "metadata.uid"},
	} {
		result = append(result, corev1.EnvVar{Name: entry.name, ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: entry.field},
		}})
	}
	return result
}
