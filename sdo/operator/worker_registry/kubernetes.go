package worker_registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

const (
	TaskDataKey   = "agent-task.json"
	ResultDataKey = "agent-result.json"
)

// KubernetesExecutor is the implementation-neutral Job transport. By default,
// a Job sees only its independent proposal checkout.
type KubernetesExecutor struct {
	Client                 kubernetes.Interface
	Namespace              string
	Registry               *Registry
	ServiceAccount         string
	RepositoryPVC          string
	RepositoryMountPath    string
	RepositoryPVCSubPath   string
	SessionPVCSubPath      string
	CapturedCorpusReadOnly bool
	CredentialsSecret      string
	LateFindingsConfigMap  string
	Environment            map[string]string
	PollInterval           time.Duration
	TaskPayload            string
	RepositoryReadOnly     bool
}

type JobFailedError struct{ JobName string }

func (err *JobFailedError) Error() string { return fmt.Sprintf("agent Job %q failed", err.JobName) }

func (e KubernetesExecutor) Execute(ctx context.Context, task *contracts.AgentTask) (*contracts.AgentResult, error) {
	if e.Client == nil || e.Registry == nil || e.Namespace == "" || e.RepositoryPVC == "" {
		return nil, fmt.Errorf("Kubernetes client, registry, namespace, and repository PVC are required")
	}
	if err := e.Registry.ValidateTask(task); err != nil {
		return nil, fmt.Errorf("validate agent task: %w", err)
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
	payload, err := canonicalJSON(task)
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
	if err := e.ensureRequest(ctx, requestName, string(payload)); err != nil {
		return nil, err
	}
	if err := e.ensureJob(ctx, task, launch, jobName, requestName, resultName, proposalSubPath); err != nil {
		return nil, err
	}
	ticker := time.NewTicker(e.PollInterval)
	defer ticker.Stop()
	for {
		job, jobErr := e.Client.BatchV1().Jobs(e.Namespace).Get(ctx, jobName, metav1.GetOptions{})
		if jobErr != nil && !apierrors.IsNotFound(jobErr) {
			return nil, fmt.Errorf("get agent Job: %w", jobErr)
		}
		if jobErr == nil && jobFailed(job) {
			return nil, &JobFailedError{JobName: jobName}
		}
		resultMap, err := e.Client.CoreV1().ConfigMaps(e.Namespace).Get(ctx, resultName, metav1.GetOptions{})
		if err == nil {
			data, ok := resultMap.Data[ResultDataKey]
			if !ok {
				return nil, fmt.Errorf("result ConfigMap %q has no %s", resultName, ResultDataKey)
			}
			result := &contracts.AgentResult{}
			if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(data), result); err != nil {
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
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
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

func (e KubernetesExecutor) ensureJob(ctx context.Context, task *contracts.AgentTask, launch *contracts.KubernetesJobLaunch, jobName, requestName, resultName, proposalSubPath string) error {
	backoffLimit := int32(0)
	ttl := int32(3600)
	nonRoot := true
	uid := int64(65532)
	noEscalation := false
	readOnlyRoot := true
	envFrom := []corev1.EnvFromSource{}
	mounts := []corev1.VolumeMount{
		{Name: "request", MountPath: "/sdo/request", ReadOnly: true},
		{Name: "repository", MountPath: task.GetWorkspace().GetCheckoutPath(), SubPath: proposalSubPath, ReadOnly: e.RepositoryReadOnly},
		{Name: "scratch", MountPath: "/tmp"},
		{Name: codexHomesVolume, MountPath: CodexHomesMountPath},
	}
	volumes := []corev1.Volume{
		{Name: "request", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: requestName}}}},
		{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: e.RepositoryPVC}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: codexHomesVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
	}
	if e.SessionPVCSubPath != "" {
		// The same PVC is mounted a second time through the repository volume: two volumes backed by one claim
		// leave the kubelet waiting forever for the pod's volumes.
		mounts = append(mounts, corev1.VolumeMount{Name: "repository", MountPath: "/sdo/session", SubPath: e.SessionPVCSubPath})
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
		{Name: "SDO_REQUEST_CONFIGMAP", Value: requestName},
		{Name: "SDO_RESULT_CONFIGMAP", Value: resultName},
		{Name: "SDO_NAMESPACE", Value: e.Namespace},
		{Name: "SDO_CODEX_HOME_ROOT", Value: CodexHomesMountPath},
	}
	reserved := map[string]bool{"SDO_REQUEST_CONFIGMAP": true, "SDO_RESULT_CONFIGMAP": true, "SDO_NAMESPACE": true, "SDO_CODEX_HOME_ROOT": true}
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
		Spec: batchv1.JobSpec{BackoffLimit: &backoffLimit, TTLSecondsAfterFinished: &ttl,
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
	if _, err := e.Client.BatchV1().Jobs(e.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create agent Job: %w", err)
	}
	return nil
}

func canonicalJSON(message proto.Message) ([]byte, error) {
	raw, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(message)
	if err != nil {
		return nil, err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
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
