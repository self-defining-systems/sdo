package broker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/kubernetes"
	kubescheme "k8s.io/client-go/kubernetes/scheme"
	"sdo.dev/operator/devmount"
	"sdo.dev/operator/memory/validate"
)

var gitTreeID = regexp.MustCompile(`^[0-9a-f]{40}$`)

// KubernetesValidationJobClient runs the existing detector builder against a
// broker-owned staging clone. It proves that the cluster enforces the Job's
// default-deny NetworkPolicy before starting untrusted detector code.
type KubernetesValidationJobClient struct {
	ToolchainImage       string
	Client               kubernetes.Interface
	Namespace            string
	Image                string
	RepositoryPVC        string
	RepositoryMountPath  string
	RepositoryPVCSubPath string
	SchemaIdentity       string
	HealthyBaseline      bool
	PollInterval         time.Duration
	Timeout              time.Duration
	OnPhase              func(context.Context, string) error
	// IsolationProof, when set, lets validations within IsolationProofTTL reuse
	// one successful network canary proof instead of rerunning both canary Jobs.
	IsolationProof    *IsolationProof
	IsolationProofTTL time.Duration
}

// validatorIsolationPolicy is the one default-deny NetworkPolicy that selects
// every validator pod. It outlives a single validation, so the policy is long
// programmed in the CNI by the time a validator pod starts.
const validatorIsolationPolicy = "sdo-validator-isolation"

const defaultIsolationProofTTL = 30 * time.Minute

// IsolationProof records that, for one validator image, an allowed canary
// reached the network and a canary under the default-deny policy did not.
type IsolationProof struct {
	mu       sync.Mutex
	image    string
	provenAt time.Time
}

func (p *IsolationProof) valid(image string, ttl time.Duration, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.image == image && !p.provenAt.IsZero() && now.Sub(p.provenAt) < ttl
}

func (p *IsolationProof) record(image string, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.image, p.provenAt = image, now
}

func (k KubernetesValidationJobClient) Validate(ctx context.Context, request ValidationJobRequest) (ValidationJobReceipt, error) {
	if k.Client == nil || k.Namespace == "" || k.Image == "" || k.RepositoryPVC == "" || k.RepositoryMountPath == "" || k.SchemaIdentity == "" {
		return ValidationJobReceipt{}, errors.New("validator Job requires Kubernetes client, namespace, image, PVC, mount path, and schema identity")
	}
	if !request.ReadOnly || !request.NoPublishingCredentials || !request.EgressIsolated || !gitTreeID.MatchString(request.CandidateTree) {
		return ValidationJobReceipt{}, errors.New("validator request lacks an isolated candidate tree")
	}
	mountRoot, err := filepath.Abs(k.RepositoryMountPath)
	if err != nil {
		return ValidationJobReceipt{}, err
	}
	root, err := filepath.Abs(request.CandidateRoot)
	if err != nil {
		return ValidationJobReceipt{}, err
	}
	if !within(mountRoot, root) || root == mountRoot {
		return ValidationJobReceipt{}, errors.New("candidate is outside the shared repository PVC mount")
	}
	relative, err := filepath.Rel(mountRoot, root)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
		return ValidationJobReceipt{}, errors.New("candidate has no safe PVC subpath")
	}
	subPath := filepath.ToSlash(filepath.Join(k.RepositoryPVCSubPath, relative))
	if strings.HasPrefix(subPath, "../") || subPath == ".." || filepath.IsAbs(subPath) {
		return ValidationJobReceipt{}, errors.New("candidate PVC subpath escapes mount")
	}
	if k.PollInterval <= 0 {
		k.PollInterval = time.Second
	}
	if k.Timeout <= 0 {
		k.Timeout = 10 * time.Minute
	}
	validationCtx, cancel := context.WithTimeout(ctx, k.Timeout)
	defer cancel()
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return ValidationJobReceipt{}, err
	}
	name := "sdo-validator-" + hex.EncodeToString(random)
	defer k.cleanup(name)
	if err := k.proveIsolation(validationCtx, name); err != nil {
		return ValidationJobReceipt{}, err
	}
	useBaseline := false
	if k.HealthyBaseline {
		useBaseline, err = baselineGateRequired(request.CandidateRoot, request.ChangedPaths)
		if err != nil {
			return ValidationJobReceipt{}, fmt.Errorf("inspect detector baseline gate: %w", err)
		}
	}
	if request.HealthyBaseline != "" {
		useBaseline = true
	}
	job := k.validationJob(name, subPath, request.CandidateTree, useBaseline)
	if request.HealthyBaseline != "" {
		baseline, err := filepath.Abs(request.HealthyBaseline)
		if err != nil {
			return ValidationJobReceipt{}, err
		}
		if !within(mountRoot, baseline) || baseline == mountRoot {
			return ValidationJobReceipt{}, errors.New("healthy baseline is outside repository PVC")
		}
		relative, err := filepath.Rel(mountRoot, baseline)
		if err != nil {
			return ValidationJobReceipt{}, err
		}
		mounts := job.Spec.Template.Spec.Containers[0].VolumeMounts
		for i := range mounts {
			if mounts[i].Name == "healthy-baseline" {
				mounts[i].SubPath = filepath.ToSlash(filepath.Join(k.RepositoryPVCSubPath, relative))
			}
		}
	}
	if k.OnPhase != nil {
		if err := k.OnPhase(validationCtx, "network_canaries_passed"); err != nil {
			return ValidationJobReceipt{}, err
		}
	}
	if err := k.requirePolicy(validationCtx); err != nil {
		return ValidationJobReceipt{}, err
	}
	if err := k.createJob(validationCtx, job); err != nil {
		return ValidationJobReceipt{}, err
	}
	if k.OnPhase != nil {
		if err := k.OnPhase(validationCtx, "validator_job_created"); err != nil {
			return ValidationJobReceipt{}, err
		}
	}
	if err := k.waitJob(validationCtx, name); err != nil {
		return ValidationJobReceipt{}, fmt.Errorf("validator build and source checks: %w", err)
	}
	return ValidationJobReceipt{
		CandidateTree: request.CandidateTree, Accepted: true, DetectorCompiled: true, SourceValidated: true,
		ReadOnlyMountObserved: true, NoPublishingCredentialsObserved: true, EgressIsolationCanaryPassed: true,
	}, nil
}

// proveIsolation establishes the default-deny policy and, unless a recent proof
// for this image still holds and the policy is unchanged, runs the allow and
// deny canaries. Validator pods are always labelled for the same policy.
func (k KubernetesValidationJobClient) proveIsolation(ctx context.Context, name string) error {
	image, now := k.Image, time.Now()
	ttl := k.IsolationProofTTL
	if ttl <= 0 {
		ttl = defaultIsolationProofTTL
	}
	if k.IsolationProof != nil && k.IsolationProof.valid(image, ttl, now) {
		current, err := k.Client.NetworkingV1().NetworkPolicies(k.Namespace).Get(ctx, validatorIsolationPolicy, metav1.GetOptions{})
		if err == nil && reflect.DeepEqual(current.Spec, validatorPolicy(k.Namespace).Spec) {
			slog.Info("validator_network_proof_reused", "age_ms", now.Sub(k.IsolationProof.provenAt).Milliseconds())
			return nil
		}
	}
	if err := k.createJob(ctx, k.canaryJob(name+"-allow", false)); err != nil {
		return err
	}
	if err := k.waitJob(ctx, name+"-allow"); err != nil {
		return fmt.Errorf("validator network positive control: %w", err)
	}
	if err := k.ensurePolicy(ctx); err != nil {
		return err
	}
	if err := k.createJob(ctx, k.canaryJob(name+"-deny", true)); err != nil {
		return err
	}
	if err := k.waitJob(ctx, name+"-deny"); err != nil {
		return fmt.Errorf("validator default-deny egress canary: %w", err)
	}
	if k.IsolationProof != nil {
		k.IsolationProof.record(image, time.Now())
	}
	return nil
}

func validatorPolicy(namespace string) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: validatorIsolationPolicy, Namespace: namespace},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{"sdo.dev/validator-isolation": validatorIsolationPolicy}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
		},
	}
}

func (k KubernetesValidationJobClient) ensurePolicy(ctx context.Context) error {
	policies := k.Client.NetworkingV1().NetworkPolicies(k.Namespace)
	policy := validatorPolicy(k.Namespace)
	current, err := policies.Get(ctx, validatorIsolationPolicy, metav1.GetOptions{})
	switch {
	case err == nil && reflect.DeepEqual(current.Spec, policy.Spec):
		return nil
	case err == nil:
		// Update in place: a delete and create would leave a window with no policy.
		current.Spec = policy.Spec
		updated, err := policies.Update(ctx, current, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("update default-deny validator NetworkPolicy: %w", err)
		}
		if !reflect.DeepEqual(updated.Spec, policy.Spec) {
			return errors.New("validator NetworkPolicy changed at admission")
		}
		return nil
	case !apierrors.IsNotFound(err):
		return fmt.Errorf("inspect validator NetworkPolicy: %w", err)
	}
	created, err := policies.Create(ctx, policy, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create default-deny validator NetworkPolicy: %w", err)
	}
	if !reflect.DeepEqual(created.Spec, policy.Spec) {
		return errors.New("validator NetworkPolicy changed at admission")
	}
	return nil
}

// requirePolicy fails closed unless the default-deny policy is in force right
// now; the validator pod must never start without it.
func (k KubernetesValidationJobClient) requirePolicy(ctx context.Context) error {
	current, err := k.Client.NetworkingV1().NetworkPolicies(k.Namespace).Get(ctx, validatorIsolationPolicy, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("validator default-deny NetworkPolicy is not in force: %w", err)
	}
	if !reflect.DeepEqual(current.Spec, validatorPolicy(k.Namespace).Spec) {
		return errors.New("validator default-deny NetworkPolicy changed before the validator started")
	}
	return nil
}

func (k KubernetesValidationJobClient) createJob(ctx context.Context, job *batchv1.Job) error {
	created, err := k.Client.BatchV1().Jobs(k.Namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create validator Job %s: %w", job.Name, err)
	}
	expected := job.DeepCopy()
	kubescheme.Scheme.Default(expected)
	labels := maps.Clone(created.Spec.Template.Labels)
	for _, key := range []string{"batch.kubernetes.io/job-name", "job-name"} {
		if value, exists := labels[key]; exists {
			if value != job.Name {
				return fmt.Errorf("validator Job %s security template changed at admission", job.Name)
			}
			delete(labels, key)
		}
	}
	for _, key := range []string{"batch.kubernetes.io/controller-uid", "controller-uid"} {
		if value, exists := labels[key]; exists {
			if value == "" || (created.UID != "" && value != string(created.UID)) {
				return fmt.Errorf("validator Job %s security template changed at admission", job.Name)
			}
			delete(labels, key)
		}
	}
	if created.Name != job.Name || created.Namespace != job.Namespace || !reflect.DeepEqual(created.Spec.Template.Spec, expected.Spec.Template.Spec) || !reflect.DeepEqual(labels, expected.Spec.Template.Labels) || !reflect.DeepEqual(created.Spec.Template.Annotations, expected.Spec.Template.Annotations) {
		return fmt.Errorf("validator Job %s security template changed at admission", job.Name)
	}
	return nil
}

func (k KubernetesValidationJobClient) waitJob(ctx context.Context, name string) error {
	ticker := time.NewTicker(k.PollInterval)
	defer ticker.Stop()
	track := k.OnPhase != nil && !strings.HasSuffix(name, "-allow") && !strings.HasSuffix(name, "-deny")
	started := false
	for {
		job, err := k.Client.BatchV1().Jobs(k.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			// Inspection is read-only. Retry temporary API/etcd failures on the
			// same Job within the existing validation deadline; they say nothing
			// about whether the candidate passed validation.
			if apierrors.IsServiceUnavailable(err) || apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsInternalError(err) || utilnet.IsTimeout(err) {
				select {
				case <-ctx.Done():
					return fmt.Errorf("inspect Job %s: %v: %w", name, err, ctx.Err())
				case <-ticker.C:
					continue
				}
			}
			return fmt.Errorf("inspect Job %s: %w", name, err)
		}
		if track && !started && (job.Status.Active > 0 || job.Status.StartTime != nil || job.Status.Succeeded > 0) {
			if err := k.OnPhase(ctx, "validator_job_started"); err != nil {
				return err
			}
			started = true
		}
		if job.Status.Succeeded > 0 && job.Status.CompletionTime != nil {
			if track {
				if err := k.OnPhase(ctx, "validator_job_finished"); err != nil {
					return err
				}
				k.logStages(name)
			}
			return nil
		}
		if job.Status.Failed > 0 {
			failure := fmt.Errorf("Job %s failed", name)
			if logs := k.jobLogs(name); logs != "" {
				if !strings.HasSuffix(name, "-allow") && !strings.HasSuffix(name, "-deny") {
					logStages(name, logs)
					if ds, err := validate.ParseReport(logs); err == nil && len(ds) > 0 {
						return validate.Err(ds)
					}
				}
				if len(logs) > 8192 {
					logs = logs[:8192]
				}
				return fmt.Errorf("%w; Pod logs: %s", failure, logs)
			}
			return failure
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Job %s did not complete: %w", name, ctx.Err())
		case <-ticker.C:
		}
	}
}

// logStages reports where the validator Job's wall time went: the Go
// toolchain stages (the "test" stage includes corpus replay) it ran.
func (k KubernetesValidationJobClient) logStages(name string) {
	logStages(name, k.jobLogs(name))
}

func logStages(name, logs string) {
	for _, timing := range validate.ParseStageTimings(logs) {
		slog.Info("validator_stage", "job", name, "stage", timing.Stage, "duration_ms", timing.Duration.Milliseconds())
	}
}

func (k KubernetesValidationJobClient) jobLogs(name string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pods, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{LabelSelector: "batch.kubernetes.io/job-name=" + name})
	if err != nil || len(pods.Items) == 0 {
		return ""
	}
	pod := pods.Items[0]
	stream, err := k.Client.CoreV1().Pods(k.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{}).Stream(ctx)
	if err != nil {
		return ""
	}
	defer stream.Close()
	contents, err := io.ReadAll(io.LimitReader(stream, int64(validate.MaxReportBytes+len(validate.ReportPrefix)+2)))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(contents))
}

func (k KubernetesValidationJobClient) cleanup(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	zero := int64(0)
	foreground := metav1.DeletePropagationForeground
	for _, job := range []string{name, name + "-allow", name + "-deny"} {
		err := k.Client.BatchV1().Jobs(k.Namespace).Delete(ctx, job, metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &foreground})
		if err != nil && !apierrors.IsNotFound(err) {
			return // The Job may still run; the shared deny policy stays in place.
		}
	}
	for _, job := range []string{name, name + "-allow", name + "-deny"} {
		for {
			_, err := k.Client.BatchV1().Jobs(k.Namespace).Get(ctx, job, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				break
			}
			if err != nil || ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

func (k KubernetesValidationJobClient) canaryJob(jobName string, denied bool) *batchv1.Job {
	labels := map[string]string{"sdo.dev/validator-canary": "allow"}
	mode := "allow"
	if denied {
		labels["sdo.dev/validator-canary"] = "deny"
		labels["sdo.dev/validator-isolation"] = validatorIsolationPolicy
		mode = "deny"
	}
	job := k.baseJob(jobName, labels, 30)
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: "network-canary", Image: k.Image, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/usr/local/bin/sdo-operator", "network-canary", mode}, TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile, SecurityContext: restrictedContainer(), Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("32Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("64Mi")}}}}
	return job
}

func (k KubernetesValidationJobClient) validationJob(name, subPath, tree string, useBaseline bool) *batchv1.Job {
	job := k.baseJob(name, map[string]string{"sdo.dev/validator-isolation": validatorIsolationPolicy}, 600)
	job.Spec.Template.Spec.Volumes = []corev1.Volume{
		{Name: "application", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: k.RepositoryPVC, ReadOnly: true}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: resourcePointer("3Gi")}}},
	}
	// Recompute the staged tree using a scratch index and object directory. Git
	// may refresh the index or write tree objects, while the candidate PVC stays
	// read-only. A mismatch fails before untrusted detector code is executed.
	command := validatorTreeCheckCommand("/workspace", "/tmp") + `
/usr/local/bin/sdo-operator validate --app /workspace --expect-schema "$SDO_SCHEMA_IDENTITY"`
	if useBaseline {
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, corev1.Volume{
			Name: "healthy-baseline", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: k.RepositoryPVC, ReadOnly: true}},
		})
		command += " --healthy-baseline /sdo/healthy-baseline"
	}
	job.Spec.Template.Spec.Containers = []corev1.Container{{
		Name: "validator", Image: k.Image, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"/bin/sh", "-eu", "-c", command}, WorkingDir: "/workspace", TerminationMessagePath: "/dev/termination-log", TerminationMessagePolicy: corev1.TerminationMessageReadFile,
		Env: []corev1.EnvVar{
			{Name: "SDO_CANDIDATE_TREE", Value: tree}, {Name: "SDO_SCHEMA_IDENTITY", Value: k.SchemaIdentity},
			{Name: "HOME", Value: "/tmp"}, {Name: "GOCACHE", Value: "/tmp/go-cache"}, {Name: "SDO_GO_CACHE_SEED", Value: "/go/build-cache"}, {Name: "GOCACHEPROG", Value: "/usr/local/bin/sdo-go-cache"},
			{Name: "GOMODCACHE", Value: "/go/pkg/mod"}, {Name: "GOPROXY", Value: "off"}, {Name: "GOSUMDB", Value: "off"},
			{Name: "GOMAXPROCS", Value: "2"}, {Name: "GOFLAGS", Value: "-mod=mod -p=2"}, {Name: "PYTHONDONTWRITEBYTECODE", Value: "1"},
		},
		VolumeMounts:    []corev1.VolumeMount{{Name: "application", MountPath: "/workspace", SubPath: subPath, ReadOnly: true}, {Name: "scratch", MountPath: "/tmp"}},
		SecurityContext: restrictedContainer(),
		Resources:       corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("512Mi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("3Gi")}},
	}}
	if useBaseline {
		job.Spec.Template.Spec.Containers[0].VolumeMounts = append(job.Spec.Template.Spec.Containers[0].VolumeMounts,
			corev1.VolumeMount{Name: "healthy-baseline", MountPath: "/sdo/healthy-baseline", SubPath: filepath.ToSlash(filepath.Join(k.RepositoryPVCSubPath, ".sdo-baseline/healthy")), ReadOnly: true})
	}
	if devmount.Enabled() {
		devmount.Apply(&job.Spec.Template.Spec)
	}
	return job
}

func baselineGateRequired(candidate string, changedPaths []string) (bool, error) {
	return validate.BaselineGateRequired(candidate, changedPaths)
}

func validatorTreeCheckCommand(root, scratch string) string {
	return fmt.Sprintf(`index=$(git -C %q rev-parse --path-format=absolute --git-path index)
objects=$(git -C %q rev-parse --path-format=absolute --git-path objects)
cp "$index" %q
mkdir -p %q
test "$(GIT_INDEX_FILE=%q GIT_OBJECT_DIRECTORY=%q GIT_ALTERNATE_OBJECT_DIRECTORIES="$objects" git -C %q write-tree)" = "$SDO_CANDIDATE_TREE"`,
		root, root, filepath.Join(scratch, "validator-index"), filepath.Join(scratch, "validator-objects"), filepath.Join(scratch, "validator-index"), filepath.Join(scratch, "validator-objects"), root)
}

func (k KubernetesValidationJobClient) baseJob(name string, labels map[string]string, deadline int64) *batchv1.Job {
	zero := int32(0)
	ttl := int32(300)
	no := false
	yes := true
	uid := int64(65532)
	terminationGrace := int64(30)
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: k.Namespace}, Spec: batchv1.JobSpec{
		BackoffLimit: &zero, TTLSecondsAfterFinished: &ttl, ActiveDeadlineSeconds: &deadline,
		Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: &terminationGrace, DNSPolicy: corev1.DNSClusterFirst, SchedulerName: "default-scheduler", AutomountServiceAccountToken: &no, EnableServiceLinks: &no,
			SecurityContext: &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &uid, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		}},
	}}
}

func restrictedContainer() *corev1.SecurityContext {
	no := false
	yes := true
	return &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
}

func resourcePointer(value string) *resource.Quantity {
	quantity := resource.MustParse(value)
	return &quantity
}
