// Package install owns Kubernetes resources for a production SDO controller.
// It never gives worker Jobs a mount of the accepted checkout; the worker
// registry derives a proposal-only subPath from each broker-issued task.
package install

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sdo.dev/operator/contracts/settings"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/deploymentpolicy"
	"sdo.dev/operator/devmount"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/worker_registry"
)

const (
	ControllerJobName            = "sdo-controller-run"
	RepositorySyncPodName        = "sdo-repository-sync"
	RuntimeExportPodName         = "sdo-runtime-export"
	InstallFingerprintAnnotation = "sdo.dev/install-fingerprint"
	WorkerMountPolicyAnnotation  = "sdo.dev/worker-mount-policy"
	ApplicationNamespaceLabel    = "sdo.dev/application-namespace"
	ControllerNamespaceLabel     = "sdo.dev/controller-namespace"
)

type Config struct {
	// Settings is the resolved operator configuration, required: every timing and policy value. The installer
	// renders it as the sdo-config ConfigMap, mounts it into the operate and controller Jobs (no policy value crosses
	// their argv) and binds its hash into the install fingerprint.
	Settings *settings.Config

	ObservationCeiling                           sdk.ObservationScope
	Namespace, ControlNamespace, Application     string
	ControllerImage, ResponderImage              string
	ScriptImage                                  string
	RepositoryPVC, CredentialsSecret             string
	AcceptedCommit                               string
	ControllerDurationSeconds                    int
	ExitAfterClosure                             bool
	NativeCommand, NativeArgs                    []string
	ExtraControllerArgs                          []string
	Credentials                                  map[string][]byte
	RegistrationConfigMap, RegistrationDirectory string
	RegistrationData                             map[string]string
	BuildKitImage, RegistryPushSecret            string
	RegistryHost                                 string
	// DeploymentPolicy is the install-time deployment policy as compact
	// ProtoJSON (deploymentpolicy.Format); empty is no policy (today's
	// behavior). The controller and the operate Job receive it as
	// --deployment-policy.
	DeploymentPolicy                 string
	RegistryInsecure                 bool
	RegistryPushDockerConfig         []byte
	ReuseExisting, WaitForCompletion bool
	// TelemetryProfile is the environment's statement of application
	// telemetry; the installer applies its recipes and hands its environment
	// to the controller. The zero profile applies and provides nothing.
	TelemetryProfile telemetry.Profile
	// BackendRecipes are the SDO-owned telemetry backends the installer may
	// apply, injected by the operator CLI wiring.
	BackendRecipes []telemetry.BackendRecipe
	// MemoryUpstream replicates accepted memory through a shared remote
	// branch. MemoryUpstreamSSHSecret names an existing Secret holding the
	// ssh "identity" and "known_hosts", mounted read-only.
	MemoryUpstreamURL, MemoryUpstreamBranch, MemoryUpstreamSSHSecret string
	// DeploymentPlan is a provided deployment (compact ProtoJSON
	// DeploymentPlan): the operate Job deploys it instead of asking the
	// deployer agent for a plan.
	DeploymentPlan string
	// EvidenceSink is the runner's evidence sink as compact JSON (EvidenceSink.Format); empty keeps evidence only on
	// the repository volume. The controller and operate Pods then run the evidence sidecar.
	EvidenceSink string
}

type Plan struct {
	// Backend holds the objects of the applied telemetry backend recipe.
	Backend         telemetry.BackendObjects
	Namespace       *corev1.Namespace
	PVC             *corev1.PersistentVolumeClaim
	ServiceAccounts []*corev1.ServiceAccount
	ClusterRoles    []*rbacv1.ClusterRole
	ClusterBindings []*rbacv1.ClusterRoleBinding
	SyncPod         *corev1.Pod
	Job             *batchv1.Job

	// TelemetryEnvironment is the controller's telemetry environment document.
	TelemetryEnvironment *corev1.ConfigMap
	// Settings is the sdo-config ConfigMap.
	Settings *corev1.ConfigMap
}

var namespaceName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
var pinnedImage = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var registryDNSLabel = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

func validRegistryHost(value string) bool {
	host, port, hasPort := strings.Cut(value, ":")
	if hasPort {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
			return false
		}
	}
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) > 63 || !registryDNSLabel.MatchString(label) {
			return false
		}
	}
	return true
}

func (c Config) controlNamespace() string {
	if c.ControlNamespace != "" {
		return c.ControlNamespace
	}
	return c.Namespace
}

// CodePathSelection is the code path the resolved configuration selects (nil for plain go).
func (c Config) CodePathSelection() *contracts.CodePathConfig {
	if c.Settings == nil {
		return nil
	}
	selection, _ := c.Settings.Agent.CodePathSelection() // validated with the configuration
	return selection
}

func (c Config) validate() error {
	if c.Settings == nil {
		return errors.New("install requires the resolved operator configuration")
	}
	if err := c.Settings.Validate(); err != nil {
		return fmt.Errorf("resolved config: %w", err)
	}
	if err := c.ObservationCeiling.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]string{"namespace": c.Namespace, "control namespace": c.controlNamespace(), "application": c.Application, "repository PVC": c.RepositoryPVC, "credentials Secret": c.CredentialsSecret} {
		if !namespaceName.MatchString(value) {
			return fmt.Errorf("%s must be a Kubernetes DNS label: %q", name, value)
		}
	}
	if c.ControllerImage == "" || c.ResponderImage == "" {
		return errors.New("controller and responder images are required")
	}
	if (c.MemoryUpstreamURL == "") != (c.MemoryUpstreamBranch == "") || (c.MemoryUpstreamSSHSecret != "" && c.MemoryUpstreamURL == "") {
		return errors.New("memory upstream requires both a URL and a branch")
	}
	if c.MemoryUpstreamSSHSecret != "" && !namespaceName.MatchString(c.MemoryUpstreamSSHSecret) {
		return fmt.Errorf("memory upstream ssh Secret must be a Kubernetes DNS label: %q", c.MemoryUpstreamSSHSecret)
	}
	if c.RegistrationConfigMap != "" && !namespaceName.MatchString(c.RegistrationConfigMap) {
		return errors.New("registration ConfigMap must be a Kubernetes DNS label")
	}
	if c.RegistrationConfigMap == "" && c.RegistrationDirectory != "" {
		return errors.New("registration directory requires a ConfigMap")
	}
	if c.RegistrationConfigMap == "" && len(c.RegistrationData) > 0 {
		return errors.New("registration data requires a ConfigMap")
	}
	if c.RegistrationDirectory != "" && !strings.HasPrefix(c.RegistrationDirectory, "/sdo/") {
		return errors.New("registration directory must be under /sdo")
	}
	if c.RegistryPushSecret != "" && !namespaceName.MatchString(c.RegistryPushSecret) {
		return errors.New("registry push Secret must be a Kubernetes DNS label")
	}
	if c.RegistryPushSecret != "" && c.RegistryPushSecret == c.CredentialsSecret {
		return errors.New("registry push Secret must differ from agent credentials Secret")
	}
	if c.RegistryHost != "" && !validRegistryHost(c.RegistryHost) {
		return errors.New("registry host must be lowercase DNS or IPv4 host with optional port")
	}
	if c.RegistryPushSecret == "" && len(c.RegistryPushDockerConfig) > 0 {
		return errors.New("registry push data requires a Secret name")
	}
	if c.BuildKitImage != "" && !pinnedImage.MatchString(c.BuildKitImage) {
		return errors.New("BuildKit image must be pinned by sha256 digest")
	}
	if c.ControllerDurationSeconds < 0 {
		return errors.New("controller duration must be nonnegative")
	}
	if !commitID.MatchString(c.AcceptedCommit) {
		return errors.New("native install requires a full accepted commit ID")
	}
	if _, err := deploymentpolicy.Parse(c.DeploymentPolicy); err != nil {
		return err
	}
	if _, err := ParseEvidenceSink(c.EvidenceSink); err != nil {
		return err
	}
	if err := checkExtraControllerArgs(c.ExtraControllerArgs); err != nil {
		return err
	}
	if err := c.validatePermissive(); err != nil {
		return err
	}
	if len(c.NativeCommand) == 0 || strings.TrimSpace(c.NativeCommand[0]) == "" {
		return errors.New("native controller entrypoint is required")
	}
	for _, arg := range append(append([]string{}, c.NativeCommand...), c.NativeArgs...) {
		if strings.Contains(arg, "broker-mode") || strings.Contains(arg, "worker-mount-mode") || strings.HasPrefix(arg, "--broker-arg") {
			return errors.New("native broker and worker mount modes are installer-owned")
		}
	}
	return nil
}

func Resources(c Config) (Plan, error) {
	if err := c.validate(); err != nil {
		return Plan{}, err
	}
	control := c.controlNamespace()
	split := control != c.Namespace
	plan := Plan{PVC: &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: c.RepositoryPVC, Namespace: control}, Spec: corev1.PersistentVolumeClaimSpec{AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}, Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")}}}}}
	if split {
		plan.Namespace = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: control, Labels: map[string]string{"app.kubernetes.io/managed-by": "sdo", ControllerNamespaceLabel: "true", ApplicationNamespaceLabel: c.Namespace}}}
	}
	for _, name := range []string{"sdo-controller", "sdo-operate", "sdo-responder", "sdo-image-builder"} {
		plan.ServiceAccounts = append(plan.ServiceAccounts, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: control}})
	}
	plan.ServiceAccounts = append(plan.ServiceAccounts, &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "sdo-script", Namespace: c.Namespace}})
	addAdminGrant(c, &plan)
	if err := addObservability(&plan, c); err != nil {
		return Plan{}, err
	}
	environment, err := telemetryEnvironmentConfigMap(c)
	if err != nil {
		return Plan{}, err
	}
	plan.TelemetryEnvironment = environment
	if plan.Settings, err = settingsConfigMap(*c.Settings, control); err != nil {
		return Plan{}, err
	}
	plan.SyncPod = syncPod(c)
	plan.Job = controllerJob(c)
	if err := worker_registry.ApplyProviderCapacity(&plan.Job.Spec.Template.Spec, worker_registry.ProviderCapacityEnvironment()); err != nil {
		return Plan{}, err
	}
	// The document is mounted, not an argument: its digest on the template
	// makes a changed environment a changed install.
	digest := sha256.Sum256([]byte(environment.Data[telemetryEnvironmentFile]))
	plan.Job.Spec.Template.Annotations = map[string]string{"sdo.dev/telemetry-environment-hash": hex.EncodeToString(digest[:8])}
	mountSettings(&plan.Job.Spec.Template.Spec)
	plan.Job.Spec.Template.Annotations[SettingsHashAnnotation] = settingsHash(plan.Settings)
	if c.AcceptedCommit != "" {
		plan.Job.Spec.Template.Annotations["sdo.dev/accepted-commit"] = c.AcceptedCommit
	}
	if c.Settings.Response.Permissive {
		namespaces, err := json.Marshal(c.Settings.Response.PermissiveNamespaces)
		if err != nil {
			return Plan{}, err
		}
		plan.Job.Spec.Template.Annotations[PermissiveResponderAnnotation] = string(namespaces)
	}

	labelInstallPlan(c, &plan)
	fingerprintJob := plan.Job.DeepCopy()
	fingerprintJob.Annotations = nil
	encoded, err := json.Marshal(fingerprintJob)
	if err != nil {
		return Plan{}, err
	}
	hash := sha256.Sum256(encoded)
	plan.Job.Annotations = map[string]string{InstallFingerprintAnnotation: hex.EncodeToString(hash[:16]), WorkerMountPolicyAnnotation: "proposal-only"}
	return plan, nil
}

func podSecurity() *corev1.PodSecurityContext {
	yes := true
	user := int64(65532)
	return &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &user, FSGroup: &user, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
}

// PodSecurityContext returns the restricted context used by installer-owned Pods.
func PodSecurityContext() *corev1.PodSecurityContext { return podSecurity() }
func containerSecurity() *corev1.SecurityContext {
	no := false
	yes := true
	return &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes, Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
}

func pvcVolume(name string) corev1.Volume {
	return corev1.Volume{Name: "repository", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: name}}}
}
func syncPod(c Config) *corev1.Pod {
	no := false
	zero := int64(0)
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: RepositorySyncPodName, Namespace: c.controlNamespace()}, Spec: corev1.PodSpec{TerminationGracePeriodSeconds: &zero, RestartPolicy: corev1.RestartPolicyNever, AutomountServiceAccountToken: &no, SecurityContext: podSecurity(), Containers: []corev1.Container{{Name: "sync", Image: c.ControllerImage, ImagePullPolicy: corev1.PullIfNotPresent, Command: []string{"sleep", "3600"}, SecurityContext: containerSecurity(), Resources: limits("50m", "64Mi", "250m", "256Mi"), VolumeMounts: []corev1.VolumeMount{{Name: "repository", MountPath: "/workspace"}, {Name: "scratch", MountPath: "/tmp"}}}}, Volumes: []corev1.Volume{pvcVolume(c.RepositoryPVC), {Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}}}
}

// runtimeExportPod transports sealed evidence archives. It is independent of
// the singleton sync Pod so delivery never blocks accepted-memory synchronization.
func runtimeExportPod(c Config) *corev1.Pod {
	pod := syncPod(c)
	pod.Name = RuntimeExportPodName
	pod.Spec.Containers[0].Name = "export"
	return pod
}

func controllerJob(c Config) *batchv1.Job {
	control := c.controlNamespace()
	args := []string{
		"controller", "--app", "/workspace/application", "--namespace", c.Namespace,
		"--application", c.Application, "--responder-image", c.ResponderImage,
		"--toolchain-image", c.ControllerImage, "--repository-pvc", c.RepositoryPVC,
		"--repository-mount-path", "/workspace", "--credentials-secret", c.CredentialsSecret,
		"--worktree-root", "/workspace/worktrees",
	}
	if control != c.Namespace {
		args = append(args, "--control-namespace", control)
	}
	command := append([]string{}, c.NativeCommand...)
	if c.ScriptImage != "" {
		args = append(args, "--script-image", c.ScriptImage)
	}
	args = append(args, "--broker-mode=native", "--worker-mount-mode=proposal-only")
	args = append(args, c.NativeArgs...)
	if c.DeploymentPolicy != "" {
		args = append(args, "--deployment-policy", c.DeploymentPolicy)
	}
	args = append(args, c.ExtraControllerArgs...)
	if c.ControllerDurationSeconds > 0 {
		args = append(args, "--duration", fmt.Sprintf("%ds", c.ControllerDurationSeconds))
	}
	if c.ExitAfterClosure {
		args = append(args, "--exit-after-closure")
	}
	args = append(args, "--telemetry-environment", TelemetryEnvironmentFile())
	if c.RegistrationConfigMap != "" {
		args = append(args, "--agent-registration-directory", c.registrationDirectory())
	}
	if c.MemoryUpstreamURL != "" {
		args = append(args, "--memory-upstream-url", c.MemoryUpstreamURL, "--memory-upstream-branch", c.MemoryUpstreamBranch)
		if c.MemoryUpstreamSSHSecret != "" {
			args = append(args, "--memory-upstream-ssh-dir", MemoryUpstreamSSHDir)
		}
	}
	args = append(args, controllerDeployArgs(c)...)
	ttl, backoff := int32(600), int32(6)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: ControllerJobName, Namespace: control},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			PodFailurePolicy: &batchv1.PodFailurePolicy{Rules: []batchv1.PodFailurePolicyRule{{
				Action:          batchv1.PodFailurePolicyActionIgnore,
				OnPodConditions: []batchv1.PodFailurePolicyOnPodConditionsPattern{{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue}},
			}}},
		},
	}
	job.Spec.Template.Labels = map[string]string{"app.kubernetes.io/name": "sdo-controller"}
	pod := &job.Spec.Template.Spec
	pod.RestartPolicy = corev1.RestartPolicyNever
	pod.ServiceAccountName = "sdo-controller"
	pod.SecurityContext = podSecurity()
	pod.Volumes = []corev1.Volume{
		pvcVolume(c.RepositoryPVC),
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: quantity("4Gi")}}},
		{Name: "credentials", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: c.CredentialsSecret}}},
	}
	pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "telemetry-environment", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: TelemetryEnvironmentName}}}})
	if c.RegistrationConfigMap != "" {
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "registrations", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: c.RegistrationConfigMap}}}})
	}
	pod.InitContainers = []corev1.Container{{
		Name: "prepare-build-cache", Image: c.ControllerImage,
		Command:         []string{"mkdir", "-p", "/workspace/.sdo-runtime/build/tmp", "/workspace/.sdo-runtime/build/go-tmp", "/workspace/.sdo-runtime/build/go-cache"},
		SecurityContext: containerSecurity(),
		Resources:       limits("10m", "16Mi", "100m", "64Mi"),
		VolumeMounts:    []corev1.VolumeMount{{Name: "repository", MountPath: "/workspace"}},
	}}
	policyJSON, _ := json.Marshal(c.ObservationCeiling)
	pod.Containers = []corev1.Container{{
		Name: "controller", Image: c.ControllerImage, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: command, Args: args,
		EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: c.CredentialsSecret}}}},
		Env: []corev1.EnvVar{
			{Name: "CODEX_HOME", Value: "/workspace/.sdo-runtime/codex"},
			{Name: "CLAUDE_CONFIG_DIR", Value: "/workspace/.sdo-runtime/claude"},
			{Name: "TMPDIR", Value: "/workspace/.sdo-runtime/build/tmp"},
			{Name: "GOTMPDIR", Value: "/workspace/.sdo-runtime/build/go-tmp"},
			{Name: "GOCACHE", Value: "/workspace/.sdo-runtime/build/go-cache"},
			{Name: observation.CeilingEnvironment, Value: string(policyJSON)},
			{Name: "SDO_GO_CACHE_SEED", Value: "/go/build-cache"},
			{Name: "SDO_TURN_USAGE_LOG", Value: "/workspace/.sdo-runtime/usage/controller-turns.jsonl"},
			{Name: "SDO_CONTROLLER_JOB", Value: ControllerJobName},
			{Name: "SDO_CONTROLLER_POD_UID", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}}},
			{Name: "GIT_AUTHOR_NAME", Value: "sdo-controller"},
			{Name: "GIT_AUTHOR_EMAIL", Value: "sdo-controller@invalid"},
			{Name: "GIT_COMMITTER_NAME", Value: "sdo-controller"},
			{Name: "GIT_COMMITTER_EMAIL", Value: "sdo-controller@invalid"},
		},
		SecurityContext: containerSecurity(),
		// Controller and memory host each use the image's two-processor Go budget.
		Resources: limits("250m", "512Mi", "4", "4Gi"),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "repository", MountPath: "/workspace"},
			{Name: "scratch", MountPath: "/tmp"},
			{Name: "credentials", MountPath: "/sdo/credentials", ReadOnly: true},
		},
	}}
	pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "telemetry-environment", MountPath: TelemetryEnvironmentDir, ReadOnly: true})
	if c.RegistrationConfigMap != "" {
		pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "registrations", MountPath: c.registrationDirectory(), ReadOnly: true})
	}
	if c.MemoryUpstreamURL != "" && c.MemoryUpstreamSSHSecret != "" {
		// Readable by the non-root controller uid; ssh accepts a key it does not own.
		mode := int32(0o444)
		pod.Volumes = append(pod.Volumes, corev1.Volume{Name: "memory-upstream-ssh", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: c.MemoryUpstreamSSHSecret, DefaultMode: &mode}}})
		pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "memory-upstream-ssh", MountPath: MemoryUpstreamSSHDir, ReadOnly: true})
	}
	// validate already parsed the sink.
	_ = evidenceSinkSidecar(c, pod)
	if devmount.Enabled() {
		pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: "SDO_DEV_MOUNT", Value: "1"})
		devmount.Apply(pod)
	}
	return job
}

// controllerDeployArgs give the controller parent the operate Job's trusted
// build and apply identity, so it can deploy upstream advances of a followed
// source. An install without a BuildKit image and push Secret (seeded or
// install-existing) passes none, and the controller does not follow.
func controllerDeployArgs(c Config) []string {
	if c.BuildKitImage == "" || c.RegistryPushSecret == "" {
		return nil
	}
	args := []string{"--buildkit-image", c.BuildKitImage, "--registry-push-secret", c.RegistryPushSecret}
	if c.RegistryHost != "" {
		args = append(args, "--registry-host", c.RegistryHost)
	}
	if c.RegistryInsecure {
		args = append(args, "--registry-insecure")
	}
	return args
}

// MemoryUpstreamSSHDir is where the controller reads the memory remote's ssh
// identity and known hosts.
const MemoryUpstreamSSHDir = "/sdo/memory-upstream-ssh"

func (c Config) registrationDirectory() string {
	if c.RegistrationDirectory != "" {
		return c.RegistrationDirectory
	}
	return "/sdo/registrations"
}
func hasArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}
func quantity(s string) *resource.Quantity { q := resource.MustParse(s); return &q }
func limits(requestCPU, requestMemory, limitCPU, limitMemory string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(requestCPU), corev1.ResourceMemory: resource.MustParse(requestMemory)}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(limitCPU), corev1.ResourceMemory: resource.MustParse(limitMemory)}}
}

// protectedControllerFlags are the controller flags whose value the installer
// owns: the deployment policy and the identity the deployer builds, pushes and
// pulls with, and the broker and worker mount modes. ExtraControllerArgs are
// appended last and Go flags are last-wins, so an extra argument naming one
// would replace the installer's value.
var protectedControllerFlags = []string{
	"deployment-policy", "broker-mode", "worker-mount-mode", "broker-arg",
	"registry-host", "registry-insecure", "registry-push-secret", "buildkit-image",
}

// ProtectedFlagError is an extra controller argument that names an
// installer-owned flag.
type ProtectedFlagError struct{ Arg string }

func (e *ProtectedFlagError) Error() string {
	return fmt.Sprintf("controller extension argument %q names an installer-owned flag; set it through the install configuration", e.Arg)
}

func checkExtraControllerArgs(args []string) error {
	for _, arg := range args {
		name := strings.TrimLeft(arg, "-")
		if name == arg {
			continue
		}
		name, _, _ = strings.Cut(name, "=")
		for _, protected := range protectedControllerFlags {
			if name == protected || (protected == "broker-arg" && strings.HasPrefix(name, protected)) {
				return &ProtectedFlagError{Arg: arg}
			}
		}
	}
	return nil
}
