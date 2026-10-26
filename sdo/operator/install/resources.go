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
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/devmount"
	"sdo.dev/operator/memory/observation"
	"sdo.dev/operator/sdk"
)

const (
	ControllerJobName            = "sdo-controller-run"
	RepositorySyncPodName        = "sdo-repository-sync"
	InstallFingerprintAnnotation = "sdo.dev/install-fingerprint"
	WorkerMountPolicyAnnotation  = "sdo.dev/worker-mount-policy"
	ApplicationNamespaceLabel    = "sdo.dev/application-namespace"
	ControllerNamespaceLabel     = "sdo.dev/controller-namespace"
)

type Config struct {
	ResponderMenu                                   bool
	ObservationCeiling                              sdk.ObservationScope
	Namespace, ControlNamespace, Application        string
	ControllerImage, ResponderImage, ValidatorImage string
	ScriptImage                                     string
	RepositoryPVC, CredentialsSecret, Model         string
	AcceptedCommit                                  string
	RepairPolicy, AgentProvider                     string
	ReflectionSession, ReflectionGuidance           string
	ForceReflection                                 bool
	LateFindings                                    string
	MaxResponseLoops                                int
	HealthyBaseline                                 bool
	TimeoutSeconds, VerificationTimeoutSeconds      int
	ControllerDurationSeconds                       int
	ExitAfterClosure                                bool
	NativeCommand, NativeArgs                       []string
	ExtraControllerArgs                             []string
	ResponderEnv                                    []string
	ResponderStandby                                *bool
	Credentials                                     map[string][]byte
	RegistrationConfigMap, RegistrationDirectory    string
	RegistrationData                                map[string]string
	BuildKitImage, RegistryPushSecret               string
	RegistryHost                                    string
	RegistryInsecure                                bool
	RegistryPushDockerConfig                        []byte
	ReuseExisting, WaitForCompletion                bool
	PermissiveResponder                             bool
	ResponderNamespaces                             []string
	MetricsURL                                      string
}

type Plan struct {
	ObservationRoles             []*rbacv1.Role
	ObservationBindings          []*rbacv1.RoleBinding
	ObservationClusterRoles      []*rbacv1.ClusterRole
	ObservationClusterBindings   []*rbacv1.ClusterRoleBinding
	Namespace                    *corev1.Namespace
	PVC                          *corev1.PersistentVolumeClaim
	ServiceAccounts              []*corev1.ServiceAccount
	ControlRoles, AppRoles       []*rbacv1.Role
	ControlBindings, AppBindings []*rbacv1.RoleBinding
	ClusterRoles                 []*rbacv1.ClusterRole
	ClusterBindings              []*rbacv1.ClusterRoleBinding
	SyncPod                      *corev1.Pod
	Job                          *batchv1.Job
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
func (c Config) validate() error {

	if err := c.ObservationCeiling.Validate(); err != nil {
		return err
	}
	for name, value := range map[string]string{"namespace": c.Namespace, "control namespace": c.controlNamespace(), "application": c.Application, "repository PVC": c.RepositoryPVC, "credentials Secret": c.CredentialsSecret} {
		if !namespaceName.MatchString(value) {
			return fmt.Errorf("%s must be a Kubernetes DNS label: %q", name, value)
		}
	}
	if c.ControllerImage == "" || c.ResponderImage == "" || c.ValidatorImage == "" {
		return errors.New("controller, responder, and validator images are required")
	}
	if strings.TrimSpace(c.Model) == "" {
		return errors.New("agent model is required")
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
	if c.TimeoutSeconds <= 0 {
		return errors.New("controller timeout must be positive")
	}
	if c.ControllerDurationSeconds < 0 {
		return errors.New("controller duration must be nonnegative")
	}
	if c.VerificationTimeoutSeconds < 0 {
		return errors.New("verification timeout cannot be negative")
	}
	if !commitID.MatchString(c.AcceptedCommit) {
		return errors.New("native install requires a full accepted commit ID")
	}
	if c.RepairPolicy != "" && !slices.Contains(wire.StringIn(&contracts.ControllerConfig{}, "repair_policy"), c.RepairPolicy) {
		return errors.New("invalid repair policy")
	}
	if c.AgentProvider != "" && c.AgentProvider != "codex" && c.AgentProvider != "claude" {
		return errors.New("invalid agent provider")
	}
	if c.ReflectionSession != "" && !slices.Contains(wire.StringIn(&contracts.ControllerConfig{}, "reflection_session"), c.ReflectionSession) {
		return errors.New("invalid reflection session")
	}
	if c.ReflectionGuidance != "" && !slices.Contains(wire.StringIn(&contracts.ControllerConfig{}, "reflection_guidance"), c.ReflectionGuidance) {
		return errors.New("invalid reflection guidance")
	}
	if c.LateFindings != "" && c.LateFindings != "off" && c.LateFindings != "pull" {
		return errors.New("invalid late findings mode")
	}
	if c.MaxResponseLoops < 0 {
		return errors.New("max response loops cannot be negative")
	}

	for _, arg := range c.ExtraControllerArgs {
		if strings.Contains(arg, "broker-mode") || strings.Contains(arg, "worker-mount-mode") || strings.HasPrefix(arg, "--broker-arg") {
			return errors.New("controller extension cannot change broker or worker mount mode")
		}
	}
	for _, value := range c.ResponderEnv {
		name, content, found := strings.Cut(value, "=")
		if !found || !environmentName.MatchString(name) || strings.ContainsAny(content, "\r\n") {
			return fmt.Errorf("invalid responder environment %q", value)
		}
		if name == observation.CeilingEnvironment {
			return errors.New("observation ceiling is installer-owned")
		}
		if name == "SDO_LATE_FINDINGS" {
			return errors.New("duplicate SDO_LATE_FINDINGS responder setting; use LateFindings")
		}
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

func Resources(c Config, resources ...*metav1.APIResourceList) (Plan, error) {
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
	controllerRules := []rbacv1.PolicyRule{
		// Kubernetes RBAC cannot distinguish metadata-only reads from full objects.
		// The baseline negotiates metadata strictly and never needs Secret get.
		rule("", []string{"secrets"}, "list", "watch"),
		rule("", []string{"configmaps"}, "get", "list", "watch", "create", "update", "delete"),
		rule("", []string{"services", "pods", "endpoints", "events"}, "get", "list", "watch"),
		rule("", []string{"pods/log"}, "get"),
		rule("", []string{"pods", "services"}, "create", "delete"),
		rule("apps", []string{"deployments", "replicasets", "statefulsets", "daemonsets"}, "get", "list", "watch"),
		rule("autoscaling", []string{"horizontalpodautoscalers"}, "get", "list"),
		rule("policy", []string{"poddisruptionbudgets"}, "get", "list"),
		rule("discovery.k8s.io", []string{"endpointslices"}, "get", "list", "watch"),
		rule("networking.k8s.io", []string{"networkpolicies"}, "get", "list", "watch", "create", "update", "delete"),
		rule("rbac.authorization.k8s.io", []string{"roles", "rolebindings"}, "get", "list", "watch"),
		rule("coordination.k8s.io", []string{"leases"}, "get", "create", "update", "patch"),
		rule("batch", []string{"jobs"}, "get", "list", "watch", "create", "delete"),
	}
	responderRules := []rbacv1.PolicyRule{
		rule("", []string{"configmaps", "services", "pods", "persistentvolumeclaims"}, "get", "list", "watch", "create", "update", "patch", "delete"),
		rule("", []string{"pods/log", "events", "endpoints"}, "get", "list", "watch"),
		rule("", []string{"pods/exec", "pods/attach", "pods/portforward"}, "get", "create"),
		rule("apps", []string{"deployments", "statefulsets", "daemonsets", "replicasets"}, "get", "list", "watch", "create", "update", "patch", "delete"),
		rule("autoscaling", []string{"horizontalpodautoscalers"}, "get", "list"),
		rule("policy", []string{"poddisruptionbudgets"}, "get", "list"),
		rule("networking.k8s.io", []string{"networkpolicies", "ingresses"}, "get", "list", "watch", "create", "update", "patch", "delete"),
		rule("discovery.k8s.io", []string{"endpointslices"}, "get", "list", "watch"),
		rule("batch", []string{"jobs"}, "get", "list", "watch", "create", "update", "patch", "delete"),
	}
	if split {
		plan.ControlRoles = append(plan.ControlRoles, role(control, "sdo-controller", controllerRules), role(control, "sdo-operate", controllerRules), role(control, "sdo-responder", []rbacv1.PolicyRule{rule("", []string{"configmaps"}, "get", "list", "watch", "create", "update", "patch")}))
		readRules := []rbacv1.PolicyRule{}
		for _, r := range controllerRules {
			if r.APIGroups[0] == "coordination.k8s.io" || r.APIGroups[0] == "batch" {
				continue
			}
			if len(r.Resources) == 1 && r.Resources[0] == "secrets" {
				readRules = append(readRules, r)
				continue
			}
			readRules = append(readRules, rule(r.APIGroups[0], r.Resources, "get", "list", "watch"))
		}
		readRules = append(readRules, programApplyRules()...)
		readRules = append(readRules, adoptionRules()...)
		plan.AppRoles = append(plan.AppRoles, role(c.Namespace, "sdo-controller", readRules), role(c.Namespace, "sdo-operate", responderRules), role(c.Namespace, "sdo-responder", responderRules), role(c.Namespace, "sdo-controller-helper-cleanup", []rbacv1.PolicyRule{rule("", []string{"pods"}, "list", "delete"), rule("batch", []string{"jobs"}, "list", "delete")}))
		for _, name := range []string{"sdo-controller", "sdo-operate", "sdo-responder", "sdo-controller-helper-cleanup"} {
			subject := name
			if name == "sdo-controller-helper-cleanup" {
				subject = "sdo-controller"
			}
			plan.AppBindings = append(plan.AppBindings, binding(c.Namespace, name, subject, control))
		}
	} else {
		controllerWrites := append(append([]rbacv1.PolicyRule{}, controllerRules...), programApplyRules()...)
		controllerWrites = append(controllerWrites, adoptionRules()...)
		plan.ControlRoles = append(plan.ControlRoles, role(control, "sdo-controller", controllerWrites), role(control, "sdo-operate", append(append([]rbacv1.PolicyRule{}, controllerRules...), responderRules...)), role(control, "sdo-responder", responderRules))
	}
	for _, name := range []string{"sdo-controller", "sdo-operate", "sdo-responder"} {
		plan.ControlBindings = append(plan.ControlBindings, binding(control, name, name, control))
	}
	if err := addPermissiveGrants(c, &plan, resources); err != nil {
		return Plan{}, err
	}
	if err := addCaptureGrants(c, &plan, resources); err != nil {
		return Plan{}, err
	}
	if err := addUnexplainedRemovalGrants(c, &plan, resources); err != nil {
		return Plan{}, err
	}
	addObservationGrants(&plan, c)
	addScriptGrants(c, &plan)
	plan.SyncPod = syncPod(c)
	plan.Job = controllerJob(c)
	if c.AcceptedCommit != "" {
		plan.Job.Spec.Template.Annotations = map[string]string{"sdo.dev/accepted-commit": c.AcceptedCommit}
	}
	if c.PermissiveResponder {
		settings, err := json.Marshal(c.ResponderNamespaces)
		if err != nil {
			return Plan{}, err
		}
		if plan.Job.Spec.Template.Annotations == nil {
			plan.Job.Spec.Template.Annotations = map[string]string{}
		}
		plan.Job.Spec.Template.Annotations[PermissiveResponderAnnotation] = string(settings)
	}

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

func programApplyRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		rule("", []string{"configmaps", "services", "pods"}, "get", "patch", "create", "delete"),
		rule("autoscaling", []string{"horizontalpodautoscalers"}, "get", "patch", "create", "delete"),
		rule("policy", []string{"poddisruptionbudgets"}, "get", "patch", "create", "delete"),
		rule("apps", []string{"deployments", "statefulsets"}, "get", "patch", "create", "delete"),
		rule("apps", []string{"daemonsets"}, "get", "patch"),
		rule("apps", []string{"replicasets", "controllerrevisions"}, "get", "list"),
		rule("networking.k8s.io", []string{"networkpolicies"}, "get", "patch", "create", "delete"),
	}
}

func adoptionRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{
		rule("", []string{"configmaps", "pods", "endpoints"}, "get", "patch"),
		rule("apps", []string{"replicasets"}, "get", "patch"),
		rule("discovery.k8s.io", []string{"endpointslices"}, "get", "patch"),
		rule("networking.k8s.io", []string{"networkpolicies"}, "get", "patch"),
	}
}

func rule(group string, resources []string, verbs ...string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{APIGroups: []string{group}, Resources: resources, Verbs: verbs}
}
func role(namespace, name string, rules []rbacv1.PolicyRule) *rbacv1.Role {
	return &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Rules: rules}
}
func binding(namespace, name, subject, subjectNamespace string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: subject, Namespace: subjectNamespace}}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: name}}
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

func controllerJob(c Config) *batchv1.Job {
	control := c.controlNamespace()
	args := []string{
		"controller", "--app", "/workspace/application", "--namespace", c.Namespace,
		"--application", c.Application, "--responder-image", c.ResponderImage,
		"--toolchain-image", c.ControllerImage, "--prober-image", c.ControllerImage, "--repository-pvc", c.RepositoryPVC,
		"--repository-mount-path", "/workspace", "--credentials-secret", c.CredentialsSecret,
		"--worktree-root", "/workspace/worktrees", "--response-timeout", fmt.Sprintf("%ds", c.TimeoutSeconds),
		"--verification-timeout", fmt.Sprintf("%ds", verificationTimeout(c)),
	}
	if control != c.Namespace {
		args = append(args, "--control-namespace", control)
	}
	command := append([]string{}, c.NativeCommand...)
	provider := c.AgentProvider
	if provider == "" {
		provider = "codex"
	}
	if c.ScriptImage != "" {
		args = append(args, "--script-image", c.ScriptImage)
	}
	args = append(args, "--broker-mode=native", "--worker-mount-mode=proposal-only", "--validator-image", c.ValidatorImage, "--model", c.Model, "--responder-backend", provider)
	args = append(args, "--reflection-session", reflectionSession(c))
	args = append(args, "--reflection-guidance", reflectionGuidance(c))
	if c.ForceReflection {
		args = append(args, "--force-reflection")
	}
	if c.MaxResponseLoops > 0 {
		args = append(args, "--max-response-loops", fmt.Sprint(c.MaxResponseLoops))
	}

	if c.RepairPolicy != "" {
		args = append(args, "--repair-policy", c.RepairPolicy)
	}
	for _, value := range c.ResponderEnv {
		args = append(args, "--responder-env="+value)
	}
	if c.LateFindings == "pull" {
		args = append(args, "--responder-env=SDO_LATE_FINDINGS=pull")
	}
	if c.ResponderStandby != nil {
		args = append(args, fmt.Sprintf("--responder-standby=%t", *c.ResponderStandby))
	}
	args = append(args, c.NativeArgs...)
	args = append(args, c.ExtraControllerArgs...)
	if c.ControllerDurationSeconds > 0 {
		args = append(args, "--duration", fmt.Sprintf("%ds", c.ControllerDurationSeconds))
	}
	if c.ExitAfterClosure {
		args = append(args, "--exit-after-closure")
	}
	if c.HealthyBaseline {
		args = append(args, "--healthy-baseline")
	}
	if c.MetricsURL != "" {
		args = append(args, "--metrics-url", c.MetricsURL)
	}
	if c.RegistrationConfigMap != "" {
		args = append(args, "--agent-registration-directory", c.registrationDirectory())
	}
	if !hasArg(args, "--exit-after-closure") && !hasArg(args, "--duration") {
		args = append(args, "--supervise")
	}
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
	menu := responderMenuValue(c.ResponderMenu)
	pod.Containers = []corev1.Container{{
		Name: "controller", Image: c.ControllerImage, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: command, Args: args,
		EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: c.CredentialsSecret}}}},
		Env: []corev1.EnvVar{
			{Name: "SDO_RESPONDER_MENU", Value: menu},
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
		Resources:       limits("250m", "512Mi", "2", "4Gi"),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "repository", MountPath: "/workspace"},
			{Name: "scratch", MountPath: "/tmp"},
			{Name: "credentials", MountPath: "/sdo/credentials", ReadOnly: true},
		},
	}}
	if c.RegistrationConfigMap != "" {
		pod.Containers[0].VolumeMounts = append(pod.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "registrations", MountPath: c.registrationDirectory(), ReadOnly: true})
	}
	if devmount.Enabled() {
		pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: "SDO_DEV_MOUNT", Value: "1"})
		devmount.Apply(pod)
	}
	if scale := os.Getenv("SDO_TIME_SCALE"); scale != "" {
		pod.Containers[0].Env = append(pod.Containers[0].Env, corev1.EnvVar{Name: "SDO_TIME_SCALE", Value: scale})
	}
	return job
}
func (c Config) registrationDirectory() string {
	if c.RegistrationDirectory != "" {
		return c.RegistrationDirectory
	}
	return "/sdo/registrations"
}
func verificationTimeout(c Config) int {
	if c.VerificationTimeoutSeconds > 0 {
		return c.VerificationTimeoutSeconds
	}
	return 300
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
