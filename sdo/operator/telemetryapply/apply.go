// Package telemetryapply is the trusted applier of designer-selected telemetry
// backends. The detection designer names a recipe in backends.yaml; Go checks
// the choice against the environment's telemetry policy, builds the recipe's
// objects itself, checks them against the applier's trust rules, applies them
// into the control namespace and waits for them to become ready. The designer
// never supplies an object.
package telemetryapply

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/install"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/telemetry"
	"sdo.dev/operator/telemetryapply/internal/waitio"
)

// Reason is why an apply failed closed. Both reasons are the designer's to
// correct; anything else is an infrastructure error returned as such.
type Reason string

const (
	// ReasonDenied: the backend choice is outside what the environment's
	// policy or the applier's trust rules allow.
	ReasonDenied Reason = "denied"
	// ReasonNotReady: the applied backend did not become ready in time.
	ReasonNotReady Reason = "not_ready"
)

// Error is a typed failure of the apply phase.
type Error struct {
	Reason  Reason
	Backend string
	// Path is the binding file the designer must correct.
	Path   string
	Detail string
}

func (e *Error) Error() string {
	return fmt.Sprintf("telemetry backend %q %s: %s", e.Backend, e.Reason, e.Detail)
}

// AppliedBackend records one backend the applier deployed.
type AppliedBackend struct {
	ID     string
	Recipe string
	Uses   []string
	// Objects names what was applied, as Kind/name.
	Objects []string
}

// Applied is what one apply phase did; it is recorded in the operability
// report.
type Applied struct {
	Backends []AppliedBackend
}

// Summary is the one-line record of what was applied.
func (a Applied) Summary() string {
	parts := make([]string, 0, len(a.Backends))
	for _, b := range a.Backends {
		parts = append(parts, fmt.Sprintf("%s (recipe %s; %s)", b.ID, b.Recipe, strings.Join(b.Objects, ", ")))
	}
	return "applied telemetry backends: " + strings.Join(parts, "; ")
}

// Applier applies the backends of an accepted-candidate checkout.
type Applier struct {
	Client  kubernetes.Interface
	Policy  telemetry.Policy
	Recipes []telemetry.BackendRecipe

	PodSecurity       *corev1.PodSecurityContext
	ContainerSecurity *corev1.SecurityContext
	// ReadyTimeout bounds the wait for one backend; PollInterval is the check
	// cadence. Zero selects 2 minutes and 2 seconds. Wait replaces the timer
	// in tests.
	ReadyTimeout time.Duration
	PollInterval time.Duration
	Wait         func(context.Context, time.Duration) error
}

// NewApplier is Applier with the installer's restricted security contexts.
func NewApplier(client kubernetes.Interface, policy telemetry.Policy, recipes []telemetry.BackendRecipe) Applier {
	return Applier{Client: client, Policy: policy, Recipes: recipes, PodSecurity: install.PodSecurityContext(), ContainerSecurity: install.ContainerSecurityContext()}
}

// Apply reads the checkout's backends file and applies each backend. A
// checkout without one applies nothing and returns an empty Applied.
func (a Applier) Apply(ctx context.Context, checkout string) (Applied, error) {
	data, err := os.ReadFile(filepath.Join(checkout, layout.TelemetryBackendsFile))
	if errors.Is(err, os.ErrNotExist) {
		return Applied{}, nil
	}
	if err != nil {
		return Applied{}, fmt.Errorf("read telemetry backends: %w", err)
	}
	file, err := layout.ParseTelemetryBackends(data)
	if err != nil {
		return Applied{}, &Error{Reason: ReasonDenied, Path: layout.TelemetryBackendsFile, Detail: err.Error()}
	}
	var applied Applied
	for _, backend := range file.Backends {
		done, err := a.applyOne(ctx, backend)
		if err != nil {
			return applied, err
		}
		applied.Backends = append(applied.Backends, done)
	}
	return applied, nil
}

func (a Applier) applyOne(ctx context.Context, backend layout.TelemetryBackend) (AppliedBackend, error) {
	denied := func(format string, args ...any) error {
		return &Error{Reason: ReasonDenied, Backend: backend.ID, Path: layout.TelemetryBackendsFile, Detail: fmt.Sprintf(format, args...)}
	}
	if mode := a.Policy.EffectiveMode(); mode != telemetry.ModeOpen {
		return AppliedBackend{}, denied("the environment's telemetry mode is %s; only an open environment lets the designer deploy backends", mode)
	}
	if len(backend.Params) > 0 {
		// No recipe takes parameters yet; ignoring them silently would let the
		// designer believe a setting (retention, say) was applied.
		return AppliedBackend{}, denied("recipe %q takes no parameters, but the backend sets %s", backend.Recipe, strings.Join(slices.Sorted(maps.Keys(backend.Params)), ", "))
	}
	policyRecipe, ok := a.Policy.Recipe(backend.Recipe)
	if !ok {
		return AppliedBackend{}, denied("recipe %q is not offered by the environment", backend.Recipe)
	}
	var recipe telemetry.BackendRecipe
	for _, r := range a.Recipes {
		if r.Name() == backend.Recipe {
			recipe = r
		}
	}
	if recipe == nil {
		return AppliedBackend{}, denied("recipe %q is offered by the policy but this operator cannot build it", backend.Recipe)
	}
	for _, use := range backend.Uses {
		capability := telemetry.ClusterCapability(use)
		switch {
		case !capability.Valid():
			return AppliedBackend{}, denied("uses unknown capability %q", use)
		case !a.Policy.Granted(capability):
			return AppliedBackend{}, denied("capability %q is not granted by the environment (granted: %v)", use, a.Policy.Capabilities)
		case !slices.Contains(policyRecipe.Declares, capability):
			return AppliedBackend{}, denied("recipe %q does not declare capability %q", backend.Recipe, use)
		}
	}
	// A recipe's own reads must be granted too, whatever `uses` says.
	for _, needed := range recipe.Capabilities() {
		if !a.Policy.Granted(needed) {
			return AppliedBackend{}, denied("recipe %q needs capability %q, which the environment does not grant", backend.Recipe, needed)
		}
	}
	objects := recipe.Build(telemetry.BackendParams{ApplicationNamespace: a.Policy.ApplicationNamespace, ControlNamespace: a.Policy.ControlNamespace, PodSecurity: a.PodSecurity, ContainerSecurity: a.ContainerSecurity})
	if err := checkObjects(a.Policy.ControlNamespace, objects); err != nil {
		return AppliedBackend{}, denied("%v", err)
	}
	if err := install.ApplyBackend(ctx, a.Client, objects); err != nil {
		return AppliedBackend{}, fmt.Errorf("apply telemetry backend %q: %w", backend.ID, err)
	}
	if err := a.waitReady(ctx, backend.ID, objects.Deployments); err != nil {
		return AppliedBackend{}, err
	}
	return AppliedBackend{ID: backend.ID, Recipe: backend.Recipe, Uses: slices.Clone(backend.Uses), Objects: objectNames(objects)}, nil
}

func objectNames(o telemetry.BackendObjects) []string {
	var names []string
	for _, x := range o.ConfigMaps {
		names = append(names, "ConfigMap/"+x.Name)
	}
	for _, x := range o.ServiceAccounts {
		names = append(names, "ServiceAccount/"+x.Name)
	}
	for _, x := range o.ClusterRoles {
		names = append(names, "ClusterRole/"+x.Name)
	}
	for _, x := range o.ClusterBindings {
		names = append(names, "ClusterRoleBinding/"+x.Name)
	}
	for _, x := range o.Services {
		names = append(names, "Service/"+x.Name)
	}
	for _, x := range o.Deployments {
		names = append(names, "Deployment/"+x.Name)
	}
	return names
}

// checkObjects is the applier's trust rule for any backend object set: it
// lives in the control namespace, runs only under its own ServiceAccounts, and
// asks for no host access, privilege, unpinned image or unbounded resources.
func checkObjects(control string, o telemetry.BackendObjects) error {
	namespaced := func(kind, name, ns string) error {
		if ns != control {
			return fmt.Errorf("%s %s is in namespace %q; backends live in the control namespace %q", kind, name, ns, control)
		}
		return nil
	}
	accounts := map[string]bool{}
	for _, x := range o.ServiceAccounts {
		accounts[x.Name] = true
		if err := namespaced("ServiceAccount", x.Name, x.Namespace); err != nil {
			return err
		}
	}
	for _, x := range o.ConfigMaps {
		if err := namespaced("ConfigMap", x.Name, x.Namespace); err != nil {
			return err
		}
	}
	for _, x := range o.Services {
		if err := namespaced("Service", x.Name, x.Namespace); err != nil {
			return err
		}
	}
	for _, binding := range o.ClusterBindings {
		for _, subject := range binding.Subjects {
			if subject.Kind != "ServiceAccount" || subject.Namespace != control || !accounts[subject.Name] {
				return fmt.Errorf("ClusterRoleBinding %s binds %s %s/%s; it may bind only the recipe's own ServiceAccounts", binding.Name, subject.Kind, subject.Namespace, subject.Name)
			}
		}
	}
	for _, d := range o.Deployments {
		if err := namespaced("Deployment", d.Name, d.Namespace); err != nil {
			return err
		}
		if err := checkPod(d.Name, d.Spec.Template.Spec, accounts); err != nil {
			return err
		}
	}
	return nil
}

func checkPod(owner string, pod corev1.PodSpec, accounts map[string]bool) error {
	if pod.HostNetwork || pod.HostPID || pod.HostIPC {
		return fmt.Errorf("Deployment %s asks for host namespaces", owner)
	}
	if pod.ServiceAccountName == "" || !accounts[pod.ServiceAccountName] {
		return fmt.Errorf("Deployment %s runs as ServiceAccount %q, which the recipe does not create", owner, pod.ServiceAccountName)
	}
	for _, v := range pod.Volumes {
		if v.HostPath != nil {
			return fmt.Errorf("Deployment %s mounts hostPath volume %q", owner, v.Name)
		}
	}
	for _, c := range append(slices.Clone(pod.InitContainers), pod.Containers...) {
		if c.SecurityContext != nil && ((c.SecurityContext.Privileged != nil && *c.SecurityContext.Privileged) ||
			(c.SecurityContext.AllowPrivilegeEscalation != nil && *c.SecurityContext.AllowPrivilegeEscalation)) {
			return fmt.Errorf("Deployment %s container %s is privileged", owner, c.Name)
		}
		if !strings.Contains(c.Image, "@sha256:") {
			return fmt.Errorf("Deployment %s container %s image %q is not pinned by digest", owner, c.Name, c.Image)
		}
		if c.Resources.Limits.Cpu().IsZero() || c.Resources.Limits.Memory().IsZero() {
			return fmt.Errorf("Deployment %s container %s sets no cpu and memory limits", owner, c.Name)
		}
	}
	return nil
}

func (a Applier) waitReady(ctx context.Context, backendID string, deployments []*appsv1.Deployment) error {
	timeout, interval := a.ReadyTimeout, a.PollInterval
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	for _, d := range deployments {
		var waited time.Duration
		for {
			current, err := a.Client.AppsV1().Deployments(d.Namespace).Get(ctx, d.Name, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("read telemetry backend %q deployment %s: %w", backendID, d.Name, err)
			}
			if err == nil && deploymentReady(current) {
				break
			}
			if waited >= timeout {
				return &Error{Reason: ReasonNotReady, Backend: backendID, Path: layout.TelemetryBackendsFile, Detail: a.notReadyDetail(ctx, d, current, timeout)}
			}
			if err := a.wait(ctx, interval); err != nil {
				return err
			}
			waited += interval
		}
	}
	return nil
}

func deploymentReady(d *appsv1.Deployment) bool {
	want := int32(1)
	if d.Spec.Replicas != nil {
		want = *d.Spec.Replicas
	}
	return d.Status.ObservedGeneration >= d.Generation && d.Status.UpdatedReplicas >= want && d.Status.AvailableReplicas >= want
}

func (a Applier) wait(ctx context.Context, d time.Duration) error {
	if a.Wait != nil {
		return a.Wait(ctx, d)
	}
	return waitio.Wait(ctx, d)
}

// notReadyDetail is what the designer needs to correct the choice: the
// deployment's conditions and why its pods wait.
func (a Applier) notReadyDetail(ctx context.Context, d *appsv1.Deployment, current *appsv1.Deployment, timeout time.Duration) string {
	parts := []string{fmt.Sprintf("deployment %s was not available after %s", d.Name, timeout)}
	if current != nil {
		for _, c := range current.Status.Conditions {
			if c.Status != corev1.ConditionTrue || c.Type == appsv1.DeploymentReplicaFailure {
				parts = append(parts, fmt.Sprintf("%s=%s %s", c.Type, c.Status, c.Message))
			}
		}
	}
	if d.Spec.Selector != nil {
		selector := metav1.FormatLabelSelector(d.Spec.Selector)
		if pods, err := a.Client.CoreV1().Pods(d.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector}); err == nil {
			for _, pod := range pods.Items {
				for _, status := range pod.Status.ContainerStatuses {
					if w := status.State.Waiting; w != nil {
						parts = append(parts, fmt.Sprintf("pod %s container %s waiting: %s %s", pod.Name, status.Name, w.Reason, w.Message))
					}
				}
			}
		}
	}
	text := strings.Join(parts, "; ")
	if len(text) > 400 {
		text = text[:400] + "..."
	}
	return text
}
