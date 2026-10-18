package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// StateChange kinds.
const (
	StateChangeAdded    = "added"
	StateChangeRemoved  = "removed"
	StateChangeModified = "modified"
)

const (
	// DefaultStateSettle is how long a new state must stay healthy before it
	// replaces the baseline, so a fault that is still below its firing
	// threshold is never absorbed into the baseline it should be compared to.
	DefaultStateSettle = 2 * time.Minute
	maxStateChanges    = 40
	maxStateFields     = 12
	maxRenderedValue   = 160
)

// StateFieldChange is one changed aspect of an object. Before and After are
// rendered values for small fields and sha256 digests for data.
type StateFieldChange struct {
	Field  string `json:"field"`
	Before string `json:"before,omitempty"`
	After  string `json:"after,omitempty"`
}

// StateChange is one application object that differs from the healthy
// baseline.
type StateChange struct {
	Kind   string             `json:"kind"`
	Name   string             `json:"name"`
	Change string             `json:"change"`
	Fields []StateFieldChange `json:"fields,omitempty"`
}

// StateChanges is the application namespace's configuration diff against its
// last healthy baseline, attached to an incident request at dispatch.
type StateChanges struct {
	BaselineAt time.Time     `json:"baseline_at"`
	ObservedAt time.Time     `json:"observed_at"`
	Changes    []StateChange `json:"changes"`
	// Omitted counts changes beyond the cap.
	Omitted int `json:"omitted,omitempty"`
	// UnobservedKinds could not be read, so their changes are unknown.
	UnobservedKinds []string `json:"unobserved_kinds,omitempty"`
}

// StateBaseline records healthy configuration and reports what changed since.
type StateBaseline interface {
	// Observe tells the baseline whether the controller currently sees the
	// application healthy: no incident, and no active or pending finding.
	Observe(now time.Time, healthy bool)
	// Changes diffs the current state against the baseline; nil without one.
	Changes(now time.Time) *StateChanges
}

// StateTrackerConfig configures a StateTracker.
type StateTrackerConfig struct {
	Client    kubernetes.Interface
	Namespace string
	// Settle defaults to DefaultStateSettle.
	Settle time.Duration
}

type objectState map[string]string

type stateSnapshot struct {
	at      time.Time
	objects map[string]objectState
}

type trackedKind struct {
	kind     string
	informer func(informers.SharedInformerFactory) cache.SharedIndexInformer
	probe    func(context.Context, kubernetes.Interface, string) error
	state    func(any) (string, objectState, bool)
}

// StateTracker keeps namespace-scoped informers over the configuration kinds
// that commonly carry faults and diffs them in memory, so a diff costs
// milliseconds and never calls the API server at dispatch.
type StateTracker struct {
	config StateTrackerConfig

	mu         sync.Mutex
	cancel     context.CancelFunc
	informers  map[string]cache.SharedIndexInformer
	unobserved []string
	baseline   *stateSnapshot
	candidate  *stateSnapshot
}

// NewStateTracker creates an unstarted tracker.
func NewStateTracker(config StateTrackerConfig) *StateTracker {
	if config.Settle <= 0 {
		config.Settle = DefaultStateSettle
	}
	return &StateTracker{config: config}
}

// Start probes which kinds may be read, starts their informers and waits for
// the initial sync. A kind that may not be read is reported, not fatal. ctx
// bounds only the probes and the initial sync: the informers keep observing
// after it ends, until Stop, so a caller may pass a startup deadline.
func (t *StateTracker) Start(ctx context.Context) error {
	t.Stop()
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	factory := informers.NewSharedInformerFactoryWithOptions(t.config.Client, 0, informers.WithNamespace(t.config.Namespace))
	started := map[string]cache.SharedIndexInformer{}
	unobserved := []string{}
	for _, kind := range trackedKinds {
		if err := kind.probe(ctx, t.config.Client, t.config.Namespace); err != nil {
			unobserved = append(unobserved, kind.kind)
			continue
		}
		started[kind.kind] = kind.informer(factory)
	}
	factory.Start(runCtx.Done())
	syncs := make([]cache.InformerSynced, 0, len(started))
	for _, informer := range started {
		syncs = append(syncs, informer.HasSynced)
	}
	if !cache.WaitForCacheSync(ctx.Done(), syncs...) {
		cancel()
		return fmt.Errorf("sync state tracker informers: %w", ctx.Err())
	}
	t.mu.Lock()
	t.cancel, t.informers, t.unobserved = cancel, started, unobserved
	t.mu.Unlock()
	return nil
}

// UnobservedKinds are the kinds the last Start could not read.
func (t *StateTracker) UnobservedKinds() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.unobserved...)
}

// Stop ends observation; the baseline is kept until Reset.
func (t *StateTracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cancel != nil {
		t.cancel()
	}
	t.cancel, t.informers = nil, nil
}

// Reset forgets the baseline, for example when a maintenance window may have
// redeployed the application.
func (t *StateTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.baseline, t.candidate = nil, nil
}

// Observe implements StateBaseline. The first healthy observation becomes the
// baseline. Later, a candidate taken at the start of a healthy stretch
// replaces the baseline once it has stayed healthy for the settle period; any
// unhealthy observation discards the candidate.
func (t *StateTracker) Observe(now time.Time, healthy bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.informers == nil {
		return
	}
	if !healthy {
		t.candidate = nil
		return
	}
	switch {
	case t.baseline == nil:
		t.baseline = t.snapshotLocked(now)
	case t.candidate == nil:
		t.candidate = t.snapshotLocked(now)
	case now.Sub(t.candidate.at) >= t.config.Settle:
		t.baseline = t.candidate
		t.candidate = t.snapshotLocked(now)
	}
}

// Changes implements StateBaseline.
func (t *StateTracker) Changes(now time.Time) *StateChanges {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.baseline == nil || t.informers == nil {
		return nil
	}
	current := t.snapshotLocked(now)
	changes := diffStates(t.baseline, current)
	changes.UnobservedKinds = append([]string(nil), t.unobserved...)
	return changes
}

func (t *StateTracker) snapshotLocked(now time.Time) *stateSnapshot {
	snapshot := &stateSnapshot{at: now.UTC(), objects: map[string]objectState{}}
	for _, kind := range trackedKinds {
		informer, ok := t.informers[kind.kind]
		if !ok {
			continue
		}
		for _, object := range informer.GetStore().List() {
			name, state, include := kind.state(object)
			if include {
				snapshot.objects[kind.kind+"/"+name] = state
			}
		}
	}
	return snapshot
}

func diffStates(baseline *stateSnapshot, current *stateSnapshot) *StateChanges {
	keys := map[string]bool{}
	for key := range baseline.objects {
		keys[key] = true
	}
	for key := range current.objects {
		keys[key] = true
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(left int, right int) bool {
		leftKind, leftName, _ := strings.Cut(ordered[left], "/")
		rightKind, rightName, _ := strings.Cut(ordered[right], "/")
		if kindRank[leftKind] != kindRank[rightKind] {
			return kindRank[leftKind] < kindRank[rightKind]
		}
		return leftName < rightName
	})
	result := &StateChanges{BaselineAt: baseline.at, ObservedAt: current.at, Changes: []StateChange{}}
	for _, key := range ordered {
		before, hadBefore := baseline.objects[key]
		after, hasAfter := current.objects[key]
		kind, name, _ := strings.Cut(key, "/")
		change := StateChange{Kind: kind, Name: name}
		switch {
		case !hadBefore:
			change.Change = StateChangeAdded
			change.Fields = fieldChanges(nil, after)
		case !hasAfter:
			change.Change = StateChangeRemoved
		default:
			change.Fields = fieldChanges(before, after)
			if len(change.Fields) == 0 {
				continue
			}
			change.Change = StateChangeModified
		}
		if len(result.Changes) >= maxStateChanges {
			result.Omitted++
			continue
		}
		result.Changes = append(result.Changes, change)
	}
	return result
}

func fieldChanges(before objectState, after objectState) []StateFieldChange {
	names := map[string]bool{}
	for name := range before {
		names[name] = true
	}
	for name := range after {
		names[name] = true
	}
	ordered := make([]string, 0, len(names))
	for name := range names {
		if before[name] != after[name] {
			ordered = append(ordered, name)
		}
	}
	sort.Strings(ordered)
	changes := make([]StateFieldChange, 0, len(ordered))
	for _, name := range ordered {
		if len(changes) == maxStateFields {
			changes = append(changes, StateFieldChange{Field: fmt.Sprintf("(%d more fields)", len(ordered)-maxStateFields)})
			break
		}
		changes = append(changes, StateFieldChange{Field: name, Before: before[name], After: after[name]})
	}
	return changes
}

var kindRank = map[string]int{}

func init() {
	for index, kind := range trackedKinds {
		kindRank[kind.kind] = index
	}
}

// trackedKinds are observed in this order, which is also the diff's order:
// traffic routing first, then workloads, network policy, configuration, and
// access control.
var trackedKinds = []trackedKind{
	{
		kind: "Service",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().Services().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.CoreV1().Services(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: serviceState,
	},
	{
		kind: "Deployment",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Apps().V1().Deployments().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: deploymentState,
	},
	{
		kind: "StatefulSet",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Apps().V1().StatefulSets().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.AppsV1().StatefulSets(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: statefulSetState,
	},
	{
		kind: "DaemonSet",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Apps().V1().DaemonSets().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.AppsV1().DaemonSets(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: daemonSetState,
	},
	{
		kind: "NetworkPolicy",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Networking().V1().NetworkPolicies().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.NetworkingV1().NetworkPolicies(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: networkPolicyState,
	},
	{
		kind: "ConfigMap",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().ConfigMaps().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.CoreV1().ConfigMaps(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: configMapState,
	},
	{
		kind: "Secret",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Core().V1().Secrets().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.CoreV1().Secrets(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: secretState,
	},
	{
		kind: "Role",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Rbac().V1().Roles().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.RbacV1().Roles(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: roleState,
	},
	{
		kind: "RoleBinding",
		informer: func(f informers.SharedInformerFactory) cache.SharedIndexInformer {
			return f.Rbac().V1().RoleBindings().Informer()
		},
		probe: func(ctx context.Context, c kubernetes.Interface, ns string) error {
			_, err := c.RbacV1().RoleBindings(ns).List(ctx, metav1.ListOptions{Limit: 1})
			return err
		},
		state: roleBindingState,
	},
}

// sdoOwned reports SDO's own objects, whose churn is not an application change.
func sdoOwned(meta metav1.ObjectMeta) bool {
	return strings.HasPrefix(meta.Name, "sdo-") || meta.Labels["app.kubernetes.io/managed-by"] == "sdo"
}

func serviceState(object any) (string, objectState, bool) {
	service, ok := object.(*corev1.Service)
	if !ok || sdoOwned(service.ObjectMeta) {
		return "", nil, false
	}
	ports := make([]string, 0, len(service.Spec.Ports))
	for _, port := range service.Spec.Ports {
		ports = append(ports, fmt.Sprintf("%s:%d->%s/%s", port.Name, port.Port, port.TargetPort.String(), port.Protocol))
	}
	other := service.Spec.DeepCopy()
	other.Selector, other.Ports, other.Type = nil, nil, ""
	other.ClusterIP, other.ClusterIPs, other.IPFamilies, other.IPFamilyPolicy = "", nil, nil, nil
	return service.Name, objectState{
		"type":     string(service.Spec.Type),
		"selector": renderLabels(service.Spec.Selector),
		"ports":    bounded(strings.Join(ports, ",")),
		"other":    digestJSON(other),
	}, true
}

func deploymentState(object any) (string, objectState, bool) {
	deployment, ok := object.(*appsv1.Deployment)
	if !ok || sdoOwned(deployment.ObjectMeta) {
		return "", nil, false
	}
	state := podTemplateState(deployment.Spec.Template)
	state["replicas"] = renderReplicas(deployment.Spec.Replicas)
	state["selector"] = renderSelector(deployment.Spec.Selector)
	state["strategy"] = digestJSON(deployment.Spec.Strategy)
	state["paused"] = strconv.FormatBool(deployment.Spec.Paused)
	return deployment.Name, state, true
}

func statefulSetState(object any) (string, objectState, bool) {
	statefulSet, ok := object.(*appsv1.StatefulSet)
	if !ok || sdoOwned(statefulSet.ObjectMeta) {
		return "", nil, false
	}
	state := podTemplateState(statefulSet.Spec.Template)
	state["replicas"] = renderReplicas(statefulSet.Spec.Replicas)
	state["selector"] = renderSelector(statefulSet.Spec.Selector)
	state["serviceName"] = statefulSet.Spec.ServiceName
	state["updateStrategy"] = digestJSON(statefulSet.Spec.UpdateStrategy)
	return statefulSet.Name, state, true
}

func daemonSetState(object any) (string, objectState, bool) {
	daemonSet, ok := object.(*appsv1.DaemonSet)
	if !ok || sdoOwned(daemonSet.ObjectMeta) {
		return "", nil, false
	}
	state := podTemplateState(daemonSet.Spec.Template)
	state["selector"] = renderSelector(daemonSet.Spec.Selector)
	state["updateStrategy"] = digestJSON(daemonSet.Spec.UpdateStrategy)
	return daemonSet.Name, state, true
}

func networkPolicyState(object any) (string, objectState, bool) {
	policy, ok := object.(*networkingv1.NetworkPolicy)
	if !ok || sdoOwned(policy.ObjectMeta) {
		return "", nil, false
	}
	types := make([]string, 0, len(policy.Spec.PolicyTypes))
	for _, policyType := range policy.Spec.PolicyTypes {
		types = append(types, string(policyType))
	}
	return policy.Name, objectState{
		"podSelector": renderSelector(&policy.Spec.PodSelector),
		"policyTypes": strings.Join(types, ","),
		"ingress":     renderJSON(policy.Spec.Ingress),
		"egress":      renderJSON(policy.Spec.Egress),
	}, true
}

func configMapState(object any) (string, objectState, bool) {
	configMap, ok := object.(*corev1.ConfigMap)
	if !ok || sdoOwned(configMap.ObjectMeta) || configMap.Name == "kube-root-ca.crt" {
		return "", nil, false
	}
	state := objectState{}
	for key, value := range configMap.Data {
		state["data["+key+"]"] = digest([]byte(value))
	}
	for key, value := range configMap.BinaryData {
		state["binaryData["+key+"]"] = digest(value)
	}
	return configMap.Name, state, true
}

func secretState(object any) (string, objectState, bool) {
	secret, ok := object.(*corev1.Secret)
	if !ok || sdoOwned(secret.ObjectMeta) || secret.Type == corev1.SecretTypeServiceAccountToken ||
		secret.Type == "helm.sh/release.v1" {
		return "", nil, false
	}
	state := objectState{"type": string(secret.Type)}
	for key, value := range secret.Data {
		state["data["+key+"]"] = digest(value)
	}
	return secret.Name, state, true
}

func roleState(object any) (string, objectState, bool) {
	role, ok := object.(*rbacv1.Role)
	if !ok || sdoOwned(role.ObjectMeta) {
		return "", nil, false
	}
	rules := make([]string, 0, len(role.Rules))
	for _, rule := range role.Rules {
		rules = append(rules, fmt.Sprintf("%s %s", strings.Join(rule.Verbs, ","), strings.Join(append(rule.Resources, rule.ResourceNames...), ",")))
	}
	return role.Name, objectState{"rules": bounded(strings.Join(rules, "; ")), "rulesDigest": digestJSON(role.Rules)}, true
}

func roleBindingState(object any) (string, objectState, bool) {
	binding, ok := object.(*rbacv1.RoleBinding)
	if !ok || sdoOwned(binding.ObjectMeta) {
		return "", nil, false
	}
	subjects := make([]string, 0, len(binding.Subjects))
	for _, subject := range binding.Subjects {
		subjects = append(subjects, strings.Trim(subject.Kind+"/"+subject.Namespace+"/"+subject.Name, "/"))
	}
	return binding.Name, objectState{
		"roleRef":  binding.RoleRef.Kind + "/" + binding.RoleRef.Name,
		"subjects": bounded(strings.Join(subjects, ",")),
	}, true
}

// secretLookingName matches environment variables whose values are shown
// only as digests.
var secretLookingName = regexp.MustCompile(`(?i)pass|secret|token|key|credential|auth`)

func podTemplateState(template corev1.PodTemplateSpec) objectState {
	podLabels := map[string]string{}
	for key, value := range template.Labels {
		if key != "pod-template-hash" {
			podLabels[key] = value
		}
	}
	spec := template.Spec
	volumes := make([]string, 0, len(spec.Volumes))
	for _, volume := range spec.Volumes {
		source := "other"
		switch {
		case volume.ConfigMap != nil:
			source = "configMap:" + volume.ConfigMap.Name
		case volume.Secret != nil:
			source = "secret:" + volume.Secret.SecretName
		case volume.PersistentVolumeClaim != nil:
			source = "pvc:" + volume.PersistentVolumeClaim.ClaimName
		case volume.EmptyDir != nil:
			source = "emptyDir"
		case volume.Projected != nil:
			source = "projected"
		}
		volumes = append(volumes, volume.Name+"="+source)
	}
	state := objectState{
		"template.labels":             renderLabels(podLabels),
		"template.annotations":        bounded(renderLabels(template.Annotations)),
		"template.serviceAccountName": spec.ServiceAccountName,
		"template.nodeSelector":       renderLabels(spec.NodeSelector),
		"template.tolerations":        renderJSON(spec.Tolerations),
		"template.affinity":           renderJSON(spec.Affinity),
		"template.volumes":            bounded(strings.Join(volumes, ",")),
	}
	for prefix, containers := range map[string][]corev1.Container{"container": spec.Containers, "initContainer": spec.InitContainers} {
		for _, container := range containers {
			for field, value := range containerState(container) {
				state[prefix+"["+container.Name+"]."+field] = value
			}
		}
	}
	other := spec.DeepCopy()
	other.Containers, other.InitContainers, other.Volumes = nil, nil, nil
	other.ServiceAccountName, other.DeprecatedServiceAccount, other.NodeSelector = "", "", nil
	other.Tolerations, other.Affinity = nil, nil
	state["template.other"] = digestJSON(other)
	return state
}

func containerState(container corev1.Container) objectState {
	env := make([]string, 0, len(container.Env))
	for _, variable := range container.Env {
		switch {
		case variable.ValueFrom != nil && variable.ValueFrom.ConfigMapKeyRef != nil:
			ref := variable.ValueFrom.ConfigMapKeyRef
			env = append(env, variable.Name+"<-configMap:"+ref.Name+"."+ref.Key)
		case variable.ValueFrom != nil && variable.ValueFrom.SecretKeyRef != nil:
			ref := variable.ValueFrom.SecretKeyRef
			env = append(env, variable.Name+"<-secret:"+ref.Name+"."+ref.Key)
		case variable.ValueFrom != nil:
			env = append(env, variable.Name+"<-"+digestJSON(variable.ValueFrom))
		case secretLookingName.MatchString(variable.Name) || len(variable.Value) > 64:
			env = append(env, variable.Name+"="+digest([]byte(variable.Value)))
		default:
			env = append(env, variable.Name+"="+variable.Value)
		}
	}
	envFrom := make([]string, 0, len(container.EnvFrom))
	for _, source := range container.EnvFrom {
		switch {
		case source.ConfigMapRef != nil:
			envFrom = append(envFrom, source.Prefix+"configMap:"+source.ConfigMapRef.Name)
		case source.SecretRef != nil:
			envFrom = append(envFrom, source.Prefix+"secret:"+source.SecretRef.Name)
		}
	}
	ports := make([]string, 0, len(container.Ports))
	for _, port := range container.Ports {
		ports = append(ports, fmt.Sprintf("%s:%d/%s", port.Name, port.ContainerPort, port.Protocol))
	}
	mounts := make([]string, 0, len(container.VolumeMounts))
	for _, mount := range container.VolumeMounts {
		mounts = append(mounts, mount.Name+"@"+mount.MountPath)
	}
	other := container.DeepCopy()
	other.Image, other.Command, other.Args, other.Env, other.EnvFrom = "", nil, nil, nil, nil
	other.Ports, other.Resources, other.VolumeMounts = nil, corev1.ResourceRequirements{}, nil
	other.LivenessProbe, other.ReadinessProbe, other.StartupProbe = nil, nil, nil
	return objectState{
		"image":          container.Image,
		"command":        bounded(strings.Join(container.Command, " ")),
		"args":           bounded(strings.Join(container.Args, " ")),
		"env":            bounded(strings.Join(env, ",")),
		"envFrom":        bounded(strings.Join(envFrom, ",")),
		"ports":          bounded(strings.Join(ports, ",")),
		"resources":      renderJSON(container.Resources),
		"volumeMounts":   bounded(strings.Join(mounts, ",")),
		"livenessProbe":  renderJSON(container.LivenessProbe),
		"readinessProbe": renderJSON(container.ReadinessProbe),
		"startupProbe":   renderJSON(container.StartupProbe),
		"other":          digestJSON(other),
	}
}

func renderLabels(values map[string]string) string {
	if len(values) == 0 {
		return ""
	}
	return labels.Set(values).String()
}

func renderSelector(selector *metav1.LabelSelector) string {
	if selector == nil {
		return ""
	}
	rendered, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return digestJSON(selector)
	}
	return bounded(rendered.String())
}

func renderReplicas(replicas *int32) string {
	if replicas == nil {
		return "1"
	}
	return strconv.Itoa(int(*replicas))
}

// renderJSON shows a small value as compact JSON and a large one as a digest.
func renderJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil || string(encoded) == "null" || string(encoded) == "{}" || string(encoded) == "[]" {
		if err == nil {
			return ""
		}
		return "unrenderable"
	}
	if len(encoded) > maxRenderedValue {
		return digest(encoded)
	}
	return string(encoded)
}

func digestJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "unrenderable"
	}
	return digest(encoded)
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(sum[:])[:12]
}

func bounded(value string) string {
	if len(value) <= maxRenderedValue {
		return value
	}
	return value[:maxRenderedValue-len("...")] + "..." + " (" + digest([]byte(value)) + ")"
}
