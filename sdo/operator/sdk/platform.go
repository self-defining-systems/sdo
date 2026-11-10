package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Platform observation parts, the keys of PlatformObservation.Errors.
const (
	PlatformNamespace         = "namespace"
	PlatformLimitRanges       = "limit_ranges"
	PlatformResourceQuotas    = "resource_quotas"
	PlatformRoles             = "roles"
	PlatformRoleBindings      = "role_bindings"
	PlatformAdmissionWebhooks = "admission_webhooks"
	PlatformSourceInventory   = "source_inventory"
)

// PlatformObservation freezes, once per evaluation, the platform policy that
// governs the application namespace and is usually not declared by the
// application's source: the Namespace's labels and annotations (Pod Security
// levels among them), its LimitRanges, ResourceQuotas (with usage), Roles and
// RoleBindings, every admission webhook entry that selects Pods in it, named
// in source or not and served in or outside the cluster, and the accepted
// source inventory. A part that could not be read is absent and named in
// Errors; its accessor returns that error, never an empty list.
//
// Baseline is the observation at the controller's last promoted healthy
// baseline (nil until the application has been healthy once); Changes lists
// what differs from it.
type PlatformObservation struct {
	ObservedAt     time.Time              `json:"observed_at"`
	Namespace      metav1.ObjectMeta      `json:"namespace"`
	LimitRangeList []corev1.LimitRange    `json:"limit_ranges,omitempty"`
	QuotaList      []corev1.ResourceQuota `json:"resource_quotas,omitempty"`
	RoleList       []rbacv1.Role          `json:"roles,omitempty"`
	BindingList    []rbacv1.RoleBinding   `json:"role_bindings,omitempty"`
	Webhooks       []AdmissionWebhook     `json:"admission_webhooks,omitempty"`
	Inventory      []ObjectRef            `json:"source_inventory,omitempty"`
	Errors         map[string]string      `json:"errors,omitempty"`
	Baseline       *PlatformObservation   `json:"baseline,omitempty"`
}

// PlatformSource supplies the frozen platform observation.
type PlatformSource interface {
	PlatformObservation() (PlatformObservation, bool)
}

// PlatformConsumer is implemented by detectors that read the platform
// observation; the controller collects it only for bundles that declare one.
type PlatformConsumer interface{ ConsumesPlatform() bool }

// ConsumesPlatform reports whether any detector reads the platform observation.
func ConsumesPlatform(detectors []Detector) bool {
	for _, detector := range detectors {
		if consumer, ok := detector.(PlatformConsumer); ok && consumer.ConsumesPlatform() {
			return true
		}
	}
	return false
}

// Platform returns the view's platform observation, or an error when the
// evaluation did not observe it (no detector declared PlatformConsumer).
func Platform(view DetectionContext) (PlatformObservation, error) {
	if source, ok := view.(PlatformSource); ok {
		if observed, ok := source.PlatformObservation(); ok {
			return observed, nil
		}
	}
	return PlatformObservation{}, errors.New("platform observation unavailable: the detector must implement sdk.PlatformConsumer")
}

func (p PlatformObservation) part(name string) error {
	if reason, failed := p.Errors[name]; failed {
		return fmt.Errorf("platform %s unavailable: %s", name, reason)
	}
	return nil
}

// NamespaceMeta returns the Namespace's name, labels and annotations.
func (p PlatformObservation) NamespaceMeta() (metav1.ObjectMeta, error) {
	return p.Namespace, p.part(PlatformNamespace)
}

// LimitRanges returns the namespace's LimitRanges, sorted by name.
func (p PlatformObservation) LimitRanges() ([]corev1.LimitRange, error) {
	return p.LimitRangeList, p.part(PlatformLimitRanges)
}

// ResourceQuotas returns the namespace's ResourceQuotas with status, sorted by name.
func (p PlatformObservation) ResourceQuotas() ([]corev1.ResourceQuota, error) {
	return p.QuotaList, p.part(PlatformResourceQuotas)
}

// Roles returns the namespace's Roles, sorted by name.
func (p PlatformObservation) Roles() ([]rbacv1.Role, error) {
	return p.RoleList, p.part(PlatformRoles)
}

// RoleBindings returns the namespace's RoleBindings, sorted by name.
func (p PlatformObservation) RoleBindings() ([]rbacv1.RoleBinding, error) {
	return p.BindingList, p.part(PlatformRoleBindings)
}

// AdmissionWebhooks returns every webhook entry whose rules and
// namespaceSelector admit Pod creation in the namespace, sorted by
// configuration kind, name and entry. ObjectSelector says which Pods it takes.
func (p PlatformObservation) AdmissionWebhooks() ([]AdmissionWebhook, error) {
	return p.Webhooks, p.part(PlatformAdmissionWebhooks)
}

// SourceInventory lists the objects the accepted source declares, Secrets
// excluded, sorted by kind, namespace and name.
func (p PlatformObservation) SourceInventory() ([]ObjectRef, error) {
	return p.Inventory, p.part(PlatformSourceInventory)
}

// MissingFromLive lists the source inventory objects in the view's namespace
// that are not live: a deleted Service, Role or ConfigMap. It judges the
// platform kinds it holds (LimitRange, ResourceQuota, Role, RoleBinding) and
// the view kinds named in kinds (for example "Service", "ConfigMap"), which
// the detector must watch so the view holds them; other kinds are not judged.
func (p PlatformObservation) MissingFromLive(view DetectionContext, kinds ...string) ([]ObjectRef, error) {
	inventory, err := p.SourceInventory()
	if err != nil {
		return nil, err
	}
	live := map[string]map[string]bool{}
	for _, kind := range kinds {
		live[kind] = map[string]bool{}
	}
	add := func(kind string, names ...string) {
		if live[kind] == nil {
			return
		}
		for _, name := range names {
			live[kind][name] = true
		}
	}
	for _, o := range view.ConfigMaps() {
		add("ConfigMap", o.Name)
	}
	for _, o := range view.Services() {
		add("Service", o.Name)
	}
	for _, o := range view.Deployments() {
		add("Deployment", o.Name)
	}
	for _, o := range view.StatefulSets() {
		add("StatefulSet", o.Name)
	}
	for _, o := range view.DaemonSets() {
		add("DaemonSet", o.Name)
	}
	for _, o := range view.Jobs() {
		add("Job", o.Name)
	}
	for _, o := range view.CronJobs() {
		add("CronJob", o.Name)
	}
	for _, o := range view.ServiceAccounts() {
		add("ServiceAccount", o.Name)
	}
	for _, o := range view.PersistentVolumeClaims() {
		add("PersistentVolumeClaim", o.Name)
	}
	for _, o := range view.HorizontalPodAutoscalers() {
		add("HorizontalPodAutoscaler", o.Name)
	}
	for _, o := range view.PodDisruptionBudgets() {
		add("PodDisruptionBudget", o.Name)
	}
	for _, o := range view.NetworkPolicies() {
		add("NetworkPolicy", o.Name)
	}
	for _, o := range view.Ingresses() {
		add("Ingress", o.Name)
	}
	if p.part(PlatformLimitRanges) == nil {
		live["LimitRange"] = map[string]bool{}
		for _, o := range p.LimitRangeList {
			add("LimitRange", o.Name)
		}
	}
	if p.part(PlatformResourceQuotas) == nil {
		live["ResourceQuota"] = map[string]bool{}
		for _, o := range p.QuotaList {
			add("ResourceQuota", o.Name)
		}
	}
	if p.part(PlatformRoles) == nil {
		live["Role"] = map[string]bool{}
		for _, o := range p.RoleList {
			add("Role", o.Name)
		}
	}
	if p.part(PlatformRoleBindings) == nil {
		live["RoleBinding"] = map[string]bool{}
		for _, o := range p.BindingList {
			add("RoleBinding", o.Name)
		}
	}
	var missing []ObjectRef
	for _, ref := range inventory {
		if ref.Namespace != view.Namespace() {
			continue
		}
		names, judged := live[ref.Kind]
		if !judged {
			continue
		}
		if !names[ref.Name] {
			missing = append(missing, ref)
		}
	}
	return missing, nil
}

// Platform change kinds.
const (
	PlatformAdded    = "added"
	PlatformRemoved  = "removed"
	PlatformModified = "modified"
)

// PlatformChange is one difference from the healthy baseline. Field names
// the label or webhook entry for a Namespace or webhook change; Before and
// After are JSON values of the compared part.
type PlatformChange struct {
	Ref    ObjectRef `json:"ref"`
	Change string    `json:"change"`
	Field  string    `json:"field,omitempty"`
	Before string    `json:"before,omitempty"`
	After  string    `json:"after,omitempty"`
}

// Changes lists what differs from the healthy baseline: added, removed and
// modified LimitRanges, ResourceQuotas (their hard limits), Roles (rules),
// RoleBindings (role and subjects), admission webhook entries, and Namespace
// labels and annotations. Parts unavailable now or at the baseline are not
// compared. It returns false when there is no baseline yet.
func (p PlatformObservation) Changes() ([]PlatformChange, bool) {
	if p.Baseline == nil {
		return nil, false
	}
	base := *p.Baseline
	var out []PlatformChange
	comparable := func(part string) bool { return p.part(part) == nil && base.part(part) == nil }
	ns := ObjectRef{APIVersion: "v1", Kind: "Namespace", Name: p.Namespace.Name}
	if comparable(PlatformNamespace) {
		out = append(out, diffStrings(ns, "labels", base.Namespace.Labels, p.Namespace.Labels)...)
		out = append(out, diffStrings(ns, "annotations", base.Namespace.Annotations, p.Namespace.Annotations)...)
	}
	if comparable(PlatformLimitRanges) {
		out = append(out, diffObjects(platformObjects(base.LimitRangeList, func(o corev1.LimitRange) (ObjectRef, any) {
			return ObjectRef{APIVersion: "v1", Kind: "LimitRange", Namespace: o.Namespace, Name: o.Name}, o.Spec
		}), platformObjects(p.LimitRangeList, func(o corev1.LimitRange) (ObjectRef, any) {
			return ObjectRef{APIVersion: "v1", Kind: "LimitRange", Namespace: o.Namespace, Name: o.Name}, o.Spec
		}))...)
	}
	if comparable(PlatformResourceQuotas) {
		quota := func(o corev1.ResourceQuota) (ObjectRef, any) {
			return ObjectRef{APIVersion: "v1", Kind: "ResourceQuota", Namespace: o.Namespace, Name: o.Name}, o.Spec
		}
		out = append(out, diffObjects(platformObjects(base.QuotaList, quota), platformObjects(p.QuotaList, quota))...)
	}
	if comparable(PlatformRoles) {
		role := func(o rbacv1.Role) (ObjectRef, any) {
			return ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role", Namespace: o.Namespace, Name: o.Name}, o.Rules
		}
		out = append(out, diffObjects(platformObjects(base.RoleList, role), platformObjects(p.RoleList, role))...)
	}
	if comparable(PlatformRoleBindings) {
		binding := func(o rbacv1.RoleBinding) (ObjectRef, any) {
			return ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding", Namespace: o.Namespace, Name: o.Name}, map[string]any{"roleRef": o.RoleRef, "subjects": o.Subjects}
		}
		out = append(out, diffObjects(platformObjects(base.BindingList, binding), platformObjects(p.BindingList, binding))...)
	}
	if comparable(PlatformAdmissionWebhooks) {
		webhook := func(o AdmissionWebhook) (ObjectRef, any) {
			ref := o.Configuration
			ref.Name += "#" + o.Webhook
			o.ReadyEndpoints = 0
			return ref, o
		}
		changes := diffObjects(platformObjects(base.Webhooks, webhook), platformObjects(p.Webhooks, webhook))
		for i := range changes {
			name, entry, _ := cutLast(changes[i].Ref.Name, "#")
			changes[i].Ref.Name, changes[i].Field = name, "webhooks["+entry+"]"
		}
		out = append(out, changes...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.Ref.Kind != b.Ref.Kind {
			return a.Ref.Kind < b.Ref.Kind
		}
		if a.Ref.Name != b.Ref.Name {
			return a.Ref.Name < b.Ref.Name
		}
		return a.Field < b.Field
	})
	return out, true
}

func cutLast(s, sep string) (string, string, bool) {
	for i := len(s) - len(sep); i >= 0; i-- {
		if s[i:i+len(sep)] == sep {
			return s[:i], s[i+len(sep):], true
		}
	}
	return s, "", false
}

func platformObjects[T any](items []T, project func(T) (ObjectRef, any)) map[ObjectRef]string {
	out := make(map[ObjectRef]string, len(items))
	for _, item := range items {
		ref, value := project(item)
		raw, _ := json.Marshal(value)
		out[ref] = string(raw)
	}
	return out
}

func diffObjects(before, after map[ObjectRef]string) []PlatformChange {
	var out []PlatformChange
	for ref, old := range before {
		current, ok := after[ref]
		switch {
		case !ok:
			out = append(out, PlatformChange{Ref: ref, Change: PlatformRemoved, Before: boundedPlatformValue(old)})
		case current != old:
			out = append(out, PlatformChange{Ref: ref, Change: PlatformModified, Before: boundedPlatformValue(old), After: boundedPlatformValue(current)})
		}
	}
	for ref, current := range after {
		if _, ok := before[ref]; !ok {
			out = append(out, PlatformChange{Ref: ref, Change: PlatformAdded, After: boundedPlatformValue(current)})
		}
	}
	return out
}

func diffStrings(ref ObjectRef, field string, before, after map[string]string) []PlatformChange {
	var out []PlatformChange
	quote := func(s string) string { raw, _ := json.Marshal(s); return string(raw) }
	for key, old := range before {
		current, ok := after[key]
		switch {
		case !ok:
			out = append(out, PlatformChange{Ref: ref, Change: PlatformRemoved, Field: field + "." + key, Before: quote(old)})
		case current != old:
			out = append(out, PlatformChange{Ref: ref, Change: PlatformModified, Field: field + "." + key, Before: quote(old), After: quote(current)})
		}
	}
	for key, current := range after {
		if _, ok := before[key]; !ok {
			out = append(out, PlatformChange{Ref: ref, Change: PlatformAdded, Field: field + "." + key, After: quote(current)})
		}
	}
	return out
}

func boundedPlatformValue(value string) string {
	if len(value) > 512 {
		return value[:512] + "…"
	}
	return value
}
