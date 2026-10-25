package runtime

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// SourceInventory is an immutable index of the accepted deployment. Evaluation
// uses informer objects only: no API requests, source rendering or model calls.
type SourceInventory struct {
	NamespaceOnboarding *contracts.NamespaceOnboarding
	acknowledged        map[sdk.ObjectRef]bool
	Namespace           string
	Revision            string
	Identity            string
	EvidenceGaps        []rendered.InventoryEvidenceGap
	declared            map[sdk.ObjectRef]*unstructured.Unstructured
	dependencies        map[sdk.ObjectRef]bool
}

func NewSourceInventory(namespace, revision string, objects []*unstructured.Unstructured) *SourceInventory {
	index := &SourceInventory{Namespace: namespace, Revision: revision, declared: map[sdk.ObjectRef]*unstructured.Unstructured{}}
	for _, object := range objects {
		if object == nil || strings.EqualFold(object.GetKind(), "Secret") {
			continue
		}
		object = object.DeepCopy()
		if inventoryClusterKind(object.GetKind()) {
			object.SetNamespace("")
		} else if object.GetNamespace() == "" {
			object.SetNamespace(namespace)
		}
		index.declared[inventoryRef(object)] = object
	}
	index.indexDependencies()
	identities := make([]string, 0, len(index.declared))
	for ref, object := range index.declared {
		encoded, _ := json.Marshal(comparableFields(object.Object))
		identities = append(identities, fmt.Sprintf("%s/%s/%s/%s:%s", ref.APIVersion, ref.Kind, ref.Namespace, ref.Name, encoded))
	}
	sort.Strings(identities)
	digest := sha256.Sum256([]byte(strings.Join(identities, "\x00")))
	index.Identity = fmt.Sprintf("%x", digest)
	return index
}

func inventoryRef(o *unstructured.Unstructured) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: o.GetAPIVersion(), Kind: o.GetKind(), Namespace: o.GetNamespace(), Name: o.GetName()}
}
func inventoryClusterKind(kind string) bool {
	return kind == "ClusterRole" || kind == "ClusterRoleBinding"
}

func sourceInventoryKind(o *unstructured.Unstructured) bool {
	if o == nil {
		return false
	}
	switch o.GetAPIVersion() + "/" + o.GetKind() {
	case "v1/ConfigMap", "v1/Service", "v1/ServiceAccount", "v1/Pod", "v1/PersistentVolumeClaim",
		"apps/v1/Deployment", "apps/v1/StatefulSet", "apps/v1/DaemonSet", "apps/v1/ReplicaSet",
		"batch/v1/Job", "batch/v1/CronJob", "networking.k8s.io/v1/NetworkPolicy",
		"rbac.authorization.k8s.io/v1/Role", "rbac.authorization.k8s.io/v1/RoleBinding",
		"rbac.authorization.k8s.io/v1/ClusterRole", "rbac.authorization.k8s.io/v1/ClusterRoleBinding":
		return true
	}
	return false
}

// Findings accounts for top-level objects absent from source and pending
// deletion of declared objects. Generated children inherit their declared root;
// controller status and ephemeral endpoint/event churn never enter the check.
func (s *SourceInventory) Findings(objects []*unstructured.Unstructured, now time.Time) []sdk.Finding {
	observed := make(map[sdk.ObjectRef]*unstructured.Unstructured, len(objects))
	for _, o := range objects {
		if o != nil {
			observed[inventoryRef(o)] = o
		}
	}
	relevant := inventoryRBACScope(s.Namespace, objects)
	findings := []sdk.Finding{}
	var generated func(*unstructured.Unstructured, map[sdk.ObjectRef]bool) bool
	generated = func(o *unstructured.Unstructured, visited map[sdk.ObjectRef]bool) bool {
		if o == nil {
			return false
		}
		ref := inventoryRef(o)
		if visited[ref] {
			return false
		}
		visited[ref] = true
		for _, owner := range o.GetOwnerReferences() {
			parent := sdk.ObjectRef{APIVersion: owner.APIVersion, Kind: owner.Kind, Namespace: o.GetNamespace(), Name: owner.Name}
			if declared := s.declared[parent]; declared != nil || s.dependencies[parent] || s.acknowledged[parent] {
				return true
			}
			if ancestor := observed[parent]; ancestor != nil && (inventoryOwned(ancestor) || generated(ancestor, visited)) {
				return true
			}
		}
		return false
	}
	for _, o := range objects {
		if !sourceInventoryKind(o) || inventoryOwned(o) {
			continue
		}
		ref := inventoryRef(o)
		if ref.Namespace != s.Namespace && !relevant[ref] {
			continue
		}
		if o.GetKind() == "ConfigMap" && o.GetName() == "kube-root-ca.crt" || o.GetKind() == "ServiceAccount" && o.GetName() == "default" {
			continue
		}
		declared := s.declared[ref]
		// Inherited identity cannot override explicit accepted-source obligations.
		if declared == nil && !s.dependencies[ref] && generated(o, map[sdk.ObjectRef]bool{}) {
			continue
		}
		deleting := o.GetDeletionTimestamp() != nil && !o.GetDeletionTimestamp().IsZero()
		rbacChanged := declared != nil && strings.HasPrefix(o.GetAPIVersion(), "rbac.authorization.k8s.io/") && !reflect.DeepEqual(inventoryRBACProjection(declared), inventoryRBACProjection(o))
		if declared == nil && !s.dependencies[ref] && s.acknowledged[ref] {
			continue
		}
		if (declared != nil || s.dependencies[ref]) && !deleting && !rbacChanged {
			continue
		}
		fields := inventoryFields(o)
		source := "absent from accepted source"
		if declared != nil || s.dependencies[ref] {
			source = "deletion pending relative to accepted source"
		}
		if rbacChanged {
			source = "RBAC differs from accepted source"
		}
		event := sdk.DriftEvent{Ref: ref, Source: source, Manager: "unknown", ObservedAt: now.UTC(), Classification: sdk.DriftUnexplained, Fields: fields}
		encoded, _ := json.Marshal(event)
		identity, _ := json.Marshal([]any{event.Ref, event.Source, event.Fields})
		sum := sha256.Sum256(identity)
		finding := sdk.Finding{DetectorID: "drift.unexplained", RuleID: "unexplained", Status: sdk.FindingActive, Severity: sdk.SeverityLatent,
			Summary:  fmt.Sprintf("unexplained %s/%s: %s", ref.Kind, ref.Name, source),
			Evidence: fmt.Sprintf("Accepted commit %s; %s", s.Revision, encoded), PrimaryResource: ref,
			ParameterBindings: map[string]sdk.ObjectRef{"resource": ref}, Metadata: map[string]any{"drift_event": event, "source_inventory": true},
		}
		finding.Fingerprint = fmt.Sprintf("%s/%x", FindingFingerprint(finding), sum)
		findings = append(findings, finding)
	}
	sortFindings(findings)
	return findings
}
func inventoryOwned(o *unstructured.Unstructured) bool {
	if (o.GetKind() == "Pod" || o.GetKind() == "Job") && o.GetLabels()["sdo.dev/responder-helper"] == "true" {
		return true
	}
	return sdoOwned(metav1.ObjectMeta{Name: o.GetName(), Labels: o.GetLabels()})
}
func inventoryFields(o *unstructured.Unstructured) []sdk.DriftField {
	projection := comparableFields(o.Object)
	// A reason accepts this exact incarnation and configuration. Re-creation or
	// spec/deletion changes invalidate it; status and resourceVersion do not.
	projection["uid"] = string(o.GetUID())
	projection["owner_references"] = o.GetOwnerReferences()
	encoded, _ := json.Marshal(projection)
	sum := sha256.Sum256(encoded)
	fields := []sdk.DriftField{{Path: "object", New: fmt.Sprintf("sha256:%x", sum)}}
	if deleted := o.GetDeletionTimestamp(); deleted != nil && !deleted.IsZero() {
		fields = append(fields, sdk.DriftField{Path: "metadata.deletionTimestamp", New: deleted.UTC().Format(time.RFC3339Nano)})
	}
	if finalizers := o.GetFinalizers(); len(finalizers) > 0 {
		sort.Strings(finalizers)
		encoded, _ := json.Marshal(finalizers)
		fields = append(fields, sdk.DriftField{Path: "metadata.finalizers", New: string(encoded)})
	}
	return fields
}
func inventoryRBACScope(namespace string, objects []*unstructured.Unstructured) map[sdk.ObjectRef]bool {
	relevant := map[sdk.ObjectRef]bool{}
	for _, o := range objects {
		if o == nil || (o.GetKind() != "RoleBinding" && o.GetKind() != "ClusterRoleBinding") {
			continue
		}
		subjects, _, _ := unstructured.NestedSlice(o.Object, "subjects")
		bound := false
		for _, raw := range subjects {
			subject, _ := raw.(map[string]any)
			ns, _ := subject["namespace"].(string)
			if ns == "" && o.GetKind() == "RoleBinding" {
				ns = o.GetNamespace()
			}
			if subject["kind"] == "ServiceAccount" && ns == namespace {
				bound = true
			}
		}
		if !bound {
			continue
		}
		relevant[inventoryRef(o)] = true
		role, _, _ := unstructured.NestedMap(o.Object, "roleRef")
		if role["kind"] == "ClusterRole" {
			name, _ := role["name"].(string)
			relevant[sdk.ObjectRef{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole", Name: name}] = true
		}
	}
	return relevant
}

// RBAC list order and a defaulted ServiceAccount namespace do not change
// authorization. Compare semantic sets; ignore API metadata and status.
func inventoryRBACProjection(o *unstructured.Unstructured) map[string]any {
	projection := map[string]any{}
	for _, field := range []string{"rules", "roleRef", "subjects", "aggregationRule"} {
		value, ok := o.Object[field]
		if !ok {
			continue
		}
		encoded, _ := json.Marshal(value)
		var copy any
		_ = json.Unmarshal(encoded, &copy)
		if field == "roleRef" {
			if reference, ok := copy.(map[string]any); ok && (reference["apiGroup"] == nil || reference["apiGroup"] == "") {
				reference["apiGroup"] = "rbac.authorization.k8s.io"
			}
		}
		if field == "subjects" {
			subjects, _ := copy.([]any)
			for _, raw := range subjects {
				subject, _ := raw.(map[string]any)
				if subject["kind"] == "ServiceAccount" && (subject["namespace"] == nil || subject["namespace"] == "") {
					subject["namespace"] = o.GetNamespace()
				}
				if subject["kind"] == "ServiceAccount" && (subject["apiGroup"] == nil || subject["apiGroup"] == "") {
					delete(subject, "apiGroup")
				}
				if (subject["kind"] == "User" || subject["kind"] == "Group") && (subject["apiGroup"] == nil || subject["apiGroup"] == "") {
					subject["apiGroup"] = "rbac.authorization.k8s.io"
				}
			}
		}
		normalized := inventorySortedSets(copy)
		if normalized == nil {
			continue
		}
		if list, ok := normalized.([]any); ok && len(list) == 0 {
			continue
		}
		projection[field] = normalized
	}
	return projection
}
func inventorySortedSets(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			typed[key] = inventorySortedSets(child)
		}
		return typed
	case []any:
		for i, child := range typed {
			typed[i] = inventorySortedSets(child)
		}
		sort.Slice(typed, func(i, j int) bool {
			a, _ := json.Marshal(typed[i])
			b, _ := json.Marshal(typed[j])
			return string(a) < string(b)
		})
		return typed
	default:
		return value
	}
}
