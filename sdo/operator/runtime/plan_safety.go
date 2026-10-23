package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
)

// PlanObjectSafety is controller-owned authorization, never program input.
// Digests authorize exact nonsecret content and effects, including consumer and
// routing closure. Accepted Git origin alone is not such authorization. The
// owner must establish provenance before allowing any opaque object read.
// UID binds live reads; source objects have no UID. Unknown versions fail closed.
type PlanObjectSafety struct {
	UID           string
	CreateAllowed bool
	DeleteAllowed bool
	Digests       map[string]bool
}

// PlanSafetyPolicy bounds the new vocabulary to independently authorized objects.
// It is process-local, expires, and is bound to the accepted source revision.
// There is deliberately no permissive default or agent-supplied attestation.
type PlanSafetyPolicy struct {
	// SourceImages and IncidentID are controller-derived source-only authority.
	SourceImages map[sdk.ObjectRef]*unstructured.Unstructured
	IncidentID   string
	SourceCommit string
	ExpiresAt    time.Time
	Objects      map[sdk.ObjectRef]PlanObjectSafety
	// ServiceAccounts pins independently authorized powerless account UIDs.
	ServiceAccounts map[sdk.ObjectRef]string
}

func (p *PlanSafetyPolicy) authorizeRead(ref sdk.ObjectRef, commit string, now time.Time) error {
	if p == nil || commit == "" || p.SourceCommit != commit || !now.Before(p.ExpiresAt) {
		return fmt.Errorf("plan safety: current controller-owned authorization is required")
	}
	proof, ok := p.Objects[ref]
	if !ok || (p.SourceImages == nil && len(proof.Digests) == 0) {
		return fmt.Errorf("plan safety: object lacks nonsecret content/effect provenance")
	}
	return nil
}

func (p *PlanSafetyPolicy) checkObject(object *unstructured.Unstructured, ref sdk.ObjectRef) error {
	if object == nil || !samePlanRef(object, ref) {
		return fmt.Errorf("plan safety: object identity is unavailable")
	}
	if err := planSecretPredicate(object); err != nil {
		return err
	}
	proof, ok := p.Objects[ref]
	if !ok || (p.SourceImages == nil && !proof.Digests[planSafetyDigest(object)]) {
		return fmt.Errorf("plan safety: object content/effect is not independently authorized")
	}
	if object.GetUID() != "" && (proof.UID == "" || proof.UID != string(object.GetUID())) {
		return fmt.Errorf("plan safety: object UID changed")
	}
	return nil
}

// planSafetyDigest is internal: callers must not hash unapproved opaque content
// to manufacture provenance. Resource/status versions do not change intent.
func planSafetyDigest(object *unstructured.Unstructured) string {
	copy := object.DeepCopy()
	delete(copy.Object, "status")
	metadata := map[string]any{"name": copy.GetName(), "namespace": copy.GetNamespace()}
	if labels := copy.GetLabels(); len(labels) > 0 {
		values := map[string]any{}
		for k, v := range labels {
			values[k] = v
		}
		metadata["labels"] = values
	}
	// Arbitrary annotations are opaque and require exact authorization too.
	if annotations := copy.GetAnnotations(); len(annotations) > 0 {
		values := map[string]any{}
		for k, v := range annotations {
			if k == "deployment.kubernetes.io/revision" && copy.GetKind() == "Deployment" {
				continue
			}
			values[k] = v
		}
		if len(values) > 0 {
			metadata["annotations"] = values
		}
	}
	copy.Object["metadata"] = metadata
	canonicalizePlanObject(copy)
	data, _ := json.Marshal(copy.Object)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

// planSecretPredicate rejects reference, projection, identity and host channels.
// It never dereferences Secret resources and never includes content in errors.
func planSecretPredicate(object *unstructured.Unstructured) error {
	switch object.GetKind() {
	case "Secret", "ServiceAccount", "Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding", "CertificateSigningRequest", "Node", "Namespace":
		return fmt.Errorf("plan safety: forbidden object kind")
	}
	var visit func(any) error
	visit = func(value any) error {
		switch v := value.(type) {
		case map[string]any:
			for k, child := range v {
				switch k {
				case "envFrom", "secretKeyRef", "imagePullSecrets", "secret", "secretRef", "secretName", "projected", "csi", "hostPath", "persistentVolumeClaim", "ipBlock", "namespaceSelector", "serviceAccount", "nfs", "flexVolume":
					return fmt.Errorf("plan safety: credential or unconfined content channel")
				case "hostNetwork", "hostPID", "hostIPC", "privileged", "allowPrivilegeEscalation":
					if child != false {
						return fmt.Errorf("plan safety: privileged or host access")
					}
				}
				if err := visit(child); err != nil {
					return err
				}
			}
		case []any:
			for _, item := range v {
				if err := visit(item); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(object.Object); err != nil {
		return err
	}
	if object.GetKind() == "NetworkPolicy" {
		for direction, key := range map[string]string{"ingress": "from", "egress": "to"} {
			rules, found, err := unstructured.NestedSlice(object.Object, "spec", direction)
			if err != nil {
				return fmt.Errorf("plan safety: unknown network policy rules")
			}
			if !found {
				continue
			}
			for _, item := range rules {
				rule, ok := item.(map[string]any)
				if !ok {
					return fmt.Errorf("plan safety: unknown network policy rule")
				}
				peers, ok := rule[key].([]any)
				if !ok || len(peers) == 0 {
					return fmt.Errorf("plan safety: allow-all network policy routing is forbidden")
				}
				for _, item := range peers {
					peer, ok := item.(map[string]any)
					if !ok || len(peer) != 1 {
						return fmt.Errorf("plan safety: unconfined network policy peer")
					}
					selector, ok := peer["podSelector"].(map[string]any)
					if !ok {
						return fmt.Errorf("plan safety: namespace-local pod selector required")
					}
					labels, ok := selector["matchLabels"].(map[string]any)
					if !ok || len(labels) == 0 || len(selector) != 1 {
						return fmt.Errorf("plan safety: bounded pod labels required")
					}
					for _, label := range labels {
						if _, ok := label.(string); !ok {
							return fmt.Errorf("plan safety: invalid pod label selector")
						}
					}
				}
			}
		}
	}

	if object.GetKind() == "Service" {
		typ, _, _ := unstructured.NestedString(object.Object, "spec", "type")
		external, _, _ := unstructured.NestedString(object.Object, "spec", "externalName")
		ips, _, _ := unstructured.NestedSlice(object.Object, "spec", "externalIPs")
		if external != "" || len(ips) > 0 || typ == "ExternalName" || typ == "LoadBalancer" || typ == "NodePort" {
			return fmt.Errorf("plan safety: external routing is forbidden")
		}
		selector, _, _ := unstructured.NestedMap(object.Object, "spec", "selector")
		if len(selector) == 0 {
			return fmt.Errorf("plan safety: selectorless routing is forbidden")
		}
	}
	if object.GetKind() == "MutatingWebhookConfiguration" || object.GetKind() == "ValidatingWebhookConfiguration" {
		webhooks, _, err := unstructured.NestedSlice(object.Object, "webhooks")
		if err != nil {
			return fmt.Errorf("plan safety: unknown webhook shape")
		}
		for _, item := range webhooks {
			webhook, ok := item.(map[string]any)
			if !ok {
				return fmt.Errorf("plan safety: unknown webhook shape")
			}
			if url, found, _ := unstructured.NestedString(webhook, "clientConfig", "url"); found && url != "" {
				return fmt.Errorf("plan safety: external webhook routing")
			}
			rules, _, _ := unstructured.NestedSlice(webhook, "rules")
			for _, item := range rules {
				rule, ok := item.(map[string]any)
				if !ok {
					return fmt.Errorf("plan safety: unknown webhook rule")
				}
				resources, _, _ := unstructured.NestedStringSlice(rule, "resources")
				for _, resource := range resources {
					if resource == "*" || resource == "*/*" || resource == "secrets" || resource == "secrets/*" {
						return fmt.Errorf("plan safety: credential-observing webhook")
					}
				}
			}
		}
	}

	if template, found, _ := unstructured.NestedMap(object.Object, "spec", "template", "spec"); found {
		if value, ok := template["automountServiceAccountToken"]; !ok || value != false {
			return fmt.Errorf("plan safety: explicit token automount disablement is required")
		}
	}
	for _, path := range [][]string{{"spec"}, {"spec", "jobTemplate", "spec", "template", "spec"}} {
		if object.GetKind() != "Pod" && len(path) == 1 {
			continue
		}
		if object.GetKind() != "CronJob" && len(path) > 1 {
			continue
		}
		pod, found, err := unstructured.NestedMap(object.Object, path...)
		if err != nil || !found || pod["automountServiceAccountToken"] != false {
			return fmt.Errorf("plan safety: explicit token automount disablement is required")
		}
	}
	return nil
}

func (e PlanExecutor) safetyNow() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}

// PlanIdentityInspector is a read-only account metadata projection.
type PlanIdentityInspector interface {
	TokenlessServiceAccount(context.Context, sdk.ObjectRef) (string, error)
}

func (e PlanExecutor) validateServiceAccount(ctx context.Context, ref sdk.ObjectRef, after *unstructured.Unstructured) error {
	name, _, _ := unstructured.NestedString(after.Object, "spec", "template", "spec", "serviceAccountName")
	account := sdk.ObjectRef{APIVersion: "v1", Kind: "ServiceAccount", Namespace: ref.Namespace, Name: name}
	expected := e.Safety.ServiceAccounts[account]
	reader, ok := e.Client.(PlanIdentityInspector)
	if !ok || expected == "" {
		return fmt.Errorf("plan safety: existing powerless service account is not authorized")
	}
	uid, err := reader.TokenlessServiceAccount(ctx, account)
	if err != nil || uid != expected {
		return fmt.Errorf("plan safety: existing tokenless service account identity is unproven")
	}
	return nil
}

// checkObservedObject authorizes the independently approved image and applies
// the hard predicate to the complete live object. Callers separately compare
// the fields their plan owns; API defaults and controller metadata are unowned.
func (p *PlanSafetyPolicy) checkObservedObject(live, approved *unstructured.Unstructured, ref sdk.ObjectRef, created bool) error {
	if live == nil || approved == nil || !samePlanRef(live, ref) {
		return fmt.Errorf("plan safety: observed identity is unavailable")
	}
	if err := planSecretPredicate(live); err != nil {
		return err
	}
	expected := approved.DeepCopy()
	if created {
		expected.SetUID("")
	}
	if err := p.checkObject(expected, ref); err != nil {
		return err
	}
	if !created && (live.GetUID() == "" || string(live.GetUID()) != p.Objects[ref].UID) {
		return fmt.Errorf("plan safety: observed UID changed")
	}
	return nil
}
