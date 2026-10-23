package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/rest"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"strings"
	"sync"
	"time"
)

type SourcePlanPolicy struct {
	Version      int               `json:"version"`
	Authority    string            `json:"authority"`
	Applications map[string]string `json:"applications"`
	FieldRepairs string            `json:"field_repairs"`
	CreateKinds  []string          `json:"create_kinds"`
	DeleteKinds  []string          `json:"delete_kinds"`
	WebhookNames []string          `json:"webhook_names,omitempty"`
	TTLSeconds   int               `json:"ttl_seconds"`
}

func LoadSourcePlanPolicy(raw string) (*SourcePlanPolicy, error) {
	if len(raw) > 65536 {
		return nil, fmt.Errorf("source plan policy exceeds size limit")
	}
	var c SourcePlanPolicy
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || c.Version != 1 || c.Authority != "accepted-source-and-incident" || c.FieldRepairs != "accepted-source-only" || len(c.Applications) == 0 || c.TTLSeconds < 1 || c.TTLSeconds > 3600 {
		return nil, fmt.Errorf("source plan policy requires bounded accepted-source-and-incident authority")
	}
	for app, ns := range c.Applications {
		if app == "" || ns == "" || strings.ContainsAny(app+ns, "*/\\ \t\n") {
			return nil, fmt.Errorf("source plan policy requires exact applications and namespaces")
		}
	}
	for _, name := range c.WebhookNames {
		if len(validation.IsDNS1123Subdomain(name)) > 0 {
			return nil, fmt.Errorf("source policy webhook must be an exact name")
		}
	}
	for _, kind := range c.CreateKinds {
		if kind != "v1/ConfigMap" && kind != "v1/Service" {
			return nil, fmt.Errorf("source plan policy create kind forbidden")
		}
	}
	for _, kind := range c.DeleteKinds {
		switch kind {
		case "networking.k8s.io/v1/NetworkPolicy", "admissionregistration.k8s.io/v1/MutatingWebhookConfiguration", "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration":
		default:
			return nil, fmt.Errorf("source plan policy delete kind forbidden")
		}
	}
	return &c, nil
}

// SourcePlanAuthority derives one immutable epoch from accepted source and the
// controller incident. It has no plan, responder-result or dry-run input.
type SourcePlanAuthority struct {
	Config     *SourcePlanPolicy
	Source     PlanSource
	UID        func(context.Context, sdk.ObjectRef) (types.UID, error)
	mu         sync.Mutex
	policy     *PlanSafetyPolicy
	incidentID string
	err        error
}

func (a *SourcePlanAuthority) Policy(ctx context.Context, incident IncidentRequest) (*PlanSafetyPolicy, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.policy != nil || a.err != nil {
		if a.incidentID != incident.IncidentID || (a.policy != nil && a.policy.SourceCommit != incident.SourceCommit) {
			return nil, fmt.Errorf("source policy incident epoch changed")
		}
		return a.policy, a.err
	}
	a.incidentID = incident.IncidentID
	a.policy, a.err = a.generate(ctx, incident)
	return a.policy, a.err
}
func (a *SourcePlanAuthority) generate(ctx context.Context, incident IncidentRequest) (*PlanSafetyPolicy, error) {
	if a.Config == nil || a.UID == nil || incident.IncidentID == "" || incident.SourceCommit == "" {
		return nil, fmt.Errorf("source plan policy requires controller incident and metadata identity reader")
	}
	ns, ok := a.Config.Applications[incident.Application]
	if !ok {
		for _, allowed := range a.Config.Applications {
			if incident.Application == allowed {
				ns = allowed
				ok = true
			}
		}
	}
	if !ok || ns != incident.Namespace {
		return nil, fmt.Errorf("source plan policy application namespace is outside reviewed scope")
	}
	inventory, ok := a.Source.(interface {
		AcceptedManifestRefs(context.Context, string, string) ([]sdk.ObjectRef, error)
	})
	if !ok {
		return nil, fmt.Errorf("source plan policy requires accepted render inventory")
	}
	objects, err := inventory.AcceptedManifestRefs(ctx, incident.SourceCommit, ns)
	if err != nil {
		return nil, fmt.Errorf("source plan policy accepted render evidence unavailable")
	}
	p := &PlanSafetyPolicy{SourceCommit: incident.SourceCommit, ExpiresAt: time.Now().Add(time.Duration(a.Config.TTLSeconds) * time.Second), Objects: map[sdk.ObjectRef]PlanObjectSafety{}, ServiceAccounts: map[sdk.ObjectRef]string{}, SourceImages: map[sdk.ObjectRef]*unstructured.Unstructured{}, IncidentID: incident.IncidentID}
	if !incident.ResponseDeadline.IsZero() && incident.ResponseDeadline.Before(p.ExpiresAt) {
		p.ExpiresAt = incident.ResponseDeadline
	}

	sourceRefs := map[sdk.ObjectRef]bool{}
	for _, ref := range objects {
		sourceRefs[ref] = true
	}
	for _, ref := range objects {
		if ref.Namespace != ns || !sourcePlanKind(ref) {
			continue
		}
		object, sourceErr := a.Source.AcceptedManifest(ctx, incident.SourceCommit, ref)
		if sourceErr != nil || object == nil || !samePlanRef(object, ref) || planSecretPredicate(object) != nil {
			continue
		}

		uid, err := a.UID(ctx, ref)
		create := containsPolicyKind(a.Config.CreateKinds, ref)
		if apierrors.IsNotFound(err) {
			if !create {
				continue
			}
			uid = ""
		} else if err != nil {
			return nil, fmt.Errorf("source policy metadata identity unavailable")
		}
		if err == nil && uid == "" {
			return nil, fmt.Errorf("source policy metadata UID absent")
		}
		p.Objects[ref] = PlanObjectSafety{UID: string(uid), CreateAllowed: create, Digests: map[string]bool{planSafetyDigest(object): true}}
		p.SourceImages[ref] = object.DeepCopy()
	}
	// Only admitted, individually named incident resources can acquire delete
	// authority; a namespace/kind match alone never generates a delete grant.
	refs := map[sdk.ObjectRef]bool{}
	for _, f := range incident.Findings {
		refs[f.PrimaryResource] = true
		for _, ref := range f.RelatedResources {
			refs[ref] = true
		}
	}
	for _, event := range incident.DriftEvents {
		refs[event.Ref] = true
	}
	for ref := range refs {
		// The detector contract permits kind/name references without a version.
		// Resolve only the explicit delete kinds; never discover an arbitrary GVR.
		if ref.APIVersion == "" {
			switch ref.Kind {
			case "NetworkPolicy":
				ref.APIVersion = "networking.k8s.io/v1"
			case "MutatingWebhookConfiguration", "ValidatingWebhookConfiguration":
				ref.APIVersion = "admissionregistration.k8s.io/v1"
			}
		}
		if ref.Kind == "NetworkPolicy" && ref.Namespace == "" {
			ref.Namespace = ns
		}
		if !containsPolicyKind(a.Config.DeleteKinds, ref) || ref.Name == "" || (ref.Namespace != "" && ref.Namespace != ns) {
			continue
		}

		if sourceRefs[ref] {
			continue
		}
		if ref.Namespace == "" {
			named := false
			for _, name := range a.Config.WebhookNames {
				named = named || name == ref.Name
			}
			if !named {
				continue
			}
		}
		// Absence from lock identities only nominates the candidate. The executor
		// still proves authoritative accepted-source absence before every delete;
		// a missing/stale/omitted source manifest never becomes deletion proof here.

		uid, err := a.UID(ctx, ref)
		if err != nil || uid == "" {
			continue
		}
		p.Objects[ref] = PlanObjectSafety{UID: string(uid), DeleteAllowed: true}
	}
	return p, nil
}
func sourcePlanKind(ref sdk.ObjectRef) bool {
	switch ref.APIVersion + "/" + ref.Kind {
	case "v1/ConfigMap", "v1/Service", "apps/v1/Deployment", "networking.k8s.io/v1/NetworkPolicy", "autoscaling/v2/HorizontalPodAutoscaler", "policy/v1/PodDisruptionBudget":
		return true
	}
	return false
}
func containsPolicyKind(kinds []string, ref sdk.ObjectRef) bool {
	for _, kind := range kinds {
		if kind == ref.APIVersion+"/"+ref.Kind {
			return true
		}
	}
	return false
}
func (p *PlanSafetyPolicy) enforceSourceAction(action program.Action, current, next *unstructured.Unstructured) error {
	if p == nil || p.SourceImages == nil || !structuredPlanAction(action) {
		return nil
	}
	switch a := action.(type) {
	case *program.SetField:
		if a != nil {
			return p.enforceSourceAction(*a, current, next)
		}
	case program.SetField:
		source := p.SourceImages[a.Ref]
		if source == nil {
			return fmt.Errorf("source-bound repair has no accepted object")
		}
		expected := current.DeepCopy()
		a.Value = program.FromSource(a.Ref, a.Path)
		value, _, resolveErr := program.ResolveSetFieldValue(a, program.ValueInputs{SourceCommit: p.SourceCommit, Source: func(sdk.ObjectRef) (map[string]any, error) { return source.Object, nil }})
		if resolveErr != nil {
			return fmt.Errorf("source-bound repair value unavailable")
		}
		if err := program.ApplySetFieldAction(expected.Object, a, value); err != nil {
			return fmt.Errorf("source-bound repair value unavailable")
		}
		if !planValuesEqual(expected.Object, next.Object) {
			return fmt.Errorf("structured stream repair must restore accepted-source value")
		}
	}
	return nil
}

// sourcePolicyUIDReader never asks for Secret resources or opaque object bodies.
func sourcePolicyUIDReader(client rest.Interface) func(context.Context, sdk.ObjectRef) (types.UID, error) {
	return func(ctx context.Context, ref sdk.ObjectRef) (types.UID, error) {
		resources := map[string]string{"v1/ConfigMap": "configmaps", "v1/Service": "services", "apps/v1/Deployment": "deployments", "networking.k8s.io/v1/NetworkPolicy": "networkpolicies", "autoscaling/v2/HorizontalPodAutoscaler": "horizontalpodautoscalers", "policy/v1/PodDisruptionBudget": "poddisruptionbudgets", "admissionregistration.k8s.io/v1/MutatingWebhookConfiguration": "mutatingwebhookconfigurations", "admissionregistration.k8s.io/v1/ValidatingWebhookConfiguration": "validatingwebhookconfigurations"}
		if len(validation.IsDNS1123Subdomain(ref.Name)) > 0 || (ref.Namespace != "" && len(validation.IsDNS1123Label(ref.Namespace)) > 0) {
			return "", fmt.Errorf("source policy metadata identity invalid")
		}
		resource := resources[ref.APIVersion+"/"+ref.Kind]
		if client == nil || resource == "" {
			return "", fmt.Errorf("source policy metadata kind unavailable")
		}
		prefix := "/api/" + ref.APIVersion
		if strings.Contains(ref.APIVersion, "/") {
			prefix = "/apis/" + ref.APIVersion
		}
		if ref.Namespace != "" {
			prefix += "/namespaces/" + ref.Namespace
		}
		var object metav1.PartialObjectMetadata
		err := client.Get().AbsPath(prefix, resource, ref.Name).SetHeader("Accept", "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1").Do(ctx).Into(&object)
		if err == nil && ((object.Kind != "" && object.Kind != "PartialObjectMetadata") || object.Name != ref.Name || object.Namespace != ref.Namespace) {
			return "", fmt.Errorf("source policy metadata identity mismatch")
		}
		return object.UID, err
	}
}

func (a *SourcePlanAuthority) cachedPolicy() *PlanSafetyPolicy {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.policy
}
func planAuthorizationMode(action program.Action, sourcePolicy bool) string {
	if sourcePolicy && structuredPlanAction(action) {
		if objectActionOperation(action) == "delete" {
			return "incident-named-delete"
		}
		return "accepted-source"
	}
	switch a := action.(type) {
	case program.SetField:

		if a.Value.Origin() == program.SourceValue {
			return "accepted-source"
		}
		return string(a.Value.Origin())
	case *program.SetField:
		if a != nil {
			return planAuthorizationMode(*a, sourcePolicy)
		}
	case program.ReconcileFromSource, *program.ReconcileFromSource:
		return "accepted-source"
	}
	return "reviewed-fixture"
}
