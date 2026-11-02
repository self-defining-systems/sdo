package runtime

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

const admissionRetryAnnotation = "sdo.dev/admission-retry"

type admissionReplicaSets interface {
	AdmissionReplicaSets(context.Context, string) ([]appsv1.ReplicaSet, error)
}

func (c TypedPlanClient) AdmissionReplicaSets(ctx context.Context, namespace string) ([]appsv1.ReplicaSet, error) {
	if namespace == "" {
		return nil, fmt.Errorf("admission retries require an incident namespace")
	}
	if err := c.checkPlanNamespace(namespace); err != nil {
		return nil, err
	}
	if c.Client == nil {
		return nil, fmt.Errorf("Kubernetes plan client is required")
	}
	sets, err := c.Client.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return sets.Items, nil
}

// Resolve affected identities before any writes, then execute the recovery in
// the same batch. This covers stage, learned programs and raw responder actions,
// including deletion, fail-open patches, source restores and private CA sync.
func (e PlanExecutor) withAdmissionRetries(ctx context.Context, incident IncidentRequest, plan program.Plan) (program.Plan, error) {
	if incident.Namespace == "" {
		return plan, nil
	}
	names := map[string]bool{}
	explicit := map[sdk.ObjectRef]int{}
	lastRepair := -1
	for index, action := range plan.Actions {
		switch a := action.(type) {
		case program.RetryAdmission:
			explicit[a.Ref] = index
		case *program.RetryAdmission:
			if a != nil {
				explicit[a.Ref] = index
			}
		}
		ref, err := planActionRef(action)
		if err != nil || ref.APIVersion != "admissionregistration.k8s.io/v1" || (ref.Kind != "ValidatingWebhookConfiguration" && ref.Kind != "MutatingWebhookConfiguration") {
			continue
		}
		switch a := generalAction(action).(type) {
		case program.WaitFor:
			continue
		case program.KubernetesRequest:
			if a.Method == "GET" || a.Method == "HEAD" {
				continue
			}
		case program.SyncWebhookCA:
			names[a.Target.Webhook] = true
			lastRepair = index
			continue
		}
		lastRepair = index
		if e.Client == nil {
			return plan, fmt.Errorf("admission recovery requires an object client")
		}
		object, err := e.Client.Get(ctx, ref)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return plan, fmt.Errorf("read admission recovery configuration: %w", err)
		}
		if object == nil || !samePlanRef(object, ref) {
			return plan, fmt.Errorf("admission recovery configuration identity unavailable")
		}
		entries, _, err := unstructured.NestedSlice(object.Object, "webhooks")
		if err != nil {
			return plan, err
		}
		for _, entry := range entries {
			if webhook, ok := entry.(map[string]any); ok {
				if name, ok := webhook["name"].(string); ok {
					names[name] = true
				}
			}
		}
	}
	if len(names) == 0 {
		return plan, nil
	}
	client, ok := e.Client.(admissionReplicaSets)
	if !ok {
		return plan, fmt.Errorf("admission recovery ReplicaSet observation unavailable")
	}
	sets, err := client.AdmissionReplicaSets(ctx, incident.Namespace)
	if err != nil {
		return plan, fmt.Errorf("observe admission-blocked ReplicaSets: %w", err)
	}
	sort.Slice(sets, func(i, j int) bool { return sets[i].Name < sets[j].Name })
	actions := append([]program.Action(nil), plan.Actions...)
	for _, set := range sets {
		// Never trust the list transport to enforce the incident boundary.
		if set.Namespace != incident.Namespace {
			continue
		}
		affected := false
		for _, message := range program.AdmissionFailedCreate(set) {
			for _, match := range admissionWebhookName.FindAllStringSubmatch(message, -1) {
				affected = affected || names[match[1]]
			}
		}
		ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: set.Namespace, Name: set.Name}
		explicitIndex, hasExplicit := explicit[ref]
		if affected && (!hasExplicit || explicitIndex < lastRepair) {
			actions = append(actions, program.RetryAdmission{Ref: ref})
		}
	}
	plan.Actions = actions
	return plan, nil
}

// Append the actual recovery actions to the durable application. Each action
// retains its own live write proof and ordinary failure/compensation contract.
func (e PlanExecutor) applyAdmissionRetries(ctx context.Context, incident IncidentRequest, actions []program.Action, result *PlanApplicationResult) error {
	for _, action := range actions {
		nested := e
		nested.deferConvergence = true
		step, err := nested.Apply(ctx, incident, program.Plan{Actions: []program.Action{action}}, false)
		offset := len(result.Plan.Actions)
		result.Plan.Actions = append(append([]program.Action(nil), result.Plan.Actions...), step.Plan.Actions...)
		result.ActionsApplied = append(result.ActionsApplied, step.ActionsApplied...)
		result.Patches = append(result.Patches, step.Patches...)
		result.PreImages = append(result.PreImages, step.PreImages...)
		result.ObjectEffects = append(result.ObjectEffects, step.ObjectEffects...)
		for _, report := range step.ActionReports {
			report.ActionIndex += offset
			result.ActionReports = append(result.ActionReports, report)
		}
		result.Applied = result.Applied || step.Applied
		if err != nil {
			return fmt.Errorf("admission recovery: %w", err)
		}
	}
	return nil
}

func isAdmissionRetry(action program.Action) bool {
	switch action.(type) {
	case program.RetryAdmission, *program.RetryAdmission:
		return true
	}
	return false
}

// The retry writes a ReplicaSet, but its controlling Deployment owns rollout
// completion. Preserve that relationship for the ordinary live rollout wait.
func (e PlanExecutor) admissionRetryRolloutRefs(ctx context.Context, result PlanApplicationResult) ([]sdk.ObjectRef, error) {
	var refs []sdk.ObjectRef
	seen := map[sdk.ObjectRef]bool{}
	for index, action := range result.ActionsApplied {
		if !isAdmissionRetry(action) || index >= len(result.Patches) {
			continue
		}
		patch := result.Patches[index]
		if patch.After == nil {
			continue
		}
		for _, owner := range patch.After.GetOwnerReferences() {
			if owner.Controller == nil || !*owner.Controller || owner.APIVersion != "apps/v1" || owner.Kind != "Deployment" {
				continue
			}
			ref := sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: patch.Ref.Namespace, Name: owner.Name}
			if seen[ref] {
				continue
			}
			live, err := e.Client.Get(ctx, ref)
			if err != nil {
				return nil, fmt.Errorf("admission recovery owner observation: %w", err)
			}
			if live == nil || !samePlanRef(live, ref) || owner.UID == "" || live.GetUID() != owner.UID {
				return nil, fmt.Errorf("admission recovery owner identity changed")
			}
			seen[ref] = true
			refs = append(refs, ref)
		}
	}
	return refs, nil
}
