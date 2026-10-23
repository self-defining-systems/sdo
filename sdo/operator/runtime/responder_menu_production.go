package runtime

import (
	"context"
	"errors"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"sdo.dev/operator/sdk"
)

// Named fixture approvals do not authorize namespace-wide opaque reads. An
// embedded trusted controller may supply the existing safety capabilities;
// the standalone launch has no producer for that evidence and refuses.
func productionResponderMenu(request IncidentRequest, client kubernetes.Interface, detectors []sdk.Detector, policy *PlanSafetyPolicy, evidence *ResponderMenuSafety) *ResponderMenuConfig {
	dependencies := []sdk.WatchKind{}
	for _, kind := range []sdk.WatchKind{{APIVersion: "v1", Kind: "ConfigMap"}, {APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Pod"}, {APIVersion: "v1", Kind: "Endpoints"}, {APIVersion: "v1", Kind: "Event"}, {APIVersion: "apps/v1", Kind: "Deployment"}, {APIVersion: "apps/v1", Kind: "ReplicaSet"}, {APIVersion: "apps/v1", Kind: "StatefulSet"}, {APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"}, {APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"}, {APIVersion: "autoscaling/v2", Kind: "HorizontalPodAutoscaler"}, {APIVersion: "policy/v1", Kind: "PodDisruptionBudget"}} {
		kind.Namespace = request.Namespace
		dependencies = append(dependencies, kind)
	}
	// ProgramSpec has no read-dependency declaration: collect the entire supported
	// namespace, rather than guessing from parameter bindings. Foreign watches
	// and non-object health producers remain unavailable in this smallest version.
	refusal := func() string {
		if policy == nil || policy.SourceCommit != request.SourceCommit || !time.Now().Before(policy.ExpiresAt) {
			return "plan_safety_authority_unavailable"
		}
		if evidence == nil || evidence.Digest == "" || evidence.AuthorizeRead == nil || evidence.AuthorizeObject == nil || evidence.AuthorizeEffect == nil {
			return "collection_consumer_admission_provenance_unavailable"
		}
		for _, detector := range detectors {
			for _, kind := range detector.Spec().Watches {
				if kind.Namespace == "" {
					kind.Namespace = request.Namespace
				}
				if kind.Namespace != request.Namespace || !causalSupportedKind(kind) {
					return "health_dependency_reader_unavailable"
				}
			}
		}
		return ""
	}
	authorized := func() error {
		if reason := refusal(); reason != "" {
			return errors.New(reason)
		}
		return nil
	}
	safety := &ResponderMenuSafety{Digest: menuDigest([]string{productionMenuPolicyDigest(policy), func() string {
		if evidence != nil {
			return evidence.Digest
		}
		return ""
	}()})}
	safety.AuthorizeRead = func(ctx context.Context, kind sdk.WatchKind) error {
		if err := authorized(); err != nil {
			return err
		}
		if kind.Namespace != request.Namespace || !causalSupportedKind(kind) {
			return errMenuSafety
		}
		return evidence.AuthorizeRead(ctx, kind)
	}
	safety.AuthorizeObject = func(ctx context.Context, ref sdk.ObjectRef, object *unstructured.Unstructured) error {
		if err := authorized(); err != nil {
			return err
		}
		if err := policy.authorizeRead(ref, request.SourceCommit, time.Now()); err != nil {
			return err
		}
		if err := policy.checkObject(object, ref); err != nil {
			return err
		}
		return evidence.AuthorizeObject(ctx, ref, object)
	}
	safety.AuthorizeEffect = func(ctx context.Context, ref sdk.ObjectRef, before, after *unstructured.Unstructured) error {
		if err := authorized(); err != nil {
			return err
		}
		if err := policy.authorizeRead(ref, request.SourceCommit, time.Now()); err != nil {
			return err
		}
		proof := policy.Objects[ref]
		if before == nil && !proof.CreateAllowed || after == nil && (!proof.DeleteAllowed || before == nil || string(before.GetUID()) != proof.UID) {
			return errMenuSafety
		}
		for _, object := range []*unstructured.Unstructured{before, after} {
			if object != nil {
				if err := safety.AuthorizeObject(ctx, ref, object); err != nil {
					return err
				}
			}
		}
		return evidence.AuthorizeEffect(ctx, ref, before, after)
	}
	return &ResponderMenuConfig{Reader: KubernetesCurrentReader{TypedClient: client}, Dependencies: dependencies, Safety: safety, Refusal: refusal, Healthy: func(ctx context.Context, view sdk.DetectionContext) (healthy bool, err error) {
		defer func() {
			if recover() != nil {
				healthy = false
				err = errCausalObservation
			}
		}()
		found := false
		for _, detector := range detectors {
			if detector.Spec().Class != sdk.DetectorClassHealth || detector.Spec().Owner != sdk.DetectorOwnerHealthJudge {
				continue
			}
			found = true
			findings, err := detector.Detect(ctx, view)
			if err != nil {
				return false, err
			}
			if len(findings) > 0 {
				return false, nil
			}
		}
		return found, nil
	}}
}

func productionMenuPolicyDigest(policy *PlanSafetyPolicy) string {
	if policy == nil {
		return "unconfigured"
	}
	objects := []string{}
	for ref, proof := range policy.Objects {
		objects = append(objects, menuDigest([]any{ref, proof}))
	}
	for ref, uid := range policy.ServiceAccounts {
		objects = append(objects, menuDigest([]any{ref, uid}))
	}
	sort.Strings(objects)
	return menuDigest([]any{policy.SourceCommit, policy.ExpiresAt, objects})
}
