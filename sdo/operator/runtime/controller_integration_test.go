package runtime

import (
	"context"
	"testing"
	"time"

	"sdo.dev/operator/sdk"
)

type missingConfigMapDetector struct{}

func (missingConfigMapDetector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID: "missing-configmap", Interval: time.Second,
		Watches: []sdk.WatchKind{{APIVersion: "apps/v1", Kind: "Deployment"}, {APIVersion: "v1", Kind: "ConfigMap"}, {APIVersion: "apps/v1", Kind: "ReplicaSet"}, {APIVersion: "v1", Kind: "Pod"}},
	}
}

func (missingConfigMapDetector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
	existing := make(map[string]struct{})
	for _, configMap := range snapshot.ConfigMaps() {
		existing[configMap.Namespace+"/"+configMap.Name] = struct{}{}
	}
	findings := make([]sdk.Finding, 0)
	for _, deployment := range snapshot.Deployments() {
		for _, reference := range sdk.ConfigMapReferencesForDeployment(deployment) {
			if reference.Optional {
				continue
			}
			if _, ok := existing[deployment.Namespace+"/"+reference.Name]; ok {
				continue
			}
			deploymentRef := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
			configMapRef := sdk.ObjectRef{APIVersion: "v1", Kind: "ConfigMap", Namespace: deployment.Namespace, Name: reference.Name}
			findings = append(findings, sdk.Finding{
				RuleID: "missing-configmap", Status: sdk.FindingActive, Severity: sdk.SeverityCritical,
				Summary: "required ConfigMap is absent", Evidence: deployment.Name + " requires " + reference.Name,
				PrimaryResource: deploymentRef, RelatedResources: []sdk.ObjectRef{configMapRef},
				ParameterBindings: map[string]sdk.ObjectRef{"target_resource": deploymentRef, "missing_config_map": configMapRef},
				Fingerprint:       deployment.Namespace + "/" + deployment.Name + "/" + reference.Name,
			})
		}
	}
	return findings, nil
}

func awaitEventValue(t *testing.T, cache *KubernetesCache) []sdk.WatchKind {
	t.Helper()
	<-cache.Notifications()
	return cache.TakeEvents()
}
