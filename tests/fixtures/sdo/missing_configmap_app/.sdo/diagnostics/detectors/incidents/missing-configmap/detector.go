package missing_configmap

import (
	"context"
	"fmt"
	"time"

	"sdo.dev/operator/sdk"
)

const playbookPath = "missing-configmap"

type Detector struct{}

func New() sdk.Detector {
	return Detector{}
}

func (Detector) Spec() sdk.DetectorSpec {
	return sdk.DetectorSpec{
		ID:          "missing-configmap",
		Class:       sdk.DetectorClassIncident,
		Owner:       sdk.DetectorOwnerResponder,
		Description: "find Deployments that reference absent required ConfigMaps",
		Interval:    30 * time.Second,
		Persistence: sdk.PersistencePolicy{Firing: 2, Clearing: 2},
		Batching:    sdk.BatchingPolicy{Severity: sdk.SeverityCritical, Debounce: 500 * time.Millisecond},
		OriginatingIncident: "missing-configmap-fixture",
		OriginatingCommit:   "fixture123",
		Watches: []sdk.WatchKind{
			{APIVersion: "apps/v1", Kind: "Deployment"},
			{APIVersion: "v1", Kind: "ConfigMap"},
		},
		Playbooks: []string{playbookPath},
	}
}

func (Detector) Detect(_ context.Context, snapshot sdk.DetectionContext) ([]sdk.Finding, error) {
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
			key := deployment.Namespace + "/" + reference.Name
			if _, ok := existing[key]; ok {
				continue
			}

			deploymentRef := sdk.ObjectRefFrom("Deployment", "apps/v1", &deployment)
			configMapRef := sdk.ObjectRef{
				APIVersion: "v1",
				Kind:       "ConfigMap",
				Namespace:  deployment.Namespace,
				Name:       reference.Name,
			}
			findings = append(findings, sdk.Finding{
				RuleID:          "missing-configmap",
				Status:          sdk.FindingActive,
				Severity:        sdk.SeverityCritical,
				Summary:         "Deployment references an absent required ConfigMap",
				Evidence:        fmt.Sprintf("Deployment %s/%s requires ConfigMap %s", deployment.Namespace, deployment.Name, reference.Name),
				PrimaryResource: deploymentRef,
				RelatedResources: []sdk.ObjectRef{
					configMapRef,
				},
				Playbooks: []string{playbookPath},
				ParameterBindings: map[string]sdk.ObjectRef{
					"target_resource":    deploymentRef,
					"missing_config_map": configMapRef,
				},
				Fingerprint: deployment.Namespace + "/" + deployment.Name + "/" + reference.Name,
			})
		}
	}
	return findings, nil
}
