package validate

import (
	"testing"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/planfile"
	"sdo.dev/operator/render"
	"sdo.dev/operator/render/raw"
)

const rawPlan = `source:
  rendererId: raw
  config:
    "@type": type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig
    paths: [k8s]
`

const deploymentManifest = `apiVersion: apps/v1
kind: Deployment
metadata: {name: web, namespace: shop}
spec:
  selector: {matchLabels: {app: web}}
  template:
    metadata: {labels: {app: web}}
    spec: {containers: [{name: web, image: "web:1"}]}
---
apiVersion: v1
kind: Service
metadata: {name: web, namespace: shop}
spec:
  selector: {app: web}
  ports: [{port: 80}]
`

const oneComponentMap = `version: 1
components:
  - {name: web, workload: Deployment/web, services: [web], holds_data: false}
`

func rawCatalog(t *testing.T) *render.Catalog {
	t.Helper()
	catalog := render.NewCatalog()
	if err := raw.Register(catalog); err != nil {
		t.Fatal(err)
	}
	return catalog
}

// sourceTree is a committed checkout whose arch.md has a map but no manifest
// inventory section.
func sourceTree(t *testing.T, mapYAML string) string {
	root := goodTree(t)
	write(t, root, layout.ArchitectureFile, "---\nschema_version: 1\nowner: onboarding\n---\n# Architecture\n\n"+mapRegion(mapYAML))
	write(t, root, layout.DeploymentPlanFile, rawPlan)
	write(t, root, "k8s/web.yaml", deploymentManifest)
	commit(t, root, "source")
	return root
}

func unknownNames(t *testing.T, root string) []string {
	t.Helper()
	request := Request{Root: root, Layers: Structural | Semantic, Actor: HealthJudge, Phase: InitialPhase, Renderers: rawCatalog(t), PlanCatalogs: planfile.Catalogs{}}
	var names []string
	for _, d := range diagnose(t, request) {
		if d.Rule == RuleMapUnknownName {
			names = append(names, d.Message)
		}
	}
	return names
}

func TestMapNamesResolveAgainstManifestsWithoutInventorySection(t *testing.T) {
	root := sourceTree(t, oneComponentMap)
	if got := unknownNames(t, root); len(got) != 0 {
		t.Fatalf("a map naming manifest workloads must pass without an arch.md inventory section: %v", got)
	}
}

func TestMapNamingWorkloadAbsentFromManifestsFails(t *testing.T) {
	root := sourceTree(t, "version: 1\ncomponents:\n  - {name: ghost, workload: Deployment/ghost, services: [ghost], holds_data: false}\n")
	if got := unknownNames(t, root); len(got) == 0 {
		t.Fatal("a map naming a workload absent from the manifests must fail map.unknown-name")
	}
}

func TestStaleInventorySectionDoesNotMaskManifests(t *testing.T) {
	root := sourceTree(t, oneComponentMap)
	write(t, root, layout.ArchitectureFile, "# Manifest inventory\n\n- Deployment/shop/old (source: k8s/old.yaml)\n\n"+mapRegion(oneComponentMap))
	commit(t, root, "stale section")
	if got := unknownNames(t, root); len(got) != 0 {
		t.Fatalf("manifests are the source, not a stale section: %v", got)
	}
}
