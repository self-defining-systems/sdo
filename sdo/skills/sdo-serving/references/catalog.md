# Renderer and image catalog

The override `.sdo/deployment/plan.yaml` names at most one renderer and any image sources. Unknown IDs and configs
that do not parse are rejected at load (`deployment.invalid`). Paths are repository-relative.

## Renderers (override `source`)

- `raw`: `paths` lists manifest files or directories of plain YAML. A kustomization or chart root is rejected: use
  the other renderers.
- `kustomize`: `path` is one kustomization directory.
- `helm`: `chart_path`, `value_files` (for example `chart/values-kind.yaml`) and inline `values`. A values file is
  the way to change a chart's behavior without editing it.

## Image sources (override `images[]`)

- `buildkit`: `context_path` (the directory the Dockerfile builds from) and `dockerfile_path`. Go builds the
  committed tree and pushes to the trusted registry, so the image name must start with that host.
- `prebuilt`: no build; the operator pins third-party tags to digests. Never write digests yourself.

## plan.yaml shape

```yaml
source:
  rendererId: kustomize
  config:
    "@type": type.googleapis.com/sdodev.contracts.v1alpha1.KustomizeRenderConfig
    path: deploy/overlays/kind
images:
  - image: registry.example:5000/api:v1
    sourceId: buildkit
    config:
      "@type": type.googleapis.com/sdodev.contracts.v1alpha1.BuildKitImageConfig
      contextPath: services/api
      dockerfilePath: deploy/build/api/Dockerfile
```

Both fields are optional: without `source` Go derives the renderer from the files, and images it can match to a
Dockerfile need no entry. A declaration that must keep matching later upstream releases names an image with no tag
and no digest (`image: registry.example/api`): it covers every tag of that repository.
