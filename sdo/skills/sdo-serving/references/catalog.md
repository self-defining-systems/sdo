# Renderer and image catalog

A plan names one renderer and zero or more image sources. Unknown IDs and configs that do not parse are rejected at
load (`deployment.invalid`). All paths address the effective tree.

## Renderers (plan `source`)

- `raw`: `paths` lists manifest files or directories of plain YAML. A kustomization or chart root is rejected: use the
  other renderers. An agent-authored plan usually uses `raw` over its manifest paths.
- `kustomize`: `path` is one kustomization directory; its patches and resources may live in `values/`.
- `helm`: `chart_path`, `value_files` (for example `.sdo/deployment/values/prod.yaml`) and inline `values`.
  Human-provided plans use these; a values file is the way to change a chart's behavior without a patch.

## Image sources (plan `images[]`)

- `buildkit`: `context_path` (the directory the Dockerfile builds from) and `dockerfile_path`. Go builds the exact
  effective tree and pushes to the trusted registry, so the image name must start with that host.
- `prebuilt`: no build; the operator pins third-party tags to digests. Never write digests yourself.

## plan.yaml shape (for `deployment render --plan`)

```yaml
source:
  rendererId: raw
  config:
    "@type": type.googleapis.com/sdodev.contracts.v1alpha1.RawRenderConfig
    paths: [.sdo/deployment/manifests/app.yaml]
images:
  - image: registry.example:5000/api:v1
    sourceId: buildkit
    config:
      "@type": type.googleapis.com/sdodev.contracts.v1alpha1.BuildKitImageConfig
      contextPath: services/api
      dockerfilePath: .sdo/deployment/build/api/Dockerfile
expectedWorkloadIds: [Deployment/demo/api]
```

A plan that must keep matching later upstream releases declares release-independent entries: an image with no tag
and no digest (`image: registry.example/api`) covers every tag of that repository, and a Job whose name carries a
generated suffix (a hash of its inputs, five to ten lowercase alphanumerics) is `Job/demo/migrate-*`. `*` is allowed
only as that final `-*`; every other workload keeps its literal ID, and a renamed Deployment is a different workload.
