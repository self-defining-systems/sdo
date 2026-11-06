---
name: sdo-deploy
description: Author the deployment files SDO deploys from. Covers the .sdo/deployment layout, renderer and image catalog, values versus overlays versus patches, and offline rendering.
---

# Deploying with `.sdo/deployment/`

The deployer writes only `.sdo/`. Everything it adds to make the application run lives in `.sdo/deployment/`; Go
renders and builds the **effective tree**: the repository, plus its patch series, plus these files. Application
files are never edited ([sdo-patching](../sdo-patching/SKILL.md) covers the exception).

## Layout

| Path | Holds | Writer |
|---|---|---|
| `plan.yaml` | what is deployed: renderer, images, expected workloads | Go, from the plan you return; never edit it |
| `manifests/**` | authored Kubernetes manifests (compose-only or manifest-less apps, missing config) | deployer |
| `build/**` | authored Dockerfiles, `.dockerignore` | deployer |
| `values/**` | inputs for renderers that read values (Helm value files, kustomize patches) | deployer |
| `overlays/telemetry/**` | the detection designer's slot | not yours |
| `patches/NNNN-<slug>.patch` | reasoned changes to upstream files | [sdo-patching](../sdo-patching/SKILL.md) |

Any other path under `.sdo/deployment/` is rejected. Plan paths address the effective tree, so they read
`.sdo/deployment/manifests/app.yaml`, a context `services/api`, or a `dockerfile_path` of
`.sdo/deployment/build/api/Dockerfile` (with `context_path` still the upstream directory).

## Choosing where a change goes

1. Missing or wrong deployment inputs (a manifest, a Dockerfile the repo lacks, config values): author a file in
   `manifests/`, `build/` or `values/`. This is the default.
2. Use existing upstream manifests or a chart as they are, and put differences in `values/` (or a kustomize layer).
3. An upstream file must itself differ: a patch with a reason. Last resort.
4. Overlays are the detection designer's slot; do not write them.

Details of the catalog and plan format: [catalog](references/catalog.md).

## Workflow

1. Read the repository: compose files, Dockerfiles, existing manifests, charts, README.
2. Author the files; build/ and manifests/ first, then plan the paths.
3. Preview offline, with no cluster: `sdo-operator deployment render --plan <scratch plan file>` renders what Go would
   deploy (put the scratch plan outside the checkout; plan.yaml is Go's). It prints `render.invalid`,
   `deployment.invalid` or `patch.*` as errors, and lists uncaptured app edits as not included.
4. Return the typed plan: image builds, manifest paths, expected workload IDs. Go writes `plan.yaml`, builds,
   applies and verifies independently, then reports back as correction feedback.

Rejections name the rule: `ownership.denied` (a path outside the deployer's grant, including any app file),
`patch.uncaptured` (an app file edit with no patch), `deployment.invalid` (plan problem). See
[rules](../sdo-memory/references/rules.md) for memory rules.

## Operability feedback

Go checks what you deployed against the operability items O1-O6 and words each failing check you own as one line,
`<check id> [<cause>]: <detail> (<paths>)`. Causes you correct: `patch` (a patch does not apply), `render`,
`render_nondeterministic` (two renders differ: remove clocks, random values, map-order output), `build` (image
declaration or build failure), `rollout` (a workload never became ready) and `immutable_change` (the API server
refuses an in-place update the deployer does not recreate on its own: a Service field such as `clusterIP`, or a Job
still running past the wait bound; keep the field stable or retry once the Job finished). Go already recreates
finished Jobs and workloads whose selector changed; `data_bearing_change` (a PersistentVolumeClaim) goes to a human.
`scope` and `allowlist` failures (an object outside the application namespace, a cluster-scoped kind, an unlisted
registry) are fixed in the files too.
Fix the named paths and return the plan again; never edit `plan.yaml`.
