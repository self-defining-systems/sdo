---
name: sdo-serving
description: How SDO builds and deploys an application from its repository. Covers .sdo/deployment, the plan, images and registry, source changes and patches, and failed builds or rollouts.
---

# Serving an application from its repository

Go deploys the **effective tree**: the repository at a commit, with the patch series in
`.sdo/deployment/patches/` applied and `.sdo/` memory projected away. It renders the plan's manifests, builds
the plan's images with BuildKit, pushes them to the trusted registry, applies the objects and waits for the
expected workloads. The same tree serves every render, build, drift read and repair.

## Layout of `.sdo/deployment/`

| Path | Holds |
|---|---|
| `plan.yaml` | what is deployed: renderer, images, expected workloads ([catalog](references/catalog.md)) |
| `manifests/**` | authored Kubernetes manifests (an app with no manifests, or one that lacks some config) |
| `build/**` | authored Dockerfiles and `.dockerignore` |
| `values/**` | inputs for renderers that read values (Helm value files, kustomize patches) |
| `overlays/telemetry/**` | telemetry backends SDO applies with the application |
| `patches/NNNN-<slug>.patch` | reasoned changes to application files ([patches](references/patch-rules.md)) |

Any other path under `.sdo/deployment/` is rejected. Plan paths address the effective tree: a manifest reads
`.sdo/deployment/manifests/app.yaml`, an image `context_path` is the upstream directory and its
`dockerfile_path` may be `.sdo/deployment/build/api/Dockerfile`.

## Where a change goes

1. Missing deployment inputs (a manifest, a Dockerfile the repository lacks, config values): an authored file
   under `manifests/`, `build/` or `values/`. This is the default.
2. Existing manifests or a chart used as they are, with differences in `values/` or a kustomize layer.
3. Application code that must change (instrumentation, a bug that stops it running): change the source.
   - The onboarding agent commits the change directly in its repository.
   - A responder commits source in its incident checkout; SDO publishes it after the incident closes. Never
     edit a file a deployment patch changes; add a later patch instead.
4. Never commit Secret values. Reference Secrets by name; generate test credentials at deploy time.

## Images and registry

- Every built image's name starts with the trusted registry host; Go pushes there and pins what it deployed.
- Third-party images use the `prebuilt` source; the operator pins tags to digests. Never write digests.
- Builds are cached by layer, so a later deploy of the same tree to another environment is fast.

## Reading a failed deploy

`sdo deploy` (and SDO's operability report) names each failing check as
`<check id> [<cause>]: <detail> (<paths>)`:

- `patch`: a patch no longer applies. Re-create it against the current files.
- `render`: a manifest or chart does not render; `render_nondeterministic`: two renders differ (remove clocks,
  random values and map-order output).
- `build`: an image declaration or a build step failed. The detail carries the failing step and its log tail;
  reproduce it locally with `docker build` on the same context before you change the plan.
- `rollout`: a workload never became ready. Read its events, its Pods' status and the previous container's
  logs (`kubectl logs --previous`) in the environment the report names.
- `immutable_change`: the API server refuses an in-place update (a Service `clusterIP`, a running Job). Keep the
  field stable or wait for the Job. Go recreates finished Jobs and workloads whose selector changed.
- `data_bearing_change`: a PersistentVolumeClaim would change. A person decides.
- `scope` and `allowlist`: an object outside the application namespace, a cluster-scoped kind or an unlisted
  registry. Fix the files.

Preview a render without a cluster: `sdo-operator deployment render --plan FILE`.
