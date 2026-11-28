---
name: sdo-serving
description: How SDO deploys an application from its repository - what Go derives from the files, the image rule, the optional override .sdo/deployment/plan.yaml, failing deploy steps.
---

# Serving an application from its repository

The deployment is the repository's files. Go deploys a commit: it takes the commit's tree without `.sdo/`,
derives the renderer and the image builds from the files, builds the images with BuildKit and pushes them to the
trusted registry, applies the render and waits for every rendered workload. Uncommitted files are never deployed.
The same tree serves every render, build, drift read and repair.

## What Go reads from the files

- Renderer: one Helm chart root (a `Chart.yaml` with no chart above it; vendored subcharts belong to it) is helm;
  else one kustomization that no other includes (components never count) is kustomize; else every file whose
  documents are all Kubernetes objects is raw. Several roots, a chart beside a kustomization, or Helm value files
  other than the chart's `values.yaml` need the override.
- Images: an image under the trusted registry (a host, possibly with a path such as `host/team/app`) named
  `<registry>/.../N[:tag]` is built from `Dockerfile.N` or
  `N.Dockerfile` (context: that file's directory), from a directory `N/` holding a `Dockerfile` (context: `N/`), or
  from the tree's only Dockerfile. Any other build is an `images` entry in the override. Every other image is
  `prebuilt`: Go pins its tag to a digest.

## Images and infrastructure

- Third-party images use the `prebuilt` source; the operator pins tags to digests. Never write digests.
- Run the databases, caches and queues the source is written for: its engine, and its version where the source
  pins one. Use another engine only when the goal says `Infrastructure: compatible`, and name the swap.
- Builds are cached by layer, so a later deploy of the same tree to another environment is fast.

## The override `.sdo/deployment/plan.yaml`

Optional, and the only file under `.sdo/deployment/`. It names only what the files cannot say: `source` (renderer
and config) and `images` ([catalog](references/catalog.md)). Without it, Go derives both.

## Where a change goes

1. An app with no deployment files: commit Dockerfiles and a kustomization (or a chart, or plain manifests) in the
   repository, for example under `deploy/`.
2. An app that ships its deployment: add only what is missing (a values file, a kustomize overlay, a Dockerfile),
   and prefer layering to editing upstream files. Vendored Helm subcharts are changed through values only.
3. Application code that must change (instrumentation, a bug that stops it running): commit the source change.
   A responder commits source in its incident checkout; SDO publishes it after the incident closes.
4. Never commit Secret values. Reference Secrets by name; generate test credentials at deploy time. What a commit
   may hold: [committed source rules](references/rules.md).

## Reading a failed deploy

SDO's operability report names each failing check as `<check id> [<cause>]: <detail> (<paths>)`:

- `render`: a manifest or chart does not render, the tree has no deployment files, or it has several roots.
- `build`: an image declaration or a build step failed. The detail carries the failing step and its log tail.
- `rollout` on `O1.entry`: the workloads are ready but a Service users enter through does not answer (no
  connection, or `GET /` returns 5xx). Compare the Service with the Pods it should reach, and read their logs.
- `rollout`: a workload never became ready. Read its events, its Pods' status and the previous container's logs
  (`kubectl logs --previous`). One that only ran out of time is not a file error.
- `immutable_change`: the API server refuses an in-place update (a Service `clusterIP`, a running Job). Keep the
  field stable or wait for the Job. Go recreates finished Jobs and workloads whose selector changed.
- `data_bearing_change`: a PersistentVolumeClaim would change. A person decides.
- `scope` and `allowlist`: an object outside the application namespace, a cluster-scoped kind or an unlisted
  registry. Fix the files.

Check your last commit the way Go will, without a cluster, in seconds:
`sdo-operator operability check --offline --registry-host <trusted registry>` (render, image rule, scope).
Preview the render itself: `sdo-operator deployment render`.
