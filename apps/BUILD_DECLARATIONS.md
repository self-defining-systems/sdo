# Benchmark source builds

`hotel_reservation/build.yaml`, `social_network/build.yaml`, and
`astronomy_shop/build.yaml` are authoritative source-build declarations for the
opt-in SREGym generic adapter. Production SDO does not import the adapter.
The source and deployment inputs record immutable Git revisions independently.
Existing per-app adapters remain the default.

Initialize the source submodules, then configure the harness:

```bash
export SREGYM_APP_BUILD_DECLARATION=/absolute/sdo/apps/hotel_reservation/build.yaml
export SREGYM_DEPLOY_FROM_SOURCE=1
export SREGYM_SOURCE_BUILD_CACHE=1
export SREGYM_SOURCE_BUILD_JOBS=1
export SREGYM_DOWNLOAD_CACHE=/absolute/shared/download-cache
```

The declaration's `source.path` is relative to its directory. A prepared agent
workspace is selected with the existing `SREGYM_APP_SOURCE_DIR`. Images declare
contexts and Dockerfiles relative to that source root; no Dockerfile is selected
by discovery. Image references use the existing app/cluster tag plus an optional
component suffix. The deployment source is either relative to the source or to
SREGym-applications (`root: harness`). Namespace mismatches fail before builds.

The generic adapter builds all declared images and loads them into the selected
kind cluster. Helm uses the existing application deployment settings and applies
only the declared image overrides and chart path. Hotel uses a temporary
Kustomize overlay of its existing manifests. This does not change fault mapping,
workloads, or health/mitigation judgments.

Explicit archive downloads are pinned by URL and SHA-256. Social's declaration
redirects the historical Stanford PCRE archive to the verified Exim mirror.
The adapter verifies shared cache entries and copies them into a named BuildKit
context, substituting the matching curl/wget commands in a temporary Dockerfile.
A populated cache works when the origin is offline; a cold unavailable origin
reports the URL, expected checksum, and cache location. Package managers, Git
clones and base-image pulls retain their normal BuildKit caches. This mechanism
requires a BuildKit-capable Docker builder; `SREGYM_DOCKER_BUILDER` selects one.

Set `SREGYM_SOURCE_BUILD_JOBS` from 1 to 4 (default 1). The adapter further reduces
parallelism according to the host's current load. This bounds concurrent Docker
builds; upstream Make/Gradle/package managers can have their own worker settings.

Astronomy source and chart initialization depends on AS1. Keep its fresh-workspace
.NET advisory preparation when using the pinned historical demo. The generic
adapter preserves AS1's twenty images, component suffixes, Dockerfiles and build
arguments. Its `llm` component is a local mock and needs no live LLM calls.
Social's optional ARM Dockerfile matches the existing adapter's selection; the
pinned DeathStarBench input does not include that file and fails clearly before
building on ARM. Platform support is unchanged.

For adding another app: pin its source and chart/manifests, write a declaration,
run the declaration/adapter contracts and one source-build smoke on an idle lane.
The existing application deployment integration remains responsible for its
application metadata and workload hooks. Adapter migration/deletion follows the
stream run separately.
