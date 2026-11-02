# Scoped application image inputs

The astronomy and social `build.yaml` declarations are split from SDO PR #171
(GA1); their source pins, contexts, Dockerfiles, build arguments, downloads and
deployment settings are unchanged. They can be used by GA1's opt-in generic
benchmark builder. This split does not enable that adapter or change its pin.

Each app's `build-inputs/` directory contains conventional Dockerfile-specific
`.dockerignore` files. Install them in a **fresh standalone source checkout**
(the source root named by `source.path`, not this repository's root) **before**
building images or recording deployment evidence:

```bash
cp -R apps/astronomy_shop/build-inputs/. /absolute/fresh-astronomy-source/
# Or, for the standalone socialNetwork source root:
cp -R apps/social_network/build-inputs/. /absolute/fresh-social-source/
```

Keep those files in the source repository and commit them with the source used
for the fresh build. The same prepared source must be given to the builder and
to SDO; copying scope files only into the evidence checkout is insufficient.
There is no source-submodule edit or implicit upgrade of an existing seed.

Astronomy's twenty Dockerfiles require root-relative `COPY` paths, so their
contexts remain `.`. Each ignore file admits its component's `src/` subtree;
accounting, ad, cart, currency, fraud-detection and payment also admit shared
`pb/` inputs. Relevant upstream exclusions remain in force. Dockerfiles and
ignore files themselves are always identity inputs under LK2, even when the
context excludes them. Source repairs that add new build dependencies must
also update the relevant ignore file and rebuild that image.

Social retains its three builds: the root microservices image (shared by eleven
services), `docker/openresty-thrift`, and `docker/media-frontend`. The root image
preserves the pinned source whitelist, so conventional deployment files are
excluded. The other contexts retain their source files and exclude operational
and deployment files. The optional ARM Dockerfile keeps its existing declaration;
this change does not supply the missing upstream ARM Dockerfile.

## Trusted deployment evidence

Use each declaration's `context` and `dockerfile` as repository-relative
`ImageBuild.context_path` and `ImageBuild.dockerfile_path` when preparing the
native raw deployment plan. Its `image` must match the corresponding source
manifest reference, including the selected tag and component suffix. The trusted
Go strategy retains these paths as `DeployedImage.inputs` after building and
independently verifying the rollout. External regular and init images explicitly
record `source_image` with no local paths. A scripted trusted deployer must retain
the same metadata alongside its independently observed named image identities;
reading a declaration alone is not proof of a build or rollout.

For the next astronomy rehearsal, create a new seed from those freshly built
images, with all 37 named image entries scoped (20 local builds plus explicit
external regular/init references across 26 workloads). Never patch the old
broker ledger or infer scope from live Pods. Legacy missing inputs still use
all-source comparison and retire all 26 workloads after a source edit.

The synthetic Go lock regression measures 26 gaps before these declarations,
zero after a manifest-only repair, one after a shipping source/Dockerfile/ignore
edit, and six after a shared protobuf edit. It uses the checked-in manifest
shape and fabricated image digests, without a cluster. L6 must independently
re-measure a fresh verified seed; no live gap improvement is claimed here.
