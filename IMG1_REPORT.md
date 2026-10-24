# IMG1: fast image delivery

Measured on this host on 2026-10-24 UTC, without model calls. Source-only work now
uses mounted Python and `.fastloop/sdo-operator`; image changes use a lane-owned
registry and retained layers. The corrected cold delivery took **45.63 s**.

## Before / after wall time

| Operation | Before, seconds | After, seconds |
| --- | ---: | ---: |
| First full image delivery to a fresh toy lane | 100.94 | 45.63 |
| Refresh after one-line Go comment, using dev mount | 16.61 | 16.85 |
| Refresh after one-line Python comment, using dev mount | 16.08 | 14.13 |
| `fastloop reset --dev-mount` | 17.92 | 14.87 |
| Full first toy up, including cluster/CNI/controller | 174.46 | 118.32 |

Each is one observation, not a distribution. The first delivery includes the toy
app, controller, scripted worker, validator, and three Calico images on one fresh
control-plane node. It excludes image builds and includes pushes, pulls and
unpacking in the registry case. Refreshes use reset, rebuild the host operator,
restore the application/seed and wait for a healthy controller. The final three
refreshes left `image_delivery.jsonl` byte-for-byte unchanged: zero image work.
The baseline already supported production Python/Go mounts; its missing live
benchmark/recording and reflection coverage was a correctness gap, not a claim
that all old dev-mount edits needed rebuilding. Go refresh latency is unchanged
within the variation of these single observations.

Supplement: pushing/pulling the unchanged three SDO images again through the warm
registry took **1.36 s**. This is an unchanged-image check, not a changed-layer
benchmark. Fresh registry delivery is 55% faster than the measured archive path.

## Image sizes

Exact `docker image inspect .Size` content bytes, reported as decimal MB below.
This containerd-backed Docker store reports compressed image content here;
`docker image ls` also includes materialized snapshots and can display several
GB for the same image. The content metric is consistent for all rows, including
images whose old shared-daemon snapshot cannot be unpacked. Shared bytes are
counted in each image, so these rows must not be summed as unique disk usage.

| Image | Before MB | After MB | Reduction |
| --- | ---: | ---: | ---: |
| Controller | 887.7 | 689.6 | 22% |
| Production responder | 1,037.6 | 443.5 | 57% |
| Validator | 834.3 | 115.3 | 86% |
| Scripted benchmark worker | 1,045.2 | 138.5 | 87% |
| Live benchmark worker | 1,045.3 | 447.1 | 57% |

Before tags: `img1-before` and `img1-before-live`. Final after tags:
`img1-final-code` and `img1-final-live`. Delivery timings used `img1-after-clean`
with mounted source; the final image rebuild incorporates the subsequent init
admission-default fix. It changes the above rounded sizes by less than 0.1 MB.

Only the controller image contains Go, module downloads and the build-cache seed.
Controller, validator, responder and scripted images share the same pinned
Debian slim OS/tool stage. Python dependencies are shared by both worker variants;
Node and model CLIs occur only in model workers. Source and binaries occupy late
layers. No image was flattened. The separate validator Dockerfile and node
archive-preparation helper were retired in favor of the shared Bake graph and
registry delivery.

## Enforcement and decisions

- Up defaults to this worktree's dev mount. Bare `--dev-mount` works for up and
  reset. SREGym can opt out with `--no-dev-mount`, which warns; toy requires the
  mount. Warm code changes need no image delivery. Toy names and image tags now
  follow the chosen lane prefix instead of using a global cluster/image tag.
- Up runs read-only tool, Docker, disk, neighbor-capacity, worktree and image
  checks before any compiler or image build. The standalone pre-build command
  is `uv run python -m benchmarks.sregym.fastloop preflight`. Live launch policy
  checks remain separate. No kernel settings were changed.
- Each lane gets `<cluster>-registry`, `registry:3`, an ownership label and an
  automatically allocated IPv4 loopback port. Containerd hosts files route to
  the registry on the kind network. Existing unowned containers are refused.
  The external worker's SDO, Calico and source-app deliveries use the same edge.
- Archive loading is only an automatic fallback. It warns immediately and again
  with the registry error, affected images and elapsed fallback seconds. The
  lane and every incident row carry `image_delivery` and
  `image_delivery_reason`; run repeats the warning. A successful partial
  delivery cannot conceal another image's fallback. Archive save/load are
  bounded at 180 s each.
- **Decision to review:** keeping validation intact requires Go in worker and
  validator pods even though their images are compiler-free. A trusted init
  container uses the controller image to copy Go/caches into a private emptyDir;
  consumers mount it read-only and write compilation output to scratch. The init
  container has no candidate or provider-credential mounts. This adds roughly
  10–16 s of startup copy time per observed worker, which is outside image
  delivery and warm-reset timing. No compile/vet/replay or broker gate was removed.
- **Decision to review:** the common runtime base was updated to a newly pinned
  Debian bookworm slim digest. Old Node-base snapshots on the shared daemon were
  broken. A private `img1-builder` avoided the default builder's broken snapshot;
  the new runtime base avoided its broken runtime ancestor. Shared Docker storage
  was neither repaired nor pruned. Node is copied only into model-worker stages.
- Legacy live `benchmarks.sregym.run` launches require `--allow-non-fastloop`.
  The procedure/report changes are in `run-experiment`; tier changes are in
  `test-sdo`, on PR #201's `7cc9e2839` base. Retarget to main after that dependency
  merges, unless it has merged before this PR is opened.

## Validation evidence and detours

Fail-first regressions cover mount defaults, preflight ordering/capacity, lane
prefixes, automatic fallback, stderr warnings, lane metadata, failed/successful
incident rows, run-start warnings, registry ownership and endpoint configuration,
external delivery hooks, strict init admission defaults, compiler-free image
ancestry, shared stages and absence of flatten/archive-preparation script paths.

Required gates: formatting, `scripts/check_errors.sh`, focused Python/Go tests,
and `scripts/test_fastloop_hermetic.sh`. The broad focused Python run passed 274
tests; assurance/runner tests passed 333; the full Go runtime module passed.
The image build script's self-checks passed for all four runtime/scripted images
and the separate live benchmark image. The changed skills pass command and
field checks plus the maintain-skills lint. Tests were individually bounded at
five minutes.

The first real registry attempt discovered that adding a Docker network can
change the dynamically published port. It produced the requested loud fallback
warnings and completed archive loading in **71.84 s**. The registry now reads
its port after joining the kind network, uses IPv4 explicitly, waits for health,
and has a regression simulating the port change. A fresh lane then delivered
all images through the registry in 45.63 s.

The scripted incident exposed missing termination-message defaults on the new
compiler init container. A server dry run isolated the two fields; they are now
explicitly pinned and covered by a failing-then-passing regression, preserving
the strict validator admission check. A subsequent aggressively scaled smoke
hit a pre-existing observer deadline of 0.020 s under concurrent build/test load;
the final smoke uses time-scale 1 and a 600 s responder budget. It passed:
`img1-scripted-final`, oracle true, error null, 217.9 s total incident wall time,
154.0 s reflection/validation drain, and zero responder/reflection tokens. The
controller recorded isolated compile, corpus replay, registry rebuild and restart.
The actual incident row retains `image_delivery: kind-load-fallback` and its
original registry error, confirming that mode is surfaced beyond unit tests.

Evidence is under `/mnt/data/shli/sdo-exp/mixed10/`:

- `img1-before/toy_timings.json`, `img1-before-refresh.json`,
  `img1-before-reset.log`;
- `img1-final/toy_timings.json`, `img1-final/refresh_timings.json`,
  `img1-final/image_delivery.jsonl`, `img1-warm-delivery/image_delivery.jsonl`;
- `img1-*-build.log`, `img1-python-precommit.log`, `img1-go-final.log`,
  `img1-runtime-tests.log`, `img1-runner-tests.log`, `img1-hermetic-last.log`;
- `img1-scripted-final.log` and
  `img1-after/results/img1-scripted-final/` for the real no-model incident.

The baseline was archived from fetched `origin/main` at `a5c4b73bf`. Its only
measurement harness adaptations were its own cluster name/image tag and
`docker build --load` for the private builder. Nested SREGym modules were
initialized from inside its checkout; pin
`2e2752dac94cf9a82fc4e6154883db8ccabc4478` is unchanged.
Only `img1-*` clusters/registries, task-owned new image tags and the task worktree were used. Lane 52,
`luna-pilot-*`, `rs-*` and `rs-regcache-*` resources were never modified. No model
was called and no Secret data was read.
