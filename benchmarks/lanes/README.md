# lanes

The lane platform: a long-running manager that turns experiment specs into runs on isolated Kubernetes lanes (kind
clusters on hosts, vcluster, or kind in a VM). It owns hosts, admission, CPU and memory isolation, warm pools, image
delivery, detached steps that survive a manager restart, retries and results. It knows nothing about any one benchmark:
a benchmark plugs in as a *workload* adapter (the app, injection and grading) and a *subject* adapter (the operator
under test). See [docs/contract.md](docs/contract.md).

Design: the lane-platform note in the experiment directory (`campaign-20261118/lane-platform/DESIGN.md`).

## Origin

Plain copy (no history) of the bench manager of fuzzops `origin/main` at commit
`86e1c7c` (`fuzzops-bench`): `internal/bench`, `internal/lanelock`, `internal/env`, `cmd/fuzzops-bench` (now
`cmd/lanes`), and the lane scripts of `deploy/` (kind-lane, lane-limits, lane-docker-shim, lane-kubeconfig,
vcluster-lane, vcluster-lib, node-preload, registry, shared-registry, ensure-images, load-images, lib.sh, `deploy/host/`)
with their Go tests. Two small helpers it used from other fuzzops packages are inlined (`internal/miniyaml`: the JSON
and YAML-subset decoder of `internal/apppack`; the file-name sanitizer of `internal/k8sfault`).

## Compatibility

Verbs, flags, `hosts.yaml`, spec files and the on-disk root layout are unchanged, so a running fuzzops-bench root
can be served by `lanes` with no migration (`internal/bench/compat_test.go` loads a root and spec fixture). Two names
stay as they were because hosts have them installed: the remote binary path `bin/fuzzops-bench` in each pinned
checkout, and the ledger directories `/var/tmp/fuzzops-lanes-<uid>`.

Not yet moved from fuzzops: the manual (`docs/bench.md`), which still describes these verbs, and `internal/benchview`.

## Build and test

```bash
source /mnt/data/shli/cache/env.sh        # NVMe Go caches on the lab hosts
cd benchmarks/lanes
go build -o bin/lanes ./cmd/lanes
go test -tags=integration ./...                             # fake kind, kubectl and fuzzops under internal/bench/testdata
```

The tests carry the `integration` build tag. They run real processes (the manager, detached steps, shell scripts against stub docker/kind/kubectl) and take
about two minutes; CI runs them with the rest of the Go modules (`scripts/test_go_ci.sh`).
