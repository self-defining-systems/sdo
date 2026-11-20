# EKS hosts (provider `eks`)

A host with `provider: eks` makes each lane an Amazon EKS cluster. Lanes stays a resource and dispatch layer: the cluster
operations live in `deploy/eks-lane.sh` (eksctl, aws, kubectl, docker on the manager's PATH), the sizing, admission, cost
and health in `internal/bench/eks.go`. What runs on a lane (the app, its images, grading, the subject) stays in the
workload and subject adapters, which see `BENCH_LANE_PROVIDER=eks` and `BENCH_EVIDENCE_MODE=pull`.

```yaml
hosts:
  aws-west:
    local: true                     # steps run on the manager's machine with the lane's kubeconfig; no ssh
    provider: eks
    eks:
      region: us-west-2
      account: "123456789012"       # optional: refused when the credentials belong to another account
      budget_per_hour: 6.0          # cap on the estimated $/h of all clusters (control planes and nodes)
      budget_total: 60              # optional cap on the estimated spend of the host
      vcpu_quota: 64                # vCPUs of on-demand standard instances lanes may hold at once
      max_lanes: 4                  # clusters at once (default: the number of `clusters`)
      instance_families: [m6i]      # cheapest family that fits wins; known: m5 m6i m7i c5 c6i c7i r5 r6i r7i
      min_node_vcpus: 4             # pod density: a 2 vCPU node holds too few pods
      idle_timeout: 10m             # a free lane's cluster is kept this long when no experiment wants the host
      vpc: vpc-...                  # optional; unset, eksctl makes a VPC
      subnets: [subnet-..., subnet-...]
    staging: /mnt/data/lanes/staging
    kubeconfigs: /mnt/data/lanes/kube
    evidence: /mnt/data/lanes/evidence    # evidence is pulled here, not mounted on nodes
    max_builds: 2
    clusters: [eks1, eks2, eks3, eks4]    # lane names; the clusters exist only while a lane needs them
```

Variables a step on an eks lane sees: `BENCH_LANE_PROVIDER=eks`, `BENCH_EVIDENCE_MODE=pull`,
`BENCH_IMAGE_REGISTRY` (the account's ECR host), `AWS_REGION`, `KUBECONFIG` (token kubeconfig, context `kind-<lane>`), and once the
cluster is up `BENCH_ENTRY_HOST`: an address of a node the manager reaches on NodePorts (an adapter maps it to its own
name). A host's `env:` adds whatever a workload's tooling still needs, for example the lane provider name of a binary that
predates eks.

## Lifecycle

- **Up.** The first build on a lane runs `cluster_up` (default `eks-lane.sh up`): a cluster `sdo-lane-<lane>` with the
  add-ons a kind lane has (vpc-cni with the NetworkPolicy agent, coredns, kube-proxy, metrics-server, pod identity), one
  managed node group, a service-account-token kubeconfig whose context is `kind-<lane>`, and the node agent (a
  containerd registry mirror to ECR plus the node-write seam). A failed up deletes its half-built cluster.
- **Size.** From the workload's `footprint` verb (or the spec's `lane:`): two nodes of the cheapest instance type that
  holds half of it (one node under 4 cores), never under `min_node_vcpus`. Passed to the script as `EKS_INSTANCE_TYPE`,
  `EKS_NODES`.
- **Keep.** A lane's cluster stays between problems of a batch. It is deleted when no experiment can build on the host and
  the lane has been free for `idle_timeout`, or at once by `lanes gc`. Nothing bills overnight unless a manager is
  stopped with lanes up: `lanes status` shows the running estimate, `lanes gc` and the manager clean up.
- **Down.** `cluster_down` (default `eks-lane.sh down`) deletes the cluster and the kubeconfig.
- **Images.** `$BENCH_REGISTRY push IMAGE...` (`eks-lane.sh push`) publishes a local image under its declared name to
  ECR (`sdo-mirror/<upstream>/<path>:<tag>`); nodes resolve the declared name through the mirror and fall back to the
  upstream. `eks-lane.sh pin IMAGE` pushes an immutable image and prints its `@sha256` reference.

## Admission, cost, health

- A build that needs a new cluster waits while the host has `max_lanes` clusters, the new lane would pass `vcpu_quota` or
  `budget_per_hour`, or the estimated spend plus one hour of it would pass `budget_total` (the reason is in
  `bench.log`). A lane that already has its cluster is never held.
- `<root>/eks/<host>.json` is the cost ledger: clusters that exist (open since, instance type, nodes, $/h) and the cost of
  the deleted ones. `lanes status` prints `eks: 2/4 lanes, 16/64 vCPU, $0.97/h of $6.00/h, spent ~$1.20`. Prices are
  on-demand per-vCPU rates in `eks.go` plus $0.10/h per control plane: an estimate, not the bill.
- Health is per lane: the API answers and a node is Ready (bounded `kubectl get nodes`, once per pass). A lane that fails
  gets no new steps (alarm `eks-lane-unhealthy`); nothing is deleted. The alarm `eks-over-budget` fires if the ledger
  passes the limits.

## What is skipped, by design

On EKS there is no host contention: the nodes are the lane's own and autoscale into the account. The host-safety gates of
[host-safety.md](host-safety.md) therefore do not run for an eks host, and the inventory refuses `reserve`, `lanes` and
`kernel_minimums` on one instead of ignoring them: no cpuset or core ledger, no memory ledger or hard limit, no kernel
minimums, no ip-link canary or hung-task gate, no PSI gates, no starvation gate. The crash-loop detector and the churn
budget (`max_builds`) still apply. Capacity is what the account allows: lanes, vCPUs and dollars.

## Tests

`internal/bench/eks_test.go` runs the script against fake `aws`, `eksctl`, `kubectl` and `docker`
(`internal/bench/testdata/fakeeks`) and a whole lane lifecycle through the manager with a fake clock for the ledger. No
test reaches AWS.
