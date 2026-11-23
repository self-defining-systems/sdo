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
      max_idle_with_waiting: 60m    # hard ceiling when a plan still has waiting work for the lane (default 60m)
      max_load5_per_core: 4         # load gate of the manager's machine (default 4; 0 switches it off)
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

- **Host setup (once).** The VPC, two public subnets (`<region>a`, `<region>b`), the cluster and node IAM roles and the node
  launch template (100 GiB gp3 root volume) are not made per lane. `lanes host adopt HOST` runs `eks-lane.sh host-setup`,
  which makes them as the CloudFormation stack `sdo-lanes-shared` (idempotent; a first `up` does it when it is missing, and
  concurrent first ups share one creation). The ids are recorded in `<root>/eks/shared-<region>.env`. `lanes host teardown
  HOST` (`host-teardown`) removes the stack; it refuses while a lane cluster exists. The stack costs nothing while idle.
- **Up.** The first build on a lane runs `cluster_up` (default `eks-lane.sh up`): a cluster `sdo-lane-<lane>` on the shared
  network and roles, made with direct `aws eks` calls: the control plane, then the managed node group (one zone) and the
  add-ons together (vpc-cni with the NetworkPolicy agent, coredns, kube-proxy, metrics-server, pod identity, EBS CSI), a
  service-account-token kubeconfig whose context is `kind-<lane>`, and the node agent (a containerd registry mirror to ECR
  plus the node-write seam). A failed up deletes its half-built cluster. `up` prints `EKS_STAGE` lines
  (`shared_ready`, `control_plane_active`, `cluster_and_nodegroup_created`, `nodes_ready`, ...) and writes them to
  `<kubeconfig>.stages`.
- **Storage.** The default StorageClass is `local-path` (node-local, binds when a pod is placed), as on a kind lane:
  SDO mounts the same ReadWriteOnce claims from several pods and relies on the claim to place them on its node, which an
  EBS volume (it attaches to one node and places nothing) does not do. `gp3` is installed but not the default.
- **Size.** From the workload's `footprint` verb (or the spec's `lane:`): two nodes of the cheapest instance type that
  holds half of it (one node under 4 cores), never under `min_node_vcpus`. Passed to the script as `EKS_INSTANCE_TYPE`,
  `EKS_NODES`.
- **Keep.** A lane's cluster stays between problems of a batch. It is deleted when no experiment can be placed on that lane
  (its host is usable and its labels satisfy the selector; work for another lane of the host does not keep it) and
  the lane has been free for `idle_timeout`, or at once by `lanes gc`. A plan node that is only waiting (queued, blocked on
  an edge such as p1 -> p2, or back for a retry) and could use the lane (its host and selector match) keeps it past
  `idle_timeout`, so a slow p1 retry does not cost p2 a new 19 minute cluster. `max_idle_with_waiting` (default 60m) is the
  ceiling: past it the lane is deleted and the alarm `idle-ceiling` is raised, so a plan stalled for ever does not hold
  clusters for ever. This holds for cluster and vcluster lanes alike. Nothing bills overnight unless a manager is
  stopped with lanes up: `lanes status` shows the running estimate, `lanes gc` and the manager clean up. `lanes lane release LANE` deletes one free lane's cluster on the next pass whatever waits for it.
- **Release, and why it does not overlap the next row.** A release asks the subject's idle, tears the operator down (its
  evidence sink makes a last pass), and checks the transcripts: measured on the real-agent
  batch it is 13 to 14 s of idle, 15 to 18 s of teardown and 1 s of transcripts (about 35 s) when nothing else holds it. The
  closing turn is not part of it: the run's settle already waits until the cell's own incident is closed and its closing work
  done, so by the release the agent has stopped. (The 10 to 18 minute releases of that batch were an idle gate waiting for a
  store baseline only an inject needs; a teardown's idle no longer does.) Starting the next row's build while the previous
  operator still runs was considered and left out: the next build reuses the lane's namespace, cluster and app copy, and a
  closing turn, a cleanup of leftover app edits or a source publication still talks to that cluster through the lane's
  proxy, so an overlap would let one row's agent write into the next row's app, and the teardown is also what captures
  the evidence of the finished row. Rows stay isolated; the lane gain from an overlap would be the 35 s above.
- **Down.** `cluster_down` (default `eks-lane.sh down`) deletes the lane's node group and cluster and the kubeconfig, never
  the shared stack. A cluster an earlier version made with eksctl is deleted with eksctl.
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

## The manager's machine

The clusters are in AWS, but every step of an eks host runs on the manager's machine (the harness, `go run`, kubectl,
aws, fastloop), so that machine's load is the load of the steps. Under a load of thousands (other tenants' workloads, kind
and containerd threads) kubectl, `go run` and the harness calls time out inside rows: rows fail `not-injected`, a series
driver loses its lane, and the timings of whole rows are not real. `eks.max_load5_per_core` (default
`DefaultEksMaxLoad5PerCore`, 4; `0` switches the gate off) gates the manager's machine, not the host's `max_load5`, which
is a load of the host's own cores and does not apply to a host whose nodes are elsewhere:

- While the machine's 5-minute load per core (the manager's probe) is above the limit no build and no run starts; releases,
  unholds and recovers still do (they undo what exists). Each waiting lane says why in `lanes status` and `bench.log`:
  `the manager's machine has load5 9.4/core over max_load5_per_core 4 (load5 150 on 16 cores)`, and the host line shows
  `[NO NEW BUILD OR RUN, ...]`. The host's `max_load5` in `lanes status --json` and the plan alarm `host-pressure` use the
  same limit times the machine's cores.
- A step that fails with a timeout (the reason says `timeout` or `timed out`) while the machine was over the limit at any
  probe since the step started is the machine's failure: a build does not count toward the lane's `max_build_failures`, and
  a first run records no run at all, so its item stays queued with its attempts and retries intact (its lane's cluster is made
  anew), like a step lost to an unreachable host. It is written off at most three times per item; the fourth counts. A
  run past its first position keeps counting (its sequence's memory is on that lane and cannot be rerun).
- `lanes preflight` runs the check `manager-machine` for every eks host: the machine's load5, cores and free memory, failed
  above the limit, so a plan is not added to a machine that cannot run it. When it fails, move the manager to a quiet
  machine rather than trusting timings from an overloaded one.

## What is skipped, by design

On EKS there is no host contention: the nodes are the lane's own and autoscale into the account. The host-safety gates of
[host-safety.md](host-safety.md) therefore do not run for an eks host, and the inventory refuses `reserve`, `lanes` and
`kernel_minimums` on one instead of ignoring them: no cpuset or core ledger, no memory ledger or hard limit, no kernel
minimums, no ip-link canary or hung-task gate, no PSI gates, no starvation gate. The crash-loop detector and the churn
budget (`max_builds`) still apply. Capacity is what the account allows: lanes, vCPUs and dollars; the one machine gate that remains is the load of the
manager's machine (above).

## Tests

`internal/bench/eks_test.go` runs the script against fake `aws` (including CloudFormation and `aws eks`), `eksctl`, `kubectl` and `docker`
(`internal/bench/testdata/fakeeks`) and a whole lane lifecycle through the manager with a fake clock for the ledger. No
test reaches AWS.

## Lanes as virtual clusters (`eks.lanes: vcluster`)

A cluster per lane takes about twenty minutes to make and bills a control plane for as long as it exists. With
`lanes: vcluster` a host keeps **one long-lived EKS host cluster** and each lane is a **vcluster** on it, made in seconds,
with nodes of its own that Karpenter provides.

```yaml
hosts:
  aws-west:
    local: true
    provider: eks
    eks:
      region: us-west-2
      account: "123456789012"
      lanes: vcluster              # a lane is a vcluster on the host cluster
      host_cluster: lanes-host-west  # default lanes-host-<host>
      system_instance: m6i.xlarge  # the node that runs Karpenter and the lanes' control planes (default)
      full_lanes: [full1]          # lanes that stay clusters of their own (see "Capabilities")
      budget_per_hour: 6.0
      vcpu_quota: 64
    clusters: [vc1, vc2, vc3, full1]
```

- **Host cluster.** `lanes host adopt HOST` makes it once (`eks-vcluster.sh host-up`: eksctl, a managed node group `system`
  that is tainted `sdo-role=system` and scales 0..2, Karpenter, an EC2NodeClass, local-path storage, the node agent and ECR
  mirror, the NodePort range opened to the manager) or attaches the one that exists, and records the date it began to bill.
  The node agent and the mirror run once on the host cluster and serve every lane. Adopting again changes nothing.
- **A lane** (`eks-vcluster.sh up`): a Karpenter NodePool `lane-<lane>` of the lane's nodes (sized from the workload's
  `footprint` like a cluster lane: two nodes of the cheapest type that holds half of it), labeled and tainted
  `sdo-lane=<lane>` and capped at the footprint. Low-priority pause pods ask for the nodes the moment the lane is made, so
  they are ready by the time its API is. A vcluster in namespace `<lane>` syncs only the lane's nodes, adds the lane's
  toleration to every pod it syncs, sees the host's `local-path` StorageClass and exposes its API as a NodePort of the
  host (kubeconfig context `kind-<lane>`, as everywhere). The lane's nodes are its own: no other lane's pods run on them.
  The vcluster's LimitRange gives a pod without one a CPU and memory request only: the chart's default `ephemeral-storage`
  request (3Gi per container) is removed, because the host scheduler counts requests, not use, and a 32-container app would
  fill a node's 89 GiB allocatable with a nearly empty disk.
- **Delete** (`down`): the vcluster, its namespace and the NodePool go; Karpenter terminates the nodes (the step waits for
  that, at most `EKS_DOWN_WAIT`, 240 s, so the cost line ends where the nodes end). The last lane of the host also scales the
  `system` group to 0: only the control plane ($0.10/h) bills until the next lane brings a system node back.
- **Admission and spend** are as for cluster lanes (`max_lanes`, `vcpu_quota`, `budget_per_hour`, `budget_total`), with the
  host cluster added: its control plane from the adopt date, and its system node (4 vCPUs, $0.192/h for the default) while
  a vcluster lane exists. A vcluster lane costs its nodes only. A lane is held until the host is adopted. `lanes status`
  prints `eks (vcluster lanes, host included): ...`. A free vcluster lane is kept 3 minutes (it is remade in seconds); a
  failed build or gate recreates the lane, which is cheap.
- **Capabilities.** A vcluster has no real `kube-system` and no node access: those are the host cluster's. Lanes are
  therefore labeled `kube-system=virtual, node-access=virtual, shared=true`, and the lanes in `full_lanes` (and every lane of
  a `lanes: cluster` host) `kube-system=real, node-access=real, shared=true`. A spec with no selector takes either kind. The
  fault matrices (the fuzzops eks fault matrix, the SREGym eks-compat list: kubelet, CoreDNS, `kube-proxy`, node faults)
  carry `selector: {node-access: real}`; only a full-cluster lane takes them, and `lanes add` refuses them, naming the
  labels every lane has, when the host has none. The labels are the generic lane labels (`lane_labels`); lanes does not
  know what they mean.
- **Steps** see `BENCH_LANE_VIRTUAL=1` on a vcluster lane, so an adapter that reads nodes or `kube-system` knows they are
  the host's. `BENCH_ENTRY_HOST` is an address of one of the lane's own nodes. `$BENCH_REGISTRY push` works as for a
  cluster lane; the pre-pull list is the host's, so every node of the host pulls it. `BENCH_PREPULL` names the platform
  script whose `prepull KUBECONFIG IMAGE...` adds images to that list (`eks-vcluster.sh` on a vcluster lane,
  `eks-lane.sh` otherwise): a workload registers the images it pushed, while it keeps building, so the nodes hold them
  at the first install (fastloop does so for SDO's controller and responder; a failure is logged, never fatal). The
  vcluster list keeps the newest 8 images, newest first, and takes its own short lock, not the host lock.
- **Scripts and tests.** `deploy/eks-vcluster.sh` (`host-up`, `host-scale`, `host-down`, `up`, `down`, `exists`, `prepull`,
  `nodes`) and `internal/bench/eks_vcluster_test.go`, which run it against fake aws, eksctl, kubectl, helm and vcluster
  with a fake clock for the ledger. Each `up` records stage seconds in `<kubeconfig>.stages`.

## A local kind host cluster (`eks.local_kind`)

`eks.local_kind: true` (with `lanes: vcluster`) makes the host cluster a **kind cluster on the manager's machine**, so a
plan rehearses the whole vcluster path for free before it runs on EKS: the same lane (a vcluster with nodes of its own,
`BENCH_LANE_VIRTUAL=1`, NetworkPolicy and StorageClass sync, `local-path` storage and node-bound claims), the same adapters
(`BENCH_LANE_PROVIDER=eks`) and the same plan. It needs no AWS account, credentials, `eksctl` or Karpenter (`kind`, `helm`,
`vcluster`, `kubectl` and `docker` on the PATH); the account is not asked for and the cost ledger is a fiction (set the
budget high).

```yaml
hosts:
  lvc:
    local: true
    provider: eks
    eks: {region: us-west-2, lanes: vcluster, local_kind: true, host_cluster: lvc-host, local_registry_port: 5611,
          budget_per_hour: 1000, vcpu_quota: 1000, max_lanes: 6}
    staging: /var/tmp/lanes/staging
    kubeconfigs: /var/tmp/lanes/kube
    evidence: /var/tmp/lanes/evidence
    clusters: [lv1, lv2, lv3]
    env: {EKS_LOCAL_WORKERS: "7", EKS_LOCAL_NODE_CPUS: "4", EKS_LOCAL_NODE_MEMORY: "14g"}   # optional
```

`lanes host adopt HOST` makes the kind cluster (one control plane, `EKS_LOCAL_WORKERS` workers, kindnet, which enforces
NetworkPolicy), labels the first worker as the system node (`sdo-role=system`, tainted as on EKS), installs `local-path` as the
default StorageClass and starts two registries on the docker `kind` network: `<host_cluster>-registry` (published on
`localhost:<local_registry_port>`; what the adapter pushes) and `<host_cluster>-hubcache` (a pull-through cache of
docker.io). Every node's containerd resolves each upstream to `<registry>/v2/sdo-mirror/<upstream>` first and falls back to the
upstream, and `localhost:<port>` to the registry, as the node agent does with ECR. A lane claims `EKS_NODES` free workers
(label and taint `sdo-lane=<lane>`, as Karpenter's NodePool does) and gives them back on `down`. `BENCH_IMAGE_REGISTRY` is
`localhost:<port>`; the adapter then pushes to it with plain `docker push` (`benchmarks.lanes.eks.Ecr` makes no AWS call
for a `localhost:` registry).

Not rehearsed: ECR, the node agent and its prepull list, Karpenter capacity and node start time, EBS, the VPC CNI policy
agent and the AWS network. Those are what only an EKS lane confirms.
