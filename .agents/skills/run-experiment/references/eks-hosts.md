# The EKS host entry in `hosts.yaml`

A redacted skeleton of the root's `hosts.yaml` for an EKS vcluster host, from the root that ran the SREGym learning
plans and the release series (chelan4 `/mnt/data/shli/sdo-exp/lanes-root-eks/hosts.yaml`; read the live file, never
copy its `account` into notes). Field semantics: [docs/eks.md](../../../../benchmarks/lanes/docs/eks.md).

```yaml
hosts:
  aws-vc:
    local: true                     # steps run on the manager's machine
    provider: eks
    eks:
      region: us-west-2
      account: "<redacted>"         # refused when the credentials belong to another account
      lanes: vcluster
      host_cluster: sdo-host-vc1
      budget_per_hour: 12.0
      budget_total: 40              # whole root, from a fresh ledger; ask the user before raising it
      vcpu_quota: 200               # 8 per lane + 4 per system node, under the account quota
      max_lanes: 6                  # = the lanes in clusters
      idle_timeout: 2h
      # max_load5_per_core: 4       # manager-machine load gate (default 4)
    staging: /mnt/data/shli/lanes/eks-vc/staging
    kubeconfigs: /mnt/data/shli/lanes/eks-vc/kube
    evidence: /mnt/data/shli/lanes/eks-vc/evidence
    max_builds: 6
    clusters: [s1, s2, s3, g1, g2, g3]
    lane_labels: {s1: {row: hotel}, g1: {row: gitlab-v5}, g2: {row: gitlab-spare}}   # only when specs select by row
    env:                            # every step's environment
      PATH: "/mnt/data/shli/tools/bin:<go>/bin:/home/shli/.local/bin:/usr/local/bin:/usr/bin:/bin"
      AWS_SHARED_CREDENTIALS_FILE: /mnt/data/shli/.aws-sdo/credentials
      AWS_CONFIG_FILE: /mnt/data/shli/.aws-sdo/config
      AWS_PROFILE: sdo
      AWS_PAGER: ""
      UV_CACHE_DIR: /mnt/data/shli/cache/uv
      GOCACHE: /mnt/data/shli/cache/go-build
      GOMODCACHE: /mnt/data/shli/cache/gomod
      GOFLAGS: "-modcacherw -p=4"
      SDO_BINARY_CACHE: /mnt/data/shli/cache/sdo-operator-bins
      FUZZOPS_BIN: <fuzzops checkout>/bin/fuzzops   # release-series pool snapshots need it
      FUZZOPS_LANE_PROVIDER: vcluster
    workload_env:
      "benchmarks/sregym/lanes/run.sh workload":
        SREGYM_LANES_ENV: /mnt/data/shli/lanes/eks/sregym-env
        SREGYM_LANES_SEED_<application>: <onboarded application repository>   # one per app
        SREGYM_LANES_UP_ARGS: "--allow-stale-images"
        SREGYM_LANES_RUN_ARGS: "--credentials-hook <root>/inputs/sregym-eks/dummy-credentials-hook.sh"
        SREGYM_LANES_RUN_TIMEOUT: "1500"
        SREGYM_LANES_RESEED: "1"    # later rows on a lane reseed instead of reinstalling
        FASTLOOP_HOST_REDEPLOYS: "6"   # must equal <lock dir>/redeploy.capacity
        FASTLOOP_HOST_LOCK_DIR: /var/tmp/sdo-fastloop-eks/locks
        BENCH_IMAGE_TAGS: "Project=sdo"
hook_command:
  sregym: benchmarks/sregym/lanes/run.sh subject
  sdo: ./tp-hook.sh                 # release-series hooks checkout
checkouts:
  fuzzops:                          # workload pins, SREGym pins included
    "<full sha>": {aws-vc: /mnt/data/shli/lanes/eks/checkouts/sregym-<sha8>}
    "86e1c7c": {aws-vc: <fuzzops checkout>}
  hooks:                            # subject pins, per subject
    sregym: {"<full sha>": {aws-vc: /mnt/data/shli/lanes/eks/checkouts/sregym-<sha8>}}
    sdo: {"<sha8>": {aws-vc: <hooks checkout>}}
```

- `FASTLOOP_HOST_REDEPLOYS` differing from the recorded capacity fails every build with `host redeploy capacity is
  8, requested 18`. Change both only while no lane uses the lock dir.
- The credentials hook writes a placeholder agent Secret so the install can mount one.
- A lane label is how a plan or pool reaches one lane. Give a pool's selector label to exactly the lanes it may
  claim, and relabel spares (`row: gitlab-spare`), or a rep claims a fresh spare instead of the reset lane.
- A new pin needs its checkout (with `third_party/sregym` and a synced venv) on the manager host before
  `plan add`; the manager refuses a dirty checkout.
