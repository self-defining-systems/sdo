# Social Network source builds

`build.yaml` pins the three source builds from DeathStarBench's `socialNetwork`
subdirectory and their existing deployment settings. Initialize `apps/deathstarbench`
before using the declaration. See [scoped application image inputs](../BUILD_INPUTS.md)
for preparing a standalone source checkout before a fresh build and trusted rollout.

For a fresh Social Network seed, also install the tracing source correction:

```bash
cp -R apps/social_network/source-overlays/. /absolute/fresh-social-source/
```

Commit both overlays before building and rendering that same source checkout.
The overlay also bounds every MongoDB. mongod 4.4 ignores cgroup v2 limits and
sizes its WiredTiger cache from host RAM, so an unbounded instance grows until
its container is killed. `mongod.tpl` sets `cacheSizeGB: 0.25` for all six
`*-mongodb` Deployments and each chart's `values.yaml` gives them a 1Gi limit and
512Mi request, which holds the cache plus process overhead.
`tests/unit/apps/test_social_mongodb_memory.py` renders the chart with the overlay
and fails if any MongoDB lacks a cache bound. A seed already cut without the
overlay keeps its unbounded MongoDBs: cut a new seed commit that applies it, then
redeploy and re-capture the baseline on every lane, because a plain reset restores
the old captured baseline.

Stock `media-frontend` and `nginx-thrift` run an `alpine/git` init container that
clones DeathStarBench from GitHub on every pod start, so any rollout sits in `Init`
for 1.5-2 minutes and needs GitHub. The overlay deletes those init containers.
Each chart's `files/<volume>` is a symlink to the checkout's own directory, and
`templates/_baseFileConfigMaps.tpl` renders it into a `<service>-<volume>` ConfigMap
that `_baseNginxDeployment.tpl` mounts where the clone used to copy files. The
symlinks resolve only inside a full `socialNetwork` checkout, so apply the overlay
there. Rendered manifests carry the files (about 650 KB), and a seed already cut
without the overlay keeps cloning: cut a new seed commit, then redeploy and
re-capture the baseline on every lane.
`tests/unit/apps/test_social_source_files.py` fails if a rendered workload has an
init container or runs `git`, `curl` or `wget`, and checks the mounted files equal the source.

Stock SREGym pins Jaeger to `1.57`; the DeathStarBench chart's `latest` can select
an all-in-one release without the legacy agent on 5778. The overlay restores
that stock pin and enables the declared Zipkin receiver on 9411 with
`COLLECTOR_ZIPKIN_HOST_PORT=:9411`. Both Service ports remain checked. Do not
replace a historical seed's source or deployment proofs with these declarations.

Seed acceptance must build the **whole accepted diagnostics projection** with
`builder.BuildWorkspace` / `ValidateAndBuild`, then run the resulting controller
against the healthy namespace and its credential-free in-cluster prober:

```bash
sdo-controller --evaluate-once --accept-seed --namespace social-network \
  --prober-url http://127.0.0.1:<forwarded-prober-port>
```

`--accept-seed` preserves the complete findings JSON and exits unsuccessfully on
any finding or evaluation error. Reachability consumers require a prober; a
skipped reachability check cannot accept a seed. Run official health, traffic
scenarios, rollout provenance and rendered-lock checks as additional gates.
