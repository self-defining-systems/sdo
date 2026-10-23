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
