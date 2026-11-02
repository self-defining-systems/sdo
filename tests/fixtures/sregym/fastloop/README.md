# Fastloop test streams

These portable snapshots exercise stream parsing, latent fault coverage, toy
contracts and the hermetic controller gate. They are test fixtures, not supported
experiment presets. Pass a stream explicitly when running the benchmark CLI.

## Permissive sdo3 selection

`sdo3_permissive.json` preserves ST1's first ten slots and explicitly enables
permissive capability validation and broad responder permissions in the app and
`policy-system` namespaces. It is a selection/configuration fixture, not a mixed
app dispatcher or a launch-ready lane. Dispatch and app-specific seeds are
separate work. Secret API reads remain denied; rotate stale environment
credentials by restarting workloads. No attributable probe is required.
