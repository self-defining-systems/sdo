# Operability feedback

Before your commit is accepted, Go checks it against the operability items O3 (coverage and bound sources), O4
(health detectors clear and probes pass under synthetic traffic) and O5 (memory consistent). A failure comes back as
diagnostics with the rule `operability.<check id>` and a message `[<cause>] <detail>`. Causes you correct:
`coverage` (a map operation, component or edge has no probe), `binding`, `memory_invalid`, `detector_fault` (your
detector or probe fires on a healthy application) and `probe_unavailable` (a probe never produced a verdict). Fix
the named paths; do not loosen a probe or its persistence to clear it. A firing Go-installed generic detector is
the deployer's, not yours.
