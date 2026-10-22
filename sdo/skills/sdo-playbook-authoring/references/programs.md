# Direct program authoring

1. Create the final program directory with `sdo-operator program scaffold --fault-class <id> --param name=Kind [--mitigate] --out .sdo/playbooks/<id>`. Keep responder ownership and incident provenance in README front matter and register the logical ID in the playbook index.
2. Implement `diagnose.go`, optional `mitigate.go`, and Go tests against `sdo.dev/operator/sdk/program`. Use bound resources and the ReadView for names, ports and images. Shared helpers live in the diagnostics module's `lib/` packages. Diagnose only reads; mitigation returns a typed plan. Consult the installed playbook-program example for the current SDK.
3. Copy the selected controller open capture, fixed capture and phase-specific primitive recordings byte-for-byte from the read-only incident capture mount into a corpus directory as `open.json`, `fixed.json` and `recordings.json`. Use the fixed bytes for `healthy.json`. Choose the earliest pre-mitigation phase (`open@detect`, `open@settle` or `open@pre-apply`) whose capture shows the cause; detect time is not required, and the validator accepts any of them.
4. Author `labels.json` with `root_cause_program_ids`, `bindings`, `peer_bindings`, and `provenance` (`incident_id`, `open_at`, `open_sha256`, `fixed_sha256`). Bind a healthy same-kind peer from the fixed snapshot. Synthetic tightening cases use `no_match_program_ids` and `synthetic_no_match_reason`; they cannot replace captured positive evidence for a new program.
5. Keep README prose to judgement and role placeholders. Do not persist shell commands or physical memory paths in authored content. Format changed Go files with gofmt.
6. Run `sdo-operator memory check` and fix diagnostics in the same checkout until green. It checks source/import/library/README/corpus policy, provenance, compilation, vet, ordinary tests and generated open/fixed/healthy, rename and mitigation replay tests. Return a decision and changed logical playbook IDs; the broker independently checks the same final bytes.

There is no `program propose` command or scratch projection. A failed local check never authorizes publication.

## Keep source-restoration mitigators

Keep `mitigate.go` for image, readiness and ConfigMap restoration from accepted
source. Never delete a mitigator merely because its effect cannot be simulated.
The shared validator first validates its plan. A plan containing any effect other
than `SetField` is accepted with an informational `skipped_unreplayable_effect`
note; no post-plan effect is replayed for that plan. `SetField`-only plans still
must repair the open fixture and return NoMatch on the projected state.

Diagnosis replay stays mandatory: open Match, fixed/healthy-peer NoMatch,
generalization and rename invariance all still run. Missing diagnosis primitives
remain Unknown; a mitigation skip cannot supply missing diagnosis evidence.

This minimal source-restoration pattern uses a source contradiction already
established by the diagnosis. Narrow Match to the observed mechanism, such as a
source image difference plus a selected Pod's ImagePullBackOff, or an incorrect
readiness probe. Source/baseline differences used in diagnosis need recorded
primitive answers in every required corpus phase.

```go
func (p Program) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
    verdict, err := p.Diagnose(ctx, v, b)
    if err != nil || verdict.Result != program.Match { return program.Plan{}, err }
    return program.Plan{Actions: []program.Action{
        program.ReconcileFromSource{Ref: b["workload"]},
    }}, nil
}
```

Skipped replay proves no repair and earns no origin learning credit. The default
fast path requires three independently verified live successes and zero failures
for the unchanged accepted revision. Use a matching program's plan/apply tools
when available. An ad-hoc repair can also earn a live credit when the controller
has a pre-repair matching plan, successful receipts covering every planned
resource, exact observed plan equivalence, a bound post-repair NoMatch and an
independently successful outcome. Merely flipping Match to NoMatch is insufficient.
The live executor's object/rollout checks still apply; a skipped replay does not
authorize an unsupported mutation such as recreating a missing target.

## Routing: observe listeners before SetField repair

Declared container ports select probe candidates; they never prove a listener.
For a numeric targetPort mismatch, require selected ready Pods with a successful
recorded `ProbeTCP(pod, candidatePort)` and a refused
`ProbeTCP(pod, faultyTargetPort)`. An unrecorded call or read error remains Unknown.
A target that also accepts connections, an unready Pod, multiple candidate ports,
or other ambiguity should remain Unknown or NeedsJudgement.

The controller records real TCP results even before a program exists, for finding
Services and up to two healthy peers. It uses the phase snapshot's Pod IP through
the isolated prober. Sampling is limited to 24 probes, two Pods per Service, four
backend ports per Pod, 200 ms per probe and two seconds per phase. Capped or
unavailable observations are not negative evidence. Copy the phase recordings
byte-for-byte, including fixed observations; choose a peer with recorded evidence.

The installed `service-targetport-mismatch` example now includes this diagnosis,
its SetField mitigator, ready-Pod snapshots and phase-specific TCP recordings.
Its narrow worked plan derives the replacement from the unique common declared
port **after** confirming it accepts connections on all selected ready Pods:

```go
func (p Program) Mitigate(ctx context.Context, v program.ReadView, b program.Bindings) (program.Plan, error) {
    verdict, err := p.Diagnose(ctx, v, b) // positive listener + refused target TCP observations
    if err != nil || verdict.Result != program.Match { return program.Plan{}, err }
    _, _, port, ok := replacement(v, b) // same unique candidate verified by Diagnose
    if !ok { return program.Plan{}, nil }
    return program.Plan{Actions: []program.Action{program.SetField{
        Ref: b["service"], Path: "spec.ports[0].targetPort", Value: port,
        Reason: "use observed selected-Pod listener",
    }}}, nil
}
```

SetField replay still requires post-plan NoMatch. For Service-targetPort-only edits,
replay can retain already observed direct-Pod TCP inputs when the complete Pod
snapshot is unchanged. It never reuses a Service probe as proof that routing was
repaired, copies a fixed answer, or invents a successful connection. Other edits
retain the ordinary no-external-observations post-plan policy.

## Go-owned per-cause work

The supplied work list preserves the responder's confirmed resource groups. Covered repairs and their corpus cases are already settled; address only typed questions. Unwritten bindings, unproven plan equivalence, and writes without a matched binding earn no credit. Unassigned observations remain ungrouped questions. Do not infer merge/split decisions.

A program that still matches the verified fixed capture has a definite false match. Go seeds that exact capture as negative evidence and preserves it through edits; the failing NoMatch replay is the question to resolve. The [generated replay API](../../sdo-detector-authoring/references/replay.md) defines CapturedFixedNoMatchPhase for this observed negative evidence. It is never a positive incident-open phase, a synthetic positive case or a trust credit.
