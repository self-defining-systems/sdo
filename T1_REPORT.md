# T1: independent per-cause reflection contracts

## Result and scope

The requested contracts are **not satisfied** by the tested R6 implementation.
The tests deliberately remain failing for R6 to fix; there are no xfails,
skips of failing contracts, or production fixes in T1's authored changes.

Branch: `vic-lsh/dev/reflection-contract-tests`, checkout
`/mnt/data/shli/sdo-t1`. Started at dev `d6add106e`; merged #129
`eaa6fdd1bbab75e508599609ae47130498920e2d`, which contains #126
`7c4c249930abc598711baace029ab96b6f677be1`. Integration merge:
`f3591f928`. Existing main-checkout edits were left untouched.

The oracle comes from decisions 162–176/186 and `RV1_REVIEW.md`.
Only pre-existing Git/closure/task-transport setup helpers are shared with R6's
tests. Outcome facts, controller transcripts, program/recording fixtures,
expected classifications, and trust assertions are independently hand-written.
No credentials were inspected, no model or cluster was run, and no L2 data is
committed. Archived deterministic program replay is offline, in temporary files.

## Added tests

- `sdo/operator/runtime/reflection_contract_test.go`: all eleven requested
  classes through pure classification and scripted broker execution, plus
  evidence-gap decline authorization, recording-order independence, all unbound
  writes, and hand-written readiness/ConfigMap reductions of LM1's findings.
- The broker matrix crosses actual Git proposals, durable outcome documents,
  registered protobuf tasks/results, controller-authored corpus files, ordinary
  broker validation/replay rejection, and closure restart. It asserts work-item
  identity, model dispatch, positive/negative case counts, exact protected bytes,
  provenance, trust credit, and gap counts. The small diagnoser uses the real
  `DiffAgainstSource` replay primitive. Its broker validator replays the fixed
  case; this is a hermetic protocol e2e, not a controller image or cluster smoke.
- `TestT1TrustProperty`: fixed seed 186, sixteen broker instances, random
  reflection acceptance/rejection and outcome arrival permutations, including
  every requested class. A non-covered instance may never increase trust;
  shuffled input must preserve chronological trust. The loop has a four-minute
  deadline and is run under Go's five-minute timeout.
- `tests/unit/sdo/agents/responder/test_reflection_contract.py`: eighteen
  real worker/session/provider-parser cases, using agentshim's scripted executor.
  Both full and refine prompts must preserve covered work, every question class,
  unassigned work, and forced covered reflection with an existing corpus edit.
- `sdo/operator/runtime/reflection_contract_l2_test.go`: opt-in, read-only
  replay of the actual L2 receipts, protected recordings/captures, and exact
  accepted program source at each outcome's source commit. It compiles the
  archived diagnosers in temporary modules, evaluates fixed observations with
  fresh replay views, and feeds the verdicts into the new classifier. It does
  not fill missing final-equivalence fields. Historical removed proto fields
  are explicitly discarded while parsing receipts.

## Requested matrix and failures for R6

Every broker row runs with publication permitted and with scripted publication
rejection. A covered case whose publication is rejected may invoke the fallback
worker; the zero-model requirement is asserted on successful exact publication.
A false-matching unchanged program must fail its negative corpus replay.

1. **Covered exact:** passes. Covered item, no worker on accepted publication,
   one positive case, +1 credit. Rejection falls back to the worker.
2. **Fixed capture still Matches:** typed false-match item and fixed NoMatch
   case are correct, and replay rejects the unchanged program. **Fails trust:**
   the persisted outcome still gives +1, including after reflection rejection.
3. **Binding never written:** passes. Question `binding_not_written`, worker,
   no automatic corpus case, no credit.
4. **Same binding, different fields:** passes. Controller final-equivalence
   false, question `repair_equivalence_question`, worker, no case/credit.
5. **Write with no program binding:** passes for one resource. Question
   `unbound_write`, worker, no case/credit. The additional all-writes test fails
   because both grouped and unassigned lists omit the second repaired resource.
6. **No program:** passes. Unsettled-cause question, worker, no case/credit.
7. **Evidence gap:** Go flags the missing fixed resource observation and the
   worker may decline. **Fails trust:** an earlier valid apply transcript still
   gives +1 despite the current fixed observation being unavailable. An unflagged
   gap decline correctly gets three typed rejection attempts.
8. **Partly covered incident:** passes. The exact cause produces a positive
   case/+1; the other repaired cause remains an unbound-write question and
   reflection runs.
9. **Nil final-equivalence proof:** **fails** classification, dispatch, corpus,
   and credit. It becomes covered, may skip reflection, writes a positive case,
   and earns +1. Nil must remain unproven.
10. **Absent fixed verdict:** **fails** the same boundaries. The classifier
    treats an absent registry verdict like NoMatch, even with complete recordings.
    Expected: question, worker, no automatic case, no credit.
11. **Similar prior repair, current false match:** **fails** dispatch, corpus,
    and credit. The old exact-replay shortcut invokes no worker, writes no
    negative case, and still credits +1. The test enters through `ProcessClosure`
    with real prior accepted outcomes, rather than calling only the classifier.

Additional failing tests:

- `TestT1TrustProperty`: non-covered false matches, evidence gaps, nil final
  proof, absent verdicts, and bypassed repeats increase trust in the seeded
  history. These failures are independent of reflection acceptance/rejection.
- `TestT1FixedReplayIsIndependentOfProgramOrder`: the second of two accepted
  programs needing the same immutable recording becomes Unknown; reversing the
  order reverses which program loses the observation. RV1 finding 6 reproduced.
- `TestT1EveryUnboundWriteGetsAQuestion`: the second distinct resource is
  omitted, both within a cause and outside confirmed groups. RV1 finding 7.
- All nine Python `depth=refine` cases fail because the actual prompt loses the
  typed work; all nine full-prompt counterparts pass. RV1 finding 4 reproduced
  through the worker, `SessionReflector`, and real provider stream parser.

R6's priority repairs remain RV1 findings 1/2/3/4: persist controller-owned
credit eligibility before the authoritative outcome, require affirmative final
and fixed proof, remove classification-bypassing skips, and preserve typed work
in the actual session prompt. T1 does not change those implementations.

## L2 shadow replay

Run root from `LM1_REPORT.md`:

`/mnt/data/shli/sdo/.claude/worktrees/l2/third_party/sregym/logs/20261022_175841_pipeline_mixed10-sdo-luna-dev-r1`

Each input outcome is the stage's strict production receipt's
`native_ledger.outcome_document`. Protected captures come from the cumulative
stage-7 runtime session export, indexed by the first 32 hex characters of
SHA256(incident ID). Program sources are read with `git show` from stage-8's
application workspace at the outcome's accepted `source_commit`. No archive
session-preparation or corpus-writing helper is called.

- **i02 readiness** (`hotel-reservation-1792692732357519937`): question
  `unbound_write`, zero presence gaps, zero credit. Matches the expected label.
  Existing target-port evaluations did not establish an installed readiness
  program. A recorded DiffAgainstSource is present; LM1's malformed accepted/
  observed readiness comparison is not affirmative exact coverage.
- **i04 frontend image pull + reservation target port**
  (`hotel-reservation-1792693266072789954`): both accepted diagnosers replay
  **NoMatch** against the fixed capture. Zero presence gaps. **Two mismatches:**
  Go labels both causes covered and awards each program +1 while the shared
  application's final-equivalence field is absent. Under T1's explicit nil-is-
  unproven rule, both must remain questions until exact final proof is supplied.
  No historical compatibility exception was assumed.
- **i05 missing ConfigMap** (`hotel-reservation-1792693520571401079`):
  unbound-write question plus **four** resource/phase gaps, zero credit. Missing
  open/fixed observations are for ConfigMap/mongo-geo-script and
  Service/mongodb-geo; the Deployment has recorded source observations. Matches
  the expected evidence-gap label. Go's flag proves presence gaps, not the later
  ConfigMap-deletion timing; that timing is established in LM1. The hand-written
  reduction retains these exact four missing resource/phase relationships.
- **i06 rate image pull + reservation target port**
  (`hotel-reservation-1792693794136427021`): both fixed diagnoses replay
  **NoMatch**, but exact eligibility is not established. Both applications lack
  final-equivalence proof and affirmative `verification_passed`, with aggregate
  `RESULT_MATCH` and recorded application-verification failures. Go emits an
  image-pull repair-equivalence question. Reservation is outside the single
  confirmed rate cause and remains unassigned, with its separate write question.
  Zero success credit; each program currently has one failure. Do not relabel
  this fully covered from recurring-fault resemblance alone.
- **i07 recommendation + geo target ports**
  (`hotel-reservation-1792694092949244225`): both fixed bindings replay
  **NoMatch**, zero presence gaps, but both applications lack final proof. The
  first has no affirmative verification-pass flag and the outcome carries a
  program-verification failure; the second does have that pass flag. Go emits
  two repair-equivalence questions. Zero success credit, two current failures
  for the target-port program. Exact covered eligibility remains unproven.

Only i04 differs from T1's strict expected coverage labels. i06/i07's inability
to qualify is reported explicitly rather than manufacturing proof. The shadow
replay reports fixed verdicts, typed work, missing-proof counts, gaps, unassigned
items, and trust for every incident; it intentionally fails on i04's mismatches.

## Reproduction and validation

From the repository root:

```bash
uv sync --extra test
bash scripts/format_code.sh
PATH=/mnt/data/shli/cache/protoc-33.4/bin:$PATH bash scripts/check_errors.sh
uv run --extra test pytest -q --no-cov \
  tests/unit/sdo/agents/responder/test_reflection_contract.py \
  tests/unit/sdo/agents/responder/test_reflection_worker.py
cd sdo/operator/runtime
go test -json -run '^TestT1' -count=1 -timeout=5m .
SDO_T1_L2_RUN=/path/to/archived/run \
  go test -run '^TestT1L2ShadowReplay$' -v -count=1 -timeout=4m .
```

`format_code.sh`: pass (`285 files left unchanged`). `check_errors.sh`: pass;
ruff, architecture, pyright (zero errors; one pre-existing `__all__` warning),
proto, purity, and Go mains passed. The initial proto attempt hit the BSR rate
limit without protoc on PATH; rerunning with the already-installed pinned 33.4
compiler passed, including unchanged generated output.

Python combined run: **9 failed, 22 passed in 3.09s**. The thirteen pre-existing
worker tests pass; only the nine new refine contracts fail. Offline L2 replay:
**FAIL `sdo.dev/operator/runtime` 3.961s**, with i02/i05/i06/i07 passing and i04
reporting the two missing-final-proof mismatches. Hermetic Go failures are the
contract failures listed above; see final run results below.

Local logs (not committed): `/mnt/data/shli/sdo-exp/mixed10/T1_go_final.jsonl`,
`T1_python.log`, `T1_l2.log`, `T1_format.log`, `T1_checks.log`.

Final hermetic Go run: **FAIL `sdo.dev/operator/runtime` 104.375s**.
Machine-readable leaf results: **24 passed, 17 failed, 1 skipped** (the skipped
case is the opt-in L2 replay, which was run separately). Property test: **42.26s**,
well below five minutes. Both minimal historical reductions and the unflagged
Gap-decline rejection test pass. Later focused reruns verify the protected-byte
and publication/replay assertions added during final review.

Failing hermetic Go test names for R6 (brace notation expands subtests):

- `TestT1ClassificationMatrix/{nil_final_proof,absent_fixed_verdict}`
- `TestT1ScriptedBrokerMatrix/{fixed_still_matches,evidence_gap,nil_final_proof,absent_fixed_verdict,similar_repeat_current_false_match}/reject={false,true}`
- `TestT1TrustProperty`
- `TestT1EveryUnboundWriteGetsAQuestion/grouped={false,true}`
- `TestT1FixedReplayIsIndependentOfProgramOrder/reversed={false,true}`
- The separate opt-in replay: `TestT1L2ShadowReplay/i04`.
