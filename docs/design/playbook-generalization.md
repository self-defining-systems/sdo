# Playbook generalization: localize, deduplicate, organize by repair method

Status: proposed (2026-11-04). Evidence: live SREGym rounds P4/P5 on social-network
(process_frozen, pod network impairment, Redis ACL revoked).

## Problem

Learned memory is organized per (symptom x service) and binds too widely:

1. Responder incident detectors filter a journey SLO finding (`scenario-slo.home-timeline-read`) and
   bind a hard-coded Deployment name. Hint bindings apply to every program with a `deployment` role,
   and the learned stall diagnoses ("matches source, not yet restarted") match any healthy Deployment.
   Result: 3-6 restarts across every service on the failing journey.
2. `RunScript` must be byte-identical to a receipted command, so a recorded
   `kubectl exec <live-pod-name>` was frozen into a program and later hit the wrong Redis.
3. Reflection publishes a second copy of the same repair for a second service, and folds a new repair
   method (Redis ACL restore) into each symptom-named playbook. Adding a required `redis-pod` role that
   no detector binds made the refined playbook stop matching.

There is no deterministic per-component signal a detector or diagnose can use to pick the faulty
component from journey-level findings.

## Mechanism

### (a) Localization: `sdk/localize` (Go SDK, pure)

A new SDK package computes suspects from observations the controller already gives every detector and
program (no new observation, no runtime or proto change):

- `localize.FromContext(ctx sdk.DetectionContext, workload string) Result` judges the health traffic
  window (`traffic.Judge`) and does path algebra over scenario paths (`Target.Service` + `DependsOn`):
  suspects = intersection of failing paths minus every path of a healthy, evaluated scenario (if the
  intersection is empty, the union of per-failing differences). When the service-reachability window
  shows sustained dial failures for some suspects, the set narrows to those. Each suspect Service maps
  to its backing Deployment/StatefulSet by selector, sorted, with an evidence string.
- `Result.Implicates(ref) (bool, string)`: diagnose gate; a program returns NoMatch for a component the
  current findings do not implicate.
- `localize.BindSuspects(f sdk.Finding, r Result, role string) []sdk.Finding`: an incident detector
  emits one finding per suspect workload with `ParameterBindings[role]`, instead of a literal name.

Examples (synthetic shapes of P5 rows): post-storage delay fails both timeline reads while the landing
page is healthy -> {post-storage-service, its cache, its store}, narrowed by reachability;
home-timeline Redis ACL -> {home-timeline-service, home-timeline-redis}; the ACL program's own
evidence (an `Observe` of `ACL DRYRUN` on the bound Redis's Pod) then selects the Redis.

Ownership: SDK (production, deterministic, no I/O). Health detectors and the judge are unchanged.
Responders use it in the incident detectors and playbooks they own. Binding still flows through the
existing finding `ParameterBindings` (binding source `hint`/`finding`).

### (b) Publication checks in the shared `validate.Check` (static layer)

Run by both `sdo-operator memory check` and the broker, on changed paths only (existing memory keeps
validating until edited):

- `playbook.live-object-name`: a changed Go or script file under `.sdo/playbooks/`, `.sdo/lib/` or
  `.sdo/diagnostics/detectors/incidents/` contains a string literal with a controller-generated Pod
  name (`<name>-<template-hash>-<suffix>`, both drawn from Kubernetes' `SafeEncodeString` alphabet
  `bcdfghjklmnpqrstvwxz2456789`, hash 6-10, suffix 5). Hint: derive the Pod from the bound workload
  (owner relation) and use a typed exec (`EnsureExec`/`Command{Pod: ...}`), which needs no receipt.
- `playbook.duplicate-repair`: a playbook added in this diff (absent at base, not retired) has the same
  repair signature as another active playbook. Signature = ordered parameter Kinds plus the normalized
  AST of `Mitigate` and its package helpers (all but `Spec`/`Diagnose`): comments and package name
  dropped, local identifiers and role names canonicalized by first use / spec order, prose fields
  (`Reason`, `Rationale`, `Fact`, `Observed`, `Expected`, `Summary`, `Description`) erased, and the
  program ID and accepted-source object names replaced by placeholders. Exact match only, no
  thresholds. Hint: refine the existing playbook (widen its `Diagnose` or incident detector, bind the
  component by role) instead of adding a per-symptom or per-service copy.
- `detector.literal-binding`: a changed responder incident detector assigns `ParameterBindings` an
  `ObjectRef` whose `Name` is a string literal. Hint: bind from the finding's resources or
  `localize.BindSuspects`. (Lands with (a), so the hint names an API that exists.)

### (c) Skill guidance (same PRs as the behavior)

`sdo-playbook-authoring` and `sdo-detector-authoring`: a playbook is one repair method, named for the
repair (`redis-acl-restore`, `workload-restart`), not for a symptom or service; its roles bind the
component; a new cause for an existing repair widens that playbook's `Diagnose`; a new repair method is
a new playbook, not an extra action in a symptom playbook; never add a required role no detector binds
(derive Pods from the bound workload); gate diagnose on `Implicates`; when a recorded command names a
live Pod, encode a typed exec instead of `RunScript`. Rule references in `references/rules.md`.

## Testing (deterministic)

- `sdk/localize`: table tests over synthetic traffic windows and reachability windows (single journey,
  shared dependency, empty intersection, unqualified scenario, reachability narrowing, StatefulSet
  backing), plus `sdktest` snapshot use from a detector and a program.
- Validator: gate-table cases with hand-written synthetic memory trees (pass/fail per rule: new copy of
  an existing repair modulo names and prose; refinement of an existing duplicate passes; distinct field
  paths stay distinct; Pod-name literal in mitigate/script/detector; StatefulSet `-0` names and Services
  pass; literal vs relation-derived binding).
- Worked examples in `sdo/skills/sdo-playbook-authoring/assets/examples/` keep passing the memory check
  (`worked_examples_test.go`); skill lint and prompt-budget tests.

## Will not do

- No runtime/controller change (no automatic `localized` binding source, no candidate expansion, no
  recovery_wait change); PX1 owns those files. Possible follow-up once (a) shows value.
- No proto/contract change, no LLM or fuzzy similarity, no similarity thresholds.
- No automatic merge, retirement or migration of existing duplicate playbooks or detectors.
- No change to health detectors, the judge's traffic generators, or SREGym/benchmark code.
- No live runs (the orchestrator runs the reuse wave).
