# SDO review checklist

Apply the relevant checks rather than mechanically reporting every item.

## Architecture and repository scope

- [ ] `sdo operate` is the only production lifecycle entry point.
- [ ] Production behavior follows source deployment, independent health judging, `.sdo` bootstrap, and controller handoff.
- [ ] Bounded shell monitoring, generated shell health checks, duplicate deployment lifecycles, historical trajectory stacks, and standalone fault injection are absent.
- [ ] Production packages do not import SREGym packages, verdicts, task APIs, or submission transports.
- [ ] Benchmark adapters reuse production APIs instead of reimplementing controller semantics.
- [ ] Names, environment variables, module paths, docs, images, and API versions consistently use SDO terminology.

## Lifecycle and agents

- [ ] The health objective is human-supplied and persisted exactly.
- [ ] Source deployment requires a real, attributable Git commit and independent verification.
- [ ] Deployer and health judge use separate fresh sessions and structured, validated handoffs.
- [ ] Architecture inventory and topology fingerprint are grounded in tracked source.
- [ ] Health-judge corrections are bounded and detector source/tests pass deterministic validation.
- [ ] Agent backends honor one interface without nested event loops or hidden shared conversation state.

## Operational memory and Git

- [ ] Goal, architecture, playbooks, diagnostics, and outcomes enforce distinct ownership.
- [ ] Agents work in contained isolated worktrees and cannot escape through paths or symlinks.
- [ ] Commit attribution, parent/head checks, and merge ordering prevent stale or unrelated changes.
- [ ] Outcomes are controller-owned and append-only.
- [ ] Broker operations are idempotent across retries and restarts.
- [ ] Reflection is classification-aware, same-session where required, and based on committed outcomes/history.

## Controller and detectors

- [ ] Detector runtime is deterministic Go and consumes only declared snapshot APIs.
- [ ] Manifest class and owner agree; watches, persistence, batching, playbooks, and provenance are validated.
- [ ] Generated packages compile and include matching plus near-miss tests.
- [ ] Scheduler, cache, finding persistence, batching, dispatch, and acknowledgement survive restart semantics.
- [ ] Incident IDs and idempotency keys correlate requests, results, ledgers, outcomes, and receipts.
- [ ] Leader election prevents duplicate active effects.
- [ ] Accepted detector changes trigger a validated, correlated controller rollout.

## Security

- [ ] Generated code and repair proposals run in bounded controller subprocesses with explicit time and memory limits.
- [ ] Shell commands avoid injection and unresolved destructive targets.
- [ ] Secrets are mounted or passed narrowly and never committed, logged, or copied into memory.
- [ ] Kubernetes RBAC, pod security contexts, service-account tokens, volumes, and network access use least privilege.
- [ ] Repository synchronization and worktree cleanup cannot overwrite paths outside the intended application/PVC.
- [ ] Benchmark labels and external verdicts cannot leak into production prompts or detectors.

## Python and configuration

- [ ] Reused structured shapes are dataclasses or Pydantic models, not ad hoc mappings.
- [ ] Configuration validates types, values, positive timeouts, and mutually exclusive options.
- [ ] Async handlers await coroutines on the existing event loop.
- [ ] Exceptions preserve actionable context without exposing secrets.
- [ ] Subprocess timeouts, return codes, partial output, and cleanup paths are handled.
- [ ] Removed compatibility code has been deleted rather than left unreachable.

## Tests and evidence

- [ ] Bugs have reproducing tests and features have public contract tests.
- [ ] Ownership, stale commits, malformed handoffs, timeouts, retries, and cleanup failure paths are covered.
- [ ] Controller modules have Go tests for state transitions and restart behavior.
- [ ] Architecture tests reject production-to-benchmark imports and excluded modules.
- [ ] Live-cluster claims cite exact smoke-test evidence; unit tests are not presented as deployment proof.
- [ ] Docs and the component map in `docs/architecture.md` match actual entry points and modules.
