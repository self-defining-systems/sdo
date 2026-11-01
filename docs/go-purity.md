# Go functional-core boundary

Declare production core packages in `scripts/go_pure_packages.txt` and keep their
path scopes in `.golangci.yml` synchronized. `contracts/outcome` and `runtime/lifecyclecore` are enrolled. `outcome_derivation` still
has mutable lookup maps and a context parameter, so it requires a later extraction.

`bash scripts/check_go_core.sh` runs pinned golangci-lint **2.11.4** with depguard
and forbidigo, followed by a small `go/packages` architecture test. Both
`scripts/check_errors.sh` and `scripts/check_arch.sh` run it, including the existing
CI architecture job. Each linter/test invocation has a 45-second limit. A negative
fixture checks that the committed configuration rejects I/O and renamed clock/RNG
imports. Installation caches a versioned release binary after checksum verification.

Cores accept values/events and return state/effects. They must not import
Kubernetes clients, networking, processes, filesystem APIs, randomness, contexts,
or their subsystem's internal edge. Time arrives in events; clock reads and timers
are forbidden. Goroutines, channel types, mutable package variables and `init`
functions are forbidden. Local mutation inside a transition is allowed.

The structural test follows application-owned imports transitively, so a wrapper
cannot hide I/O or a global cache from the declared core's checks. Generated
protobuf support, third-party runtimes and standard-library implementations are
trusted dependencies: their implementation globals and internal I/O are not
re-linted. The rule applies to application source, not Go runtime internals.

Keep I/O adapters under each subsystem's `internal/...`. Go's internal visibility
prevents other subsystems importing those adapters; the explicit depguard rule
also prevents the core importing its own edge. Only the driver executes effects.

These checks enforce an architectural boundary, not a proof of determinism:
map iteration and input aliasing also need behavioral checks. The lifecycle
explorer must repeat every `(state, event)` transition and compare canonical
state/effect bytes, verify that inputs remain unchanged, and explore retry/restart
orders independently of the real-world driver.

## Codebase nondeterminism ratchet

`scripts/check_go_determinism.sh` complements the stricter functional-core gate.
It reuses its AST/import-resolution approach without loading dependency graphs.
It scans every `.go` file under `sdo/operator/` (including all nested modules)
and `benchmarks/`, regardless of build selection. `vendor`, `.git` and `testdata`
fixture trees are excluded; generated markers do not exempt application code.
Python is outside the deterministic simulation policy.

Production references to wall-clock APIs (`time.Now`, `Since`, `Until`, `Sleep`,
`After`, `AfterFunc`, `Tick`, `NewTimer`, `NewTicker`), global randomness,
network dial/listen APIs, concrete HTTP clients/transports, process execution and
environment reads are forbidden. Import aliases and function values are checked,
not just calls. Dot imports are forbidden. Use event time and injected clocks,
explicitly seeded RNGs, dialers and launchers. Seeded RNG construction is allowed;
`crypto/rand` is conservatively forbidden regardless of purpose.

The exact production edge-package list is:

- `sdo/operator/core/internal/kubeio`: Kubernetes configuration discovery and default observation clock.
- `sdo/operator/sdk/internal/clockio`: SDK default wall clock; engines accept an injected clock.
- `sdo/operator/builder/internal/buildio`: compiler, cache seeding and controller process launcher.
- `sdo/operator/operatorcli/internal/processio`: CLI Git, kubectl and environment adapter.

- `sdo/operator/runtime/internal/lifecycleio`: Kubernetes lifecycle driver.
- `sdo/operator/runtime/prober`: protocol dial adapter.
- `sdo/operator/install/internal/installio`: accepted-checkout Git/archive/kubectl processes, installer clock and wait timers, and time-scale environment reads.
- `sdo/operator/runtime/prometheus`: Prometheus HTTP adapter.
- `sdo/operator/memoryhost/hostio`: memory-host process launcher, Unix socket server/client and read-only network adapter.
- `sdo/operator/memory/internal/memoryio`: Git reads, isolated-index whitespace checks, shell syntax and toolchain lookup. No process handles escape.
- `sdo/operator/deployment/internal/deploymentio`: trusted deployment processes, bounded local health prober sockets, wait timers and Job-name suffixes.
- `sdo/operator/toolchain`: Go compiler process adapter.
- `sdo/operator/devmount`: development mount process adapter.
- `sdo/operator/runtime/sim`: the deterministic simulation world. Its clock reads are the synctest bubble's
  virtual time, its one environment read is the `SDO_SIM_SEED` replay seed, and its HTTP client runs over an
  in-memory transport. Only test code may import it.

These are exact directories, not recursive exemptions. Mixed packages containing
existing kubelet, logs, dial, process, git and clock code are grandfathered only
at their recorded file/symbol pairs; they are not blanket edges. Extract such
adapters before adding new nondeterministic APIs. Edge packages do not exempt tests.

Tests may use real clocks, subprocesses or sockets only when a `//go:build`
expression *requires* `integration` (e.g. `integration && linux`, never
`integration || linux`). All `time.Now` references are conservatively checked,
including assertion timestamps. `net.Pipe`, HTTP recorders, in-memory gRPC,
injected clocks and inline `testing/synctest.Test`/`Run` bubbles are allowed;
time APIs inside those bubbles are virtual, while real I/O is still rejected.

The sorted `scripts/go_determinism_allowlist.txt` inventory records one
`file:symbol` per legacy allowance, without unstable line numbers. It is a
file/symbol ratchet, not an occurrence counter: more calls to an already recorded
symbol in the same file cannot be distinguished. This static API gate does not
prove semantic determinism or inspect arbitrary third-party implementations;
retain the functional-core gate and behavioral simulation checks.

Run `bash scripts/check_go_determinism.sh` to check. After removing violations,
run `bash scripts/check_go_determinism.sh --update`; it removes fixed entries and
refuses new ones. `-bootstrap` on the Go command prints a proposed inventory for
inspection only. The check compares the working list to the committed merge-base
with `origin/main` (override with `SDO_DETERMINISM_BASE` for offline tooling), so
regenerating or committing an expanded list cannot bless new exceptions. The
first rollout pins the first committed inventory when that base predates the
allowlist; later rollout commits cannot enlarge it either. CI fetches
history for this comparison. Stale entries fail too. New baseline exceptions
require changing the policy, not regeneration.

This gate runs its negative fixtures before scanning, is wired into
`scripts/check_errors.sh`, and takes about one second with a warm compiler cache.
