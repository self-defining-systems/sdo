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
