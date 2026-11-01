# Repository tooling map

- `format_code.sh`: Python formatting (use `--check` in CI).
- `check_errors.sh`: static gate: Ruff, Tach, Python ownership/import contracts,
  types, proto drift, Go core purity, Go nondeterminism, production and benchmark compilation.
- `check_go_core.sh`, `go_pure_packages.txt`, `.golangci.yml`: declared pure Go
  cores; structural tests in `sdo/operator/internal/purity`.
- `check_go_determinism.sh`, `go_determinism_allowlist.txt`: codebase Go API
  ratchet; AST scanner and negative controls in
  `sdo/operator/internal/purity/determinism`. See `docs/go-purity.md` for workflow.
- `test_go_ci.sh`: multi-module Go suite, shared compiler cache and PR/nightly selection.
- `test_python.sh`: Python unit suite.

Extend the owning gate when adding a check. Keep policy fixtures alongside its
implementation; wire mandatory static checks through `check_errors.sh`.

- `check_go_mains.sh`: test-free production package builds plus benchmark Go vet; catches declarations accidentally supplied only by tests before integration builds.

- `split_go_integration.sh`: report subprocess-dependent Go test declarations,
  then split explicitly reviewed declarations into an integration-tagged file.
  AST boundaries preserve embedded source strings; ambiguous selections/imports
  and existing destination files fail closed. Both test build selections still
  require validation; this is a migration generator, not a policy exemption.
