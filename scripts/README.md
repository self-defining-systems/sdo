# Repository tooling map

- `format_code.sh`: Python formatting (use `--check` in CI).
- `check_errors.sh`: static gate: Ruff, Tach, Python ownership/import contracts,
  types, proto drift, Go core purity, Go nondeterminism, benchmark compilation.
- `check_go_core.sh`, `go_pure_packages.txt`, `.golangci.yml`: declared pure Go
  cores; structural tests in `sdo/operator/internal/purity`.
- `check_go_determinism.sh`, `go_determinism_allowlist.txt`: codebase Go API
  ratchet; AST scanner and negative controls in
  `sdo/operator/internal/purity/determinism`. See `docs/go-purity.md` for workflow.
- `test_go_ci.sh`: multi-module Go suite, shared compiler cache and PR/nightly selection.
- `test_python.sh`: Python unit suite.

Extend the owning gate when adding a check. Keep policy fixtures alongside its
implementation; wire mandatory static checks through `check_errors.sh`.
