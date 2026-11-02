This fixture preserves the exact `fastloop_application` section of AST3's
pre-reset sealed astronomy baseline, captured on 2026-10-26. Unrelated harness
infrastructure fields were omitted. The installed object digest remains intact.
The `kubernetes/` fixture contains only the six accepted non-Secret manifests needed
for its missing ServiceAccounts and namespace RBAC. The test commits them as the
accepted source of a scratch repository. No Secret manifest or payload is present.

The regression restores the reference-derived accounts and grants, retains all
sealed workload specs, and reports the remaining external `Secret/grafana`.
