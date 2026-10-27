# Fresh memory control state

A legacy finalized record without `reachability-reference.json` grants no port
exclusions. Startup emits a typed `reference_absent` note identifying the control
namespace, ConfigMap and missing key; ordinary reachability findings remain active.
A from-scratch memory stream explicitly stops the prior runtime and invokes
`sdo-operator reset-control-state --control-namespace <app>-sdo` before native
install. The command deletes only the three SDO-managed onboarding ConfigMaps
and the controller checkpoint,
checks ownership and deletion preconditions, and logs each deleted identity.
It also deletes every worker task ConfigMap (`<job>-request|result|execution|binding`,
`sdo-exec-*`, `sdo-late-*`; names owned by `worker_registry.IsWorkerConfigMap`);
warm resets run the same command with `--workers-only`.
It never reads Secrets or changes application objects. Fresh fastloop state
(including a v2 fresh-seed environment) uses this path; retained controller
records and warm resets preserve the existing onboarding record and reference.
The next confirmed healthy install evaluation must create a new reference;
reset itself supplies no health authority.

The legacy controller checkpoint is recognized by its exact name,
`sdo.dev/state-revision` annotation and `runtime-state.json` key. Retiring it
prevents restored controller state from resurrecting old onboarding authority.
