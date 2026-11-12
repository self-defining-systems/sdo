# Operational-memory layout

GENERATED from `memory/layout/paths.go` and `memory/validate/ownership.go`. Do not edit.
Regenerate from `sdo/operator` with `SDO_UPDATE_LAYOUT_REFERENCE=1 go test ./memory/validate -run '^TestMemoryLayoutReference$'`.

Schema version: 7. Paths are repository-relative and used only while editing a proposal checkout; worker results and memory prose use logical IDs.

- Playbook, library and corpus IDs: `^[a-z][a-z0-9_-]*$`.
- Detector IDs: `^[a-z0-9][a-z0-9_.-]*$`.
- Script IDs: `<playbook-id>/<filename>.sh`.
- Outcomes: SHA-256 of incident ID; patches: `NNNN-<slug>.patch` with `<slug>` matching `[a-z0-9][a-z0-9-]{0,48}`.

## Paths, kinds and write authority

| Path | Kind | Logical ID | Writer and phase |
| --- | --- | --- | --- |
| `.sdo/schema-version` | `schema-version` | `—` | changeable by upkeep in the initial phase |
| `.sdo/goal.md` | `goal` | `—` | changeable by human in the initial phase |
| `.sdo/arch.md` | `architecture` | `—` | changeable by health_judge in the initial phase or upkeep in the initial phase |
| `.sdo/lifecycle-provenance.yaml` | `lifecycle-provenance` | `—` | no agent may change this file; the controller owns it |
| `.sdo/diagnostics/go.mod` | `diagnostics-module` | `go.mod` | changeable by upkeep in the initial phase |
| `.sdo/diagnostics/go.sum` | `diagnostics-module` | `go.sum` | changeable by upkeep in the initial phase |
| `.sdo/diagnostics/manifest.yaml` | `legacy-detector-manifest` | `—` | changeable by upkeep in the initial phase |
| `.sdo/diagnostics/detectors/health/<detector-id>/manifest.yaml` | `health-detector-manifest` | `detector-id` | changeable by health_judge in the initial phase or upkeep in the initial phase |
| `.sdo/diagnostics/detectors/health/<detector-id>/detector.go` | `health-detector-source` | `detector-id` | changeable by health_judge in the initial phase or upkeep in the initial phase |
| `.sdo/diagnostics/detectors/incidents/<detector-id>/manifest.yaml` | `incident-detector-manifest` | `detector-id` | changeable by human in the import phase or responder in the reflection phase or upkeep in the initial phase |
| `.sdo/diagnostics/detectors/incidents/<detector-id>/detector.go` | `incident-detector-source` | `detector-id` | changeable by human in the import phase or responder in the reflection phase or upkeep in the initial phase |
| `.sdo/diagnostics/traffic/generators/application/generator.go` | `traffic-generator` | `application` | changeable by health_judge in the initial phase |
| `.sdo/diagnostics/traffic/generators/incident/generator.go` | `traffic-generator` | `incident` | changeable by responder in the reflection phase |
| `.sdo/diagnostics/traffic/workloads/application.yaml` | `traffic-workload` | `application` | changeable by health_judge in the initial phase |
| `.sdo/diagnostics/traffic/workloads/incident-example.yaml` | `traffic-workload` | `incident-example` | changeable by responder in the reflection phase |
| `.sdo/goal/sources.yaml` | `sources` | `—` | changeable by human in the initial phase |
| `.sdo/playbooks/README.md` | `playbook-index` | `—` | changeable by health_judge in the initial phase or human in the import phase or responder in the reflection phase |
| `.sdo/playbooks/<playbook-id>/README.md` | `playbook` | `playbook-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/playbooks/health-objective/README.md` | `playbook` | `health-objective` | changeable by health_judge in the initial phase or responder in the reflection phase |
| `.sdo/playbooks/<playbook-id>/diagnose.go` | `playbook-program` | `playbook-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/playbooks/<playbook-id>/mitigate.go` | `playbook-program` | `playbook-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/playbooks/<playbook-id>/program_test.go` | `playbook-program` | `playbook-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/playbooks/<playbook-id>/scripts/repair.sh` | `playbook-script` | `playbook-id/repair.sh` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/lib/<library-id>/helper.go` | `library` | `library-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/lib/<library-id>/metadata.yaml` | `library` | `library-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/open.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/fixed.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/healthy.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/recordings.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/labels.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/operator.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/corpus/<case-id>/accepted.json` | `corpus` | `case-id` | changeable by human in the import phase or responder in the reflection phase |
| `.sdo/deployment/plan.yaml` | `deployment-plan` | `—` | changeable by deployer in the advance phase or deployer in the initial phase or human in the initial phase |
| `.sdo/deployment/values/<file>.yaml` | `deployment-file` | `values` | changeable by deployer in the advance phase or deployer in the initial phase or human in the initial phase |
| `.sdo/deployment/manifests/<file>.yaml` | `deployment-file` | `manifests` | changeable by deployer in the advance phase or deployer in the initial phase or human in the initial phase |
| `.sdo/deployment/build/Dockerfile` | `deployment-file` | `build` | changeable by deployer in the advance phase or deployer in the initial phase or human in the initial phase |
| `.sdo/deployment/overlays/telemetry/<file>.yaml` | `deployment-overlay` | `telemetry` | changeable by health_judge in the initial phase or human in the initial phase |
| `.sdo/deployment/patches/<NNNN-slug>.patch` | `deployment-patch` | `<NNNN-slug>` | changeable by deployer in the advance phase or deployer in the initial phase or human in the initial phase or responder in the proposal phase |
| `.sdo/diagnostics/telemetry/sources.yaml` | `telemetry-sources` | `—` | changeable by health_judge in the initial phase |
| `.sdo/diagnostics/telemetry/backends.yaml` | `telemetry-backends` | `—` | changeable by health_judge in the initial phase |
| `.sdo/diagnostics/data/<check-id>.yaml` | `data-check` | `check-id` | changeable by health_judge in the initial phase |
| `.sdo/outcomes/<sha256(incident-id)>.json` | `outcome` | `<sha256(incident-id)>` | no agent may change this file; the controller owns it |

The controller also writes bootstrap schema/module metadata and immutable corpus captures through trusted storage edges; these are not agent grants. The import phase admits human-supplied learned memory; upkeep migrates existing formats. No agent writes outcomes or lifecycle provenance. Deployment files (`.sdo/deployment/`) are the deployment source: the deployer authors the plan, files and patches (in the initial and advance phases, changing or deleting only patches whose header names it), the detection designer owns only `overlays/telemetry/`, and a responder may add (never modify or delete) patches in a proposal. Telemetry bindings (`.sdo/diagnostics/telemetry/sources.yaml` and `backends.yaml`) are the detection designer's alone, in the initial phase, and only while the environment's telemetry mode is open. Data checks (`.sdo/diagnostics/data/<check-id>.yaml`) are the detection designer's alone; Go installs a `data-<check-id>` health detector for each. Files outside `.sdo/` still belong to the deployer in the initial phase and the responder in the proposal phase until enforcement flips to patches only.

In `arch.md` the health judge (the detection designer) may change only the application-map region between the `sdo:application-map` marker lines; everything else in it is Go upkeep's, which carries the region verbatim.

Detector packages may contain additional source/test files; traffic generators may contain package files. Program files are `diagnose.go`, optional `mitigate.go` and `*_test.go`. Incident traffic is the `incident` generator and `incident-*` workloads; all other traffic belongs to the detection designer. Legacy manifest and outcome paths exist for migration only. Preserve accepted provenance and corpus cases.
