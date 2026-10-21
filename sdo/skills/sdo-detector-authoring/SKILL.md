---
name: sdo-detector-authoring
description: Use when authoring or changing an SDO detector - a deterministic Go health or incident detector under operational memory. Gives the memory new -> write -> go test -> memory check workflow, the Go SDK reference, the validator rules, and a complete example.
---

# Authoring a detector

A detector is deterministic Go that reads a Kubernetes snapshot and returns findings. It never calls an LLM, reads
the environment, the clock, or the network, and never looks at benchmark data or verdicts. Read the
`sdo-memory` skill first for artifact IDs and the `memory` commands.

## Workflow

1. Look before you add: `sdo-operator memory ls --kind detector`. Widen an existing detector when only resource
   identity or a parameter differs. Add a new one only for a different failure mechanism.
2. Scaffold: `sdo-operator memory new detector <id> --class incident --playbook <playbook-id>` (health judge:
   `--class health`). It writes `manifest.yaml`, `detector.go`, and `detector_test.go`.
   Create the playbook first if it does not exist (see `sdo-playbook-authoring`).
3. Write the detector. Edit `manifest.yaml` and the `Spec()` in `detector.go` together: they must agree field for
   field. The API is in [SDK reference](references/sdk.md); a full example is in `assets/restart-loop/`.
4. Write tests: one case that fires (use different resource names than the incident that taught you the pattern)
   and near-miss cases that stay quiet. Use `sdktest.Snapshot` and consult the [replay reference](references/replay.md).
5. Run `go test` in the detector directory when your sandbox allows `go` and the checkout resolves the SDK
   module. If `go` is blocked or cannot resolve `sdo.dev/operator/sdk`, do not work around it: the next step
   builds, vets, and runs the same tests.
6. Run `sdo-operator memory check`. Fix every diagnostic by rule ID (see [validator rules](references/rules.md)) and
   repeat until it exits 0.

## Principles

- Detect the mechanism, not the incident: derive what a check depends on from observed objects (selectors, owner
  references, volume and env references), never from hard-coded names or the literal namespace. Use
  `DetectionContext.Namespace()` for resources that are absent.
- Refer to playbooks by logical ID in `Spec().Playbooks`, `Finding.Playbooks`, and `possiblePlaybooks`.
- Emit stable fingerprints and `ParameterBindings` whose keys match the playbook's placeholders in lower case
  (`target_resource` for `<TARGET_RESOURCE>`).
- Read [validator rules](references/rules.md) before changing a manifest; read [SDK reference](references/sdk.md) before using an SDK type.
