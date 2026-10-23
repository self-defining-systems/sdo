---
name: sdo-detector-authoring
description: Author or refine deterministic Go health and incident detectors from observed resource state. Covers SDK discovery, matching and near-miss tests, and independently validated memory edits.
---

# Authoring a detector

A detector reads a Kubernetes snapshot and returns findings. Keep it deterministic: no model, environment,
clock, network access or benchmark data. Measure ages with `sdk.ObservationTime(ctx)`, never `time.Now()`. The installed `sdo-memory` skill explains ownership and logical IDs.

1. Compare the confirmed mechanism with accepted detectors. Widen one when only resource identity or a
   parameter differs; add one only for a different mechanism. The installed `service-endpoints` and
   `workload-health` detectors already cover endpoint readiness and generic pod/Deployment health.
2. Before implementing, read the [generated SDK](references/sdk.md). Keep the detector's manifest and Spec
   identical, including watches, persistence, playbook IDs and accepted provenance.
3. Derive relationships from observed objects. Use the context namespace for absent resources and emit
   stable fingerprints and role bindings. Never hard-code incident names, ports or images.
4. Prove a matching case and near misses with the [generated replay API](references/replay.md).
   The [restart-loop source](assets/restart-loop/detector.go) and
   [tests](assets/restart-loop/detector_test.go) show the current detector interface.
5. Validate final proposal files through the memory check. The broker repeats this check independently.

Read [validator rules](references/rules.md) before changing a manifest or when a diagnostic names a detector rule.
