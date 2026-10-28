## Evidence and replay fixtures

Learn from recorded responder actions and live observations already present in the incident session.
Build a fixture from the available snapshot and those observations; a missing controller open/fixed
capture must not prevent learning. Do not invent successful primitive replies. Supply root_cause_program_ids
and object bindings for the confirmed cause. Keep originating incident attribution in labels.provenance.incident_id;
snapshot digests and a protected capture mount are not required. Include accepted.json when the executed
mitigation reads accepted source. For replay, open.json represents the fault and recordings.json contains
its observed primitive replies; fixed.json and healthy.json remain readable context, not repair proof.

For every cause with a recorded repair on a closure verified healthy, save diagnose and mitigate programs, a README describing fault
handling, and a detector that references its playbook. One detector may reference several playbooks. A missing piece is
recorded as a coverage gap.
Replay observes incident detectors, binding selection, diagnosis and mitigation on recorded states. Every outcome, including unavailable evidence or a persistent Match, is information only; it never blocks saving.
Compilation is a save-time check; the repeat path supplies advisory observations. Rename, healthy-peer and controller
capture-provenance checks are offline analysis, not publication prerequisites. Preserve accepted fixture bytes.

