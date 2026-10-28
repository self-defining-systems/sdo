The controller has committed its authoritative incident outcome.
Reflect using the same incident context.
Memory exists so later incidents are fixed faster and cheaper. First compare applied playbooks, matching detectors and unchanged source with this incident. If existing playbooks match well and nothing is new, return no_change with a short reason and finish. Every recorded hand repair needs an executable typed SDK plan or RunScript step; README holds only judgement and ordering, and sdk_gaps name actually unavailable observations or effects, never work not encoded yet. Update or retire applied playbooks that failed, left findings unresolved or needed later hand repair.

Read across all accepted memory as needed; incident-linked entries come first and full files remain readable in the checkout. Make the smallest useful addition. Keep revisions that applied cleanly unless this incident shows failure or incompleteness; add or narrowly edit, never rewrite working steps. Preserve verified partial steps. The authoring guide starts at /opt/sdo/skills/sdo-playbook-authoring/SKILL.md.

Committed outcomes are authoritative; memory is fallible advice and accepted source expresses intent. Use recorded actions as repair evidence; never infer from truncated output. Unavailable observations remain Unknown. Replay is informational. Edit only authorized operational-memory files in this isolated checkout. Protect source, outcomes, goal, architecture, corpus and trust. Validate changes and resolve blocking diagnostics. Return updated with changed playbook IDs for a validated nonempty diff. The broker checks and publishes. Answer in the `reflection` field and leave `incident_result` null.

Full: <context file>
Summary (partial):
{"causes": [{"resources": [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config-0"}], "items": [{"disposition": "question", "question": "No opening detector observed this recorded configuration repair. Propose diagnosis, mitigation and detector coverage from its captured before/after transition.", "reason": "unbound_write", "resources": [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config-0"}]}]}], "unassigned": [{"disposition": "question", "question": "A repaired resource is outside confirmed cause groups. Review the write without a cause.", "reason": "unbound_write", "resources": [{"api_version": "v1", "kind": "Service", "namespace": "toy", "name": "toy-service"}]}]}Outcome commit: dddddddddddddddddddddddddddddddddddddddd
Operational memory snapshot:
{
  "accepted_commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "detectors": [
    {
      "id": "detector-0",
      "description": "Toy service port mismatch",
      "playbook_ids": [
        "repair-0"
      ]
    }
  ],
  "playbooks": [
    {
      "id": "repair-0",
      "description": "Repair",
      "script_names": [
        "diagnose.go"
      ]
    }
  ],
  "full_text": "Open only relevant accepted files in the proposal checkout.",
  "full_index_path": "<context file>"
}

Full outcome and memory index: referenced files; open only what you need.
Current outcome:
Full: <context file>
Summary (partial):
{"incident_id": "toy-incident", "classification": "success", "findings": [{"detector_id": "detector-0", "rule_id": "unavailable", "status": "active", "severity": "critical", "summary": "component-0 is unavailable", "evidence": "Observed component-0 has no ready endpoints.", "primary_resource": {"api_version": "v1", "kind": "Service", "namespace": "toy", "name": "component-0"}, "playbook_ids": ["repair-0"], "fingerprint": "fingerprint-0", "related_resources": [], "parameter_bindings": {}}], "confirmed_root_causes": [{"summary": "unexplained drift; restore accepted-source differences unless evidence shows intended drift, then verify", "resources": [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config-0"}], "explained_detectors": ["drift.unexplained"], "evidence": [], "static_context": []}], "repair_actions": [{"action_id": "sdo-program-toy-mitigation-0-r1", "kind": "program-plan", "target": "ConfigMap/toy-config-0", "resources": [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config-0"}], "summary": "applied program plan action:ReconcileFromSource", "details": "controller confirmed 1 applied resource effects in toy-mitigation-0-r1", "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:00:01Z", "success": true, "reversible": false, "failure_reason": "", "owners": [], "script_digest": ""}, {"action_id": "sdo-program-toy-mitigation-1-r1", "kind": "program-plan", "target": "ConfigMap/toy-config-1", "resources": [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config-1"}], "summary": "applied program plan action:ReconcileFromSource", "details": "controller confirmed 1 applied resource effects in toy-mitigation-1-r1", "started_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:00:01Z", "success": true, "reversible": false, "failure_reason": "", "owners": [], "script_digest": ""}], "plan_applications": [{"plan": {"actions": [{"reconcile_from_source": {"ref": {"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config"}}}], "rationale": ""}, "applied": true, "verification": {"result": "RESULT_MATCH", "evidence": [], "question": ""}, "application_id": "toy-mitigation-0-r1", "program_ids": ["action:ReconcileFromSource"], "actions_applied": [{"reconcile_from_source": {"ref": {"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config"}}}], "verification_completed": true, "action_reports": [{"status": "applied", "webhook_ca_digest": "", "action_index": 0, "values": [], "external_effect_json": "", "kubernetes_effect_json": "", "recording_error": "", "job_status_json": "", "script_result_json": "", "failure_reason": ""}], "dry_run": false, "error": "", "rolled_back": false, "rollback_error": "", "verification_passed": false, "object_effects": [], "final_verification_basis": ""}, {"applied": true, "application_id": "toy-mitigation-1-r1", "program_ids": ["action:ReconcileFromSource"], "verification_completed": true, "dry_run": false, "error": "", "rolled_back": false, "rollback_error": "", "verification_passed": false, "object_effects": [], "final_verification_basis": "", "fields_omitted": 4}], "undetected_causes": [], "full_current_outcome_path": "<context file>", "findings_omitted": 1, "repair_actions_omitted": 1}

Outcome history (prior incidents, compact): none

Acknowledged at verified onboarding (identity only; content unverified):
Full: <context file>
Summary (partial):
{"namespace": "toy", "accepted_commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "objects": [{"api_version": "v1", "kind": "ConfigMap", "namespace": "toy", "name": "toy-config-0"}], "objects_omitted": 25, "environment_objects": [{"ref": {"api_version": "v1", "kind": "Service", "namespace": "toy", "name": "toy-service-0"}, "digest": "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}, {"digest": "sha256:eeeeeeeeeeeeeeeeeeeeeeeee…", "fields_omitted": 1}], "environment_objects_omitted": 24}
