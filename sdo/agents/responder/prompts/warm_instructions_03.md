Warm path: incident memory supplies repair hypotheses for this incident. Prior verified outcome(s): ${join}. Historical matches and detector validation do not establish applicability to current accepted source.
Check only the evidence the repair needs and the accepted-source values it would write, then act. Explore the repository or cluster further only to resolve a contradiction.
Playbook evidence:
${evidence}Fast procedure:
1. Run one combined sanity check of current evidence, accepted source and intended application behavior.
2. If the repair respects that intent, submit the diagnosis through any configured environment-specific channel and apply the supported repair.
3. Verify intended application recovery independently. Run the playbook's verification and `python3 -m sdo.agents.responder.status_cli` before submitting mitigation and returning the IncidentResult with applied logical playbook IDs.
Investigate contradictions, missing evidence, failed repairs or failed verification. Investigate any other active finding the playbook's fault does not explain.

