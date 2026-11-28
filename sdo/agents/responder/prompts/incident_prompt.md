Incident ${incident_id} in namespace ${namespace}: investigate and fix it. There may be several root causes that mask each other or arrive later. Reason from first principles: test each hypothesis; correlate failing user tests with the components on their path. You have kubectl, the shell and the `sdo` command (skills at ${skills_root}). After a change, wait for its effect, not a fixed time. On detector alerts, SDO ends work on recovery; until then, return only to give up. Never print a credential value.
${symptom}
${recent_changes}
${branch_instruction}