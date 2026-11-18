SDO confirmed users recovered; incident toy-incident closed as success.
Turn your fix into a playbook so SDO can apply it next time without you.
Read /opt/sdo/skills/playbook-writing/SKILL.md, then work here; edit only .sdo/playbooks/, .sdo/lib/ and .sdo/diagnostics/detectors/incidents/.
Your recorded changes:
1. 03:04:05 api-exec Pod/db-1
2. 03:04:06 api-delete Pod/api-1
Answer in the `reflection` field and leave `incident_result` null.
If this cause has a signal of its own, also add an incident detector that names the playbook, with a test and a near-miss test: /opt/sdo/skills/playbook-writing/detector.md.
Memory is for this application: a playbook may name its own tables, resources and commands. Generalize over what varies between incidents (which rows, which workload), not away from the application. A program for just the mechanical part beats none; leave judgement to the README.
Put in note one line: what the playbook does, or why nothing here is worth one.
