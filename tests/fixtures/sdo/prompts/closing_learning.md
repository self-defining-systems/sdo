SDO confirmed users recovered; incident toy-incident closed as success.
Turn your fix into a playbook so SDO can apply it next time without you.
Read /opt/sdo/skills/playbook-writing/SKILL.md, then work here; edit only .sdo/playbooks/ and .sdo/lib/.
Your recorded changes:
1. 03:04:05 api-exec Pod/db-1
2. 03:04:06 api-delete Pod/api-1
Answer in the `reflection` field and leave `incident_result` null.
Memory is for this application: a playbook may name its own tables, resources and commands. Generalize over what varies between incidents (which rows, which workload), not away from the application. A program for just the mechanical part beats none; leave judgement to the README.
Put in note one line: what the playbook does, or why nothing here is worth one.
