SDO confirmed users recovered; incident ${incident_id} closed as success.
Turn your fix into a playbook so SDO can apply it next time without you.
Read ${skills_root}/playbook-writing/SKILL.md, then work here; edit only .sdo/playbooks/, .sdo/lib/ and .sdo/diagnostics/detectors/incidents/.
${changes}
${branch_instruction}
If this cause has a signal of its own, also add an incident detector that names the playbook, with a test and a near-miss test: ${skills_root}/playbook-writing/detector.md.
Memory is for this application: a playbook may name its own tables, resources and commands. Generalize over what varies between incidents (which rows, which workload), not away from the application. A program for just the mechanical part beats none; leave judgement to the README.
Put in note one line: what the playbook does, or why nothing here is worth one.
