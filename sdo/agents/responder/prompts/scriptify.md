SDO confirmed users recovered; incident ${incident_id} closed as success.
Write a playbook for this repair.
Read ${skills_root}/playbook-writing/SKILL.md. Edit only .sdo/playbooks/, .sdo/lib/ and .sdo/diagnostics/detectors/learned/.
${changes}
${branch_instruction}
If this cause has a signal of its own, also add a learned detector that names the playbook, with a test and a near-miss test: ${skills_root}/playbook-writing/detector.md.
Memory is for this application: a playbook may name its own tables, resources and commands. Generalize over what varies between incidents, not away from the application. Encode mechanical steps. Keep judgement in the README.
Check the diagnosis and mitigation against recorded evidence. Use supported cause classes and plan forms.
Put in note one line: what the playbook does, or why nothing here is worth one.
