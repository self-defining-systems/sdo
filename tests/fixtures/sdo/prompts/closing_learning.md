SDO confirmed users recovered; incident toy-incident closed as success.
Write one playbook per independent repair. Each must match and fix only its part.
Read /opt/sdo/skills/playbook-writing/SKILL.md. Edit only .sdo/playbooks/, .sdo/lib/ and .sdo/diagnostics/detectors/learned/.
Known changes by target:
1. Deployment/app/web: patch; plan refresh-config; reported: corrected config
2. cache entries: reported: removed stale entries
Answer in the `reflection` field and leave `incident_result` null.
For a distinct signal, add a learned detector naming its playbook, with a test and a near-miss test: /opt/sdo/skills/playbook-writing/detector.md.
Memory may name this application's commands and fields. Generalize over what varies, not away from the application. A program for just the mechanical part beats none; leave judgement to the README.
Check learned diagnoses and mitigations against recorded evidence before use. Use supported cause classes and plan forms.
One-line note: map repairs to playbooks or explain omissions.
