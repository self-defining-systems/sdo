# Coverage and learned repairs

`playbook_coverage`: matches/bindings, mitigation, unexplained findings. Counts inform; never permission gates. Diagnosis-only matches supply evidence. Apply `can_mitigate` matches by playbook ID; check rollout, current status, late findings, preconditions and accepted-source values. Investigate contradictions. Keep verified stage receipts; never retry plan_not_executable unchanged; `ineffective` means the playbook already applied here and left those findings open. Partial playbooks: stages/`sdo mitigate` handle mechanics; README guides judgement.

Ready matches include named object/scalar bindings and their mitigation selection. Use that supplied
evidence to act on useful independent matches together; spend further investigation on contradictions or
missing preconditions. Stage actions have already happened; begin with the remaining work.

An applied source repair with unavailable readback remains pending and unverified; inspect its application
before repeating the write. A nonmatching or broken learned program is evidence to investigate the live cause
and use a direct recorded repair. Avoid spending the incident rewriting a generic program; reflection saves
the verified repair afterward.

The first message lists open findings only (`fingerprint: summary`; the rest counted in `findings_omitted`), coverage matches that explain them, and `stage_actions`: one line per automatic repair with the objects it wrote and what it left: `cleared`; `reverted-ineffective` (it did not help, so SDO restored those objects and the original fault is in place); `kept-partial` (it fixed part; keep it); `kept-irreversible` (a delete, exec or restart stays). Stage-cleared findings are done; start from what remains. Full facts, including `prior_stages` receipts and finding evidence, are in `details_file`.
