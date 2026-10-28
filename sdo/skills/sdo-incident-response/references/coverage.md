# Coverage and learned repairs

`playbook_coverage`: matches/bindings, mitigation, unexplained findings. Counts inform; never permission gates. Diagnosis-only matches supply evidence. Apply `can_mitigate` matches by playbook ID; check rollout, current status, late findings, preconditions and accepted-source values. Investigate contradictions. Keep verified stage receipts; never retry plan_not_executable unchanged; `ineffective` means the playbook already applied here and left those findings open. Partial playbooks: stages/`sdo mitigate` handle mechanics; README guides judgement.

The first message lists open findings only (`fingerprint: summary`; the rest counted in `findings_omitted`), coverage matches that explain them, and `stage_actions`: one line per automatic repair with its targets, verdict, writes and cleared count. Stage-cleared findings are done; start from what remains. Full facts, including `prior_stages` receipts and finding evidence, are in `details_file`.
