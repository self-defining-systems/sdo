# Responder workflow

Source: the agreed LE workflow, implemented by
`sdo/agents/responder/codex.py`, the installed incident-response skill and the
Go responder tool transport/controller. Check the fields and tools available
at the rollout's revision; report unavailable steps instead of assuming them.

1. Read the coverage view: matched learned playbooks, explained findings and
   findings that remain unexplained.
2. After a quick live check, apply matched mitigations first, one `sdo mitigate`
   call per selected mitigation; read each result before continuing.
3. Repair the remaining findings using recorded actions and commit source changes.
4. Self-check with `sdo detect` on affected findings, then `sdo verify`.
5. Undo only when evidence warrants `sdo rollback`; the controller does not
   automatically undo the responder's repairs.
6. On a continuation, work only on still-broken findings and use already-done
   receipts to avoid repeating successful work.

A matched program's success/failure counts inform the responder; they do not
replace live diagnosis. Check the original incident session and every resumed
turn for omitted coverage, repeated fixes, discovery overhead and final checks.
