---
schema_version: 1
owner: responder
fault_class: restart-loop
originating_incident: example-incident
---
# Restart loop

Symptoms: a container of <TARGET_RESOURCE> keeps restarting and waits in CrashLoopBackOff.

Diagnosis: read the previous container logs and the last termination reason of <TARGET_RESOURCE>. Run
`restart-reason.sh` to print both. Confirm the cause (bad configuration, missing dependency, failing probe) from
the evidence rather than from the pod name.

Repair: fix the cause in the source manifest of the owning workload, redeploy from source, and wait for the
replacement pods to become ready. Do not delete pods to hide the loop.

Verify: the restart count of the replacement pods stays flat for one full probe interval and the application
health objective passes.
