## verify

read cached controller verification; optionally await a stable verdict

```text
sdo verify [-h] [--workload WORKLOAD] [--scenario SCENARIO] [--wait SECONDS]
```

Reads the controller's shared recovery proof: fresh health after the last repair, observed write effects, and the unchanged 15-second stability window. drift[] lists objects that still differ from accepted source and never decides healthy. verification_status=awaiting_responder_completion: finish with evidence; proof follows your result. Explain remaining source differences; SDO may resume once when progress remains. Pending observation windows, including unproven synthetic scenarios, are not healthy. Settling traffic SLOs wait. verification_blockers names incomplete detectors and reasons. SDO resumes once for these blockers, then stops any running blocker turn and records an unconfirmed outcome at the verification bound if they persist. Fresh deployment-rollout-pending findings name owned ReplicaSet/Pod blockers and old-template Pods, including after recorded actions; inspect their evidence before repeating workload writes. Wait on SDO's verification instead of sleeping. After closure, incident_verified is an informational warning on every response; reads and reports remain available, while mutations are rejected. Verified recovery fences writes and publishes the outcome without waiting for final narration; worker quiescence and outcome/reflection work continue during maintenance pauses.

- `--workload`: traffic workload to burst (default: the health judge's verify workload)
- `--scenario`: traffic scenario ID to burst; repeatable
- `--wait`: wait for stable verification, a new/reactivated finding, or this timeout (0: cached and instant)

Key JSON fields: `healthy, verification_status, findings[], detector_states[], verification_blockers[{detector_id,reason}], drift[], drift_events[]`.
