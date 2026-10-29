## verify

read cached controller verification; optionally await a stable verdict

```text
sdo verify [-h] [--wait SECONDS]
```

Reads the controller's shared recovery proof: fresh health after the last repair, observed write effects, and the unchanged 15-second stability window. drift[] lists objects that still differ from accepted source and never decides healthy. After a healthy return, SDO may resume once for source differences when progress remains; terminal inventory and no-progress stops do not resume. restore them or explain the intended difference. Pending observation windows, including unproven synthetic scenarios, are not healthy. verification_blockers names incomplete detectors and reasons. SDO resumes once for these blockers, then stops any running blocker turn and records an unconfirmed outcome at the verification bound if they persist. Fresh deployment-rollout-pending findings name owned ReplicaSet/Pod blockers and old-template Pods, including after recorded actions; inspect their evidence before repeating workload writes. Wait on SDO's verification instead of sleeping. After closure, incident_verified is an informational warning on every response; reads and reports remain available, while mutations are rejected. Verified recovery fences writes and publishes the outcome without waiting for final narration; worker quiescence and outcome/reflection work continue during maintenance pauses.

- `--wait`: wait for stable verification, a new/reactivated finding, or this timeout (0: cached and instant)

Key JSON fields: `healthy, unresolved, findings[], detector_states[], verification_blockers[{detector_id,reason}], drift[], drift_events[]`.
