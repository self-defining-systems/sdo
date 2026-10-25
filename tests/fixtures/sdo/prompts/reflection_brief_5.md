Incident brief (built by the broker from the verified closure; about 894 tokens):
## Live evidence at dispatch (closure request)
Application toy in namespace toy; source commit bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb, deployed commit cccccccccccccccccccccccccccccccccccccccc.
Detected 1970-01-01T00:00:00+00:00, dispatched 1970-01-01T00:00:00+00:00, responder completed 1970-01-01T00:00:00+00:00, health verified 1970-01-01T00:00:00+00:00.
Findings:
Findings:
- [critical] detector-0 / unavailable (active): component-0 is unavailable
  primary: Service toy/component-0 (v1)
  fingerprint: fingerprint-0
  playbooks: repair-0
  evidence (verbatim):
```
Observed component-0 has no ready endpoints.
```
- [critical] detector-1 / unavailable (active): component-1 is unavailable
  primary: Service toy/component-1 (v1)
  fingerprint: fingerprint-1
  playbooks: repair-1
  evidence (verbatim):
```
Observed component-1 has no ready endpoints.
```
- [critical] detector-2 / unavailable (active): component-2 is unavailable
  primary: Service toy/component-2 (v1)
  fingerprint: fingerprint-2
  playbooks: repair-2
  evidence (verbatim):
```
Observed component-2 has no ready endpoints.
```
- [critical] detector-3 / unavailable (active): component-3 is unavailable
  primary: Service toy/component-3 (v1)
  fingerprint: fingerprint-3
  playbooks: repair-3
  evidence (verbatim):
```
Observed component-3 has no ready endpoints.
```
- [critical] detector-4 / unavailable: component-4 is unavailable (Service toy/component-4 (v1); evidence omitted)
Detector history before dispatch:
- detector-0: 1 evaluation(s) 1970-01-01T00:00:00+00:00 .. 1970-01-01T00:00:00+00:00; latest statuses: firing
- detector-1: 1 evaluation(s) 1970-01-01T00:00:00+00:00 .. 1970-01-01T00:00:00+00:00; latest statuses: firing
- detector-2: 1 evaluation(s) 1970-01-01T00:00:00+00:00 .. 1970-01-01T00:00:00+00:00; latest statuses: firing
- detector-3: 1 evaluation(s) 1970-01-01T00:00:00+00:00 .. 1970-01-01T00:00:00+00:00; latest statuses: firing
- detector-4: 1 evaluation(s) 1970-01-01T00:00:00+00:00 .. 1970-01-01T00:00:00+00:00; latest statuses: firing
Surfaced playbooks: repair-0
Post-response health detector states: none recorded
Post-response incident detector states: none recorded

## Responder structured result
No responder result was recorded (dispatch error: none).

## Existing operational memory (trusted logical snapshot)
Health detectors (judge-owned, never edit): none
- detector-0 (watches: v1/Service, firing: 0, playbooks: ['repair-0'], originatingIncident: prior-toy) [raised a finding in this incident]
  Description: Toy service port mismatch
```
return service.Port != intended.Port
```
  Reads Kubernetes Events: no
Playbook index:
```
# Playbooks
repair-0

```
Existing playbooks: repair-0
Excerpt of playbook repair-0:
```
手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手手
[... 8300 characters omitted ...]
```

## Responder shell commands
The responder's shell commands are not available for this session.
End of incident brief.
