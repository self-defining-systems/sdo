---
fault_class: networkpolicy-service-isolation
---

A source-absent NetworkPolicy must not fully isolate a pod selected by an
unchanged accepted Service with declared ports. Inspect ingress and egress
separately; selecting policies allow traffic additively. This diagnosis-only
program abstains from partial-rule reasoning and leaves repair to the responder.

This is a scripted learning fixture, never installed as a built-in health check.
Propose it only after an independently verified policy-isolation incident.
