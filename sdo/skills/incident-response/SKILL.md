---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

Habits, not a procedure.

- Start from what failed: user operations that fail (and which pass), or what stopped healing. Other alerts are
  clues; the opening alert can be a side effect, so confirm the symptom is real now. Run a failing operation and
  read the exact response.
- Survey: `sdo detect`, `sdo changes`, `sdo timeline`, logs, the data the operations touch. An unexplained
  row near onset outranks a readable one off that path.
- Reason over the app map: `sdo map diff` shows what only failing paths touch; `sdo map path OP` and
  `sdo map near C` show a path and neighbours. Prefer causes on the failing path; an upstream dependency or data
  holder can break what is downstream. Hypothesize along the path, including the data it reads and writes.
- Before acting, hold two to four explanations differing in component, layer or timing, each with a check that
  could refute it. Drop what evidence refutes.
- For longer checks spawn a sub-agent per hypothesis. It reads `sdo-investigator/SKILL.md` first; tell it the
  hypothesis, what you expect either way, what is ruled out. It only reads. Keep its "How to diagnose" note for
  closing.
- Read what does not fit first: an unexplained observation is a new candidate; a change that left the failure
  as is counts against its hypothesis.
- Done when what failed passes and keeps passing; else escalate with findings.
