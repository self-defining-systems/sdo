---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

Habits, not a procedure.

- Start from what failed: user operations that fail (and which pass), or what stopped healing. Other alerts are
  clues; the opening alert can be a side effect, so confirm the symptom is real now. Run a failing operation
  yourself and read the exact response.
- Survey widely: `sdo detect`, `sdo changes`, `sdo timeline`, logs, the data the operations touch. An unreadable or
  unexplained row near onset outranks a readable one off the failing path.
- Before acting, hold two to four explanations that differ in component, layer or timing and predict different
  observations, each with a check that could prove it wrong. Drop what evidence contradicts; an unexplained
  observation is a new candidate.
- For longer checks spawn a sub-agent (fresh-context helper, about 2 minutes) per hypothesis. It reads
  `sdo-investigator/SKILL.md` first; give it the hypothesis, what you expect if true or false, what led to it,
  what is ruled out. It only reads; you alone change things and check the result. Keep its "How to diagnose"
  note: your closing turn needs it.
- Read what does not fit first. A change that left the failure unchanged counts against its hypothesis.
- Done when what failed passes and keeps passing; else escalate with what you found and ruled out.
