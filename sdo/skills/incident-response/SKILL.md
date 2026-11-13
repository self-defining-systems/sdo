---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

Habits, not a procedure.

- Start from what failed: user operations that fail (and which pass), or what stopped healing. Other alerts are
  clues; confirm the symptom is real now. Run a failing operation and read the exact response.
- Survey: `sdo detect`, `sdo changes`, `sdo timeline`, logs, the data the operations touch. An unexplained
  row near onset outranks a readable one off that path.
- Reason over `.sdo/arch.md`: trace each failing operation's path through components, edges and data holders.
  Prefer causes on that path, not the passing operations'; an upstream dependency or data holder can break what
  is downstream. Hypothesize from different points on the path, including the data it reads and writes.
- Before acting, hold two to four explanations differing in component, layer or timing, each with a check that
  could prove it wrong. Drop what evidence contradicts.
- For longer checks spawn a sub-agent (fresh-context, ~2 min) per hypothesis. It reads
  `sdo-investigator/SKILL.md` first; tell it the hypothesis, what you expect either way, what led to it, what's
  ruled out. It only reads; you alone change things. Keep its "How to diagnose" note for closing.
- Read what does not fit first. A change that left the failure as is counts against its hypothesis.
- Done when what failed passes and keeps passing; else escalate with what you found and excluded.
