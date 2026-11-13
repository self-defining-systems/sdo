---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

A reasoning loop, not a procedure. Move between the parts as the evidence asks.

- **Observe.** What fails, which operations, since when, and what still works. The opening alert can be a side
  effect, so run a failing operation and read the exact response.
- **Localize.** The graph (`.sdo/arch.md`, `sdo map`) is a prior, not proof. A cause usually sits on or upstream
  of every failing operation's path; `sdo detect` lists those candidate nodes with evidence. A passing operation
  shows a node is alive, not that its data or code for the failing one is right; shared nodes stay candidates.
- **Explain the timing.** Something changed near onset on a candidate (config, code, data, a dependency, load),
  or a latent condition was triggered. Changes there are the strongest leads. A candidate with no observed
  state is a gap, not a clean bill.
- **Hypothesize.** Hold two to four explanations, each a node and a mechanism, from different layers and path
  positions (entry, dependency, data holder). An unexplained observation is a new candidate.
- **Discriminate.** Pick the check whose outcome differs most between them, ideally one that could refute.
  With more than one plausible explanation, spawn one sub-agent per hypothesis, in parallel and early, while
  you keep surveying; skip that only when a single quick read settles it. Each reads
  `sdo-investigator/SKILL.md`; say what you expect either way and what is ruled out. It only reads. Keep its
  "How to diagnose" note for closing.
- **Update.** Drop what is refuted. Stop when one explanation covers the failing, the passing and the timing.
- **Fix as a test.** You alone change things and check the result. Predict the effect; a change that left the
  failure as is counts against its hypothesis. Done when what failed passes and keeps passing; else escalate
  with what you found and excluded.
