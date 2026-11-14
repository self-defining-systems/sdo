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
  After the first look, name your lead explanation and the runner-up. Unless a failing response or recorded change
  already states the cause, launch a read-only sub-agent for the runner-up (and each other open explanation, about
  three at most; they read `sdo-investigator/SKILL.md`) before you act on the lead, and keep working. Launch also
  during a slow action, and when a change left the failure as is or an observation is unexplained. You alone
  change things. Brief each with the symptom users see failing and one explanation; ask for evidence, or
  "inconclusive" if it cannot reproduce. Weigh the reports; do not return while any are pending. Keep their
  "How to diagnose" notes for closing. SDO may start investigators: read `sdo investigations`, weigh their
  reports, above all "observations that don't fit".
- **Update.** Drop what is refuted. Stop when one explanation covers the failing, the passing and the timing.
- **Fix as a test.** You alone change things and check the result. Predict the effect; a change that left the
  failure as is counts against its hypothesis. Done when what failed passes and keeps passing; else escalate
  with what you found and excluded.
