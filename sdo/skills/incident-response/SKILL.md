---
name: incident-response
description: A general method for finding and removing what is hurting an application's users.
---

# Incident response

A reasoning loop, not a procedure. Move between the parts as the evidence asks.

- **Observe.** What fails, which operations, since when, and what still works. The opening alert can be a side
  effect, so run the operation that is failing (not a neighbour) and read the exact response.
- **Localize.** The graph (`.sdo/arch.md`, `sdo map`) is a prior, not proof. A cause usually sits on or upstream
  of every failing operation's path; `sdo detect` lists those candidate nodes with evidence. A passing operation
  shows a node is alive, not that its data or code for the failing one is right; shared nodes stay candidates.
- **Explain the timing.** Something changed near onset on a candidate (config, code, data, a dependency, load),
  or a latent condition was triggered. Changes there are the strongest leads. A candidate with no observed
  state is a gap, not a clean bill. The environment moves on its own: before crediting or blaming your change,
  check what else changed in the same window and whether it is on the same path. An unexplained change or
  finding on a failing path stays an open explanation until checked.
- **Hypothesize.** Hold two to four explanations, each a node and a mechanism, from different layers and path
  positions (entry, dependency, data holder). An unexplained observation is a new candidate.
- **Discriminate.** Pick the check whose outcome differs most between them, ideally one that could refute.
  After the first look, name your lead explanation and the runner-up (an unexplained finding on the failing path is a
  first-choice runner-up). Unless a failing response or recorded change already states the cause, launch a read-only sub-agent for the runner-up (one per explanation still open, only if
  its answer could change your next step; they read `sdo-investigator/SKILL.md`) before you act on the lead, and
  keep working. Launch also during a slow action, and when a change left the failure as is or an
  observation is unexplained. You alone change things. Brief each with the symptom users see failing and one explanation; ask for evidence, or
  "inconclusive" if it cannot reproduce. Weigh the reports; do not return while any are pending. Keep their
  "How to diagnose" notes for closing. SDO may start investigators: read `sdo investigations`, weigh their
  reports, above all "observations that don't fit".
- **Update.** Drop what is refuted. Stop when one explanation covers the failing, the passing and the timing.
- **Fix as a test.** You alone change things; each change is an experiment. Before it, say which failing
  operations it should fix and through which part of the map, then check exactly those. Effects take time: know
  the expected delay; what happened before it was not caused by the change. Judge after the delay, waiting on
  `sdo detect --wait`, not a fixed sleep. A change that left the failure as is counts against its hypothesis.
  Done when what failed passes and keeps passing; SDO ends your session when recovery holds. Return only to
  give up (with what you found and excluded), never while users are still failing.
