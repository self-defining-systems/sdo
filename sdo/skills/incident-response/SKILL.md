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
  positions (entry, dependency, data holder). When forming explanations, ask whether the data or state the failing operation needs is right, not only whether components are up. An unexplained observation is a new candidate.
- **Discriminate.** Pick the check whose outcome differs most between them, ideally one that could refute.
  After the first look, name your lead explanation and the runner-up (an unexplained finding on the failing path is a
  first-choice runner-up). Check them yourself. You may start read-only sub-agents (they read
  `sdo-investigator/SKILL.md`) to test explanations in parallel when that saves time; brief each with the symptom
  users see failing and one explanation, and ask for evidence, or "inconclusive" if it cannot reproduce. You alone
  change things. Combine facts across reports: data in one component that does not match another's is a finding
  even when each looks fine alone. Keep their "How to diagnose" notes for closing. SDO may start investigators: read `sdo investigations`, weigh their reports, above all
  "observations that don't fit".
- **Update.** Drop what is refuted. Stop when one explanation covers the failing, the passing and the timing.
- **Fix as a test.** You alone change things; each change is an experiment. Before it, say which failing
  operations it should fix and through which part of the map, then check exactly those. Effects take time: know
  the expected delay; what happened before it was not caused by the change. Judge after the delay. A change that left the failure as is counts against its hypothesis.
  Done when what failed passes and keeps passing; SDO ends your session when recovery holds. Return only to
  give up (with what you found and excluded), never while users are still failing.
- Wait on events, not a clock: `sdo detect --wait SECONDS` returns when an alert or finding changes. While your
  sub-agents run, keep them in the foreground. Ending your turn submits your result, which is for giving up.
- **Record findings.** The result's `findings` are required, also when you give up: what you saw and where, what
  you ruled out and why (and what you did not check), never the probable cause. A later incident on the same
  symptoms reads them as unverified notes.
- **Record changes.** `agent_changes`: each change you made, what, where, and when (RFC 3339); empty if none.
