---
name: audit-agent-instructions
description: Audit the instructions an operational agent actually receives, using rendered rollout text, skill reads, source attribution and observed behavior. Use when reviewing prompt or skill quality or investigating instruction failures.
---

# Audit agent instructions

1. Select a real rollout and record its role, code revision, accepted memory revision and session mode. Audit the rendered text received by the model, including continuation turns, rather than inferring it from source. Keep raw artifacts private and omit secrets from findings.
2. Run `uv run python .agents/skills/audit-agent-instructions/scripts/extract_prompt.py <rollout.jsonl>` to separate developer/harness text, our instructions, request data and tool calls, with character counts, attempted skill reads and `--help` counts. Use `--include-text` only for a private local inspection; confirm inferred boundaries and successful reads against the original records.
3. Map each rendered paragraph to its source file and line: template, skill, reference, output-schema description or request producer. Preserve the source revision; classify unattributed paragraphs as findings. Trace harness text separately from repository-owned instructions.
4. Compare against the role workflow: [deployer](references/deployer.md), [architecture](references/architecture.md), [health judge](references/health-judge.md), [responder](references/responder.md) or [reflection](references/reflection.md). Every needed step appears once, in one place; flag missing, duplicate, contradictory or buried steps, including conflicts across turns.
5. Test each rule's value: cap reminders of Go-enforced rules at one line, replace command recipes with principles and delete dead rules. Read `git blame` and the introducing change before cutting a rule; retain its purpose if the failure still applies.
6. Check placement: prompts hold role, goal, hard limits, skill pointers and task data; skills hold workflow and the tool table; references hold material needed at one step; output-schema descriptions hold output rules. Production prompt text belongs only in Markdown/template files, never inline in Python or Go source. Follow [budgets](references/budgets.md) without copying limits into this skill.
7. Run `uv run python .agents/skills/audit-agent-instructions/scripts/check_names.py` and its unit test. Check command names, typed request fields, skill paths and stale role/tool names; inspect dynamically constructed names and unqualified field prose manually. During the C5 migration the checker includes current Python prompt sources as well as template directories.
8. Prove skill loading: the image contains the skill, the session installs and indexes it, its reference paths resolve inside that image and the rollout shows the relevant files successfully read. A pointer or installed file alone is insufficient.
9. Inspect behavior: discovery turns (`--help`, repeated capability searches), skipped or reordered steps, failed tool calls and continuation handling. Separate a content defect from missing input, transport failure or unenforced installation; cite a minimal sanitized event span. Production instructions contain no benchmark/grader wording; findings contain no secrets.
10. Deliver the report table below and a concrete diff. Validate changed wiring with the scripted end-to-end gate in the testing guide, then one authorized live rollout that exercises the changed rule. Record revision, commands and observed behavior for each; if live execution is unavailable, explicitly leave that validation pending rather than claiming it passed.

| Rendered paragraph/event | Source file:line | Workflow step | Finding and evidence | Proposed change | Scripted/live validation |
| --- | --- | --- | --- | --- | --- |

The scripts are triage tools: character counts are Unicode code points, not tokens; user-message splitting uses explicit request headings, and tool calls establish attempted reads. Verify ambiguous boundaries and tool results manually before attributing behavior.
