---
name: sdo-patching
description: Keep a change to a vendored or application file as a reasoned patch under .sdo/deployment/patches. Use before editing any file outside .sdo/, and when a patch fails to apply.
---

# Patches: the only way a source change persists

Agents write only `.sdo/`. Application files (everything outside `.sdo/`) are never published or committed. Go applies
the patch series in `.sdo/deployment/patches/` to the accepted repository to build the **effective tree**; every render,
build, drift read, upgrade and responder uses that tree. Your checkout already holds the series applied.

## When a patch is justified

1. Prefer an authored file: a manifest, Dockerfile or values file under `.sdo/deployment/` (see
   [sdo-deploy](../sdo-deploy/SKILL.md)) needs no patch and survives upstream changes untouched.
2. Patch only when an upstream file itself must differ: the application code, chart template or vendored manifest is
   wrong for this deployment or incident, and no values, overlay or authored file can express the fix.
3. A patch is repair history, not an experiment log. If a change did not fix the fault, revert it.
4. A patch that only adds an SDO-authored new file belongs in `manifests/`, `build/` or `values/` instead.

## Capture loop

1. Edit the app files in your checkout (scratch view): read, run and test freely.
2. Capture: `sdo-operator patch create --reason "<why>" [paths...]`. The launch sets `SDO_ACTOR` (deployer or
   responder) and, for a responder, `SDO_INCIDENT_ID`, so no flag is needed; `--owner` and `--incident ID` override.
3. The reason (40 to 2000 characters) states what fails without the patch: the symptom, the file, and why a values or
   authored file cannot fix it. A path list is not a reason. The tool numbers the patch `NNNN-<slug>.patch` and refuses
   to write one that does not reproduce your files exactly.
4. `sdo-operator patch list` shows the series (owner, paths, reason); `sdo-operator patch check` applies it to the base
   tree and names files no patch reproduces.
5. Preview what Go will deploy with `sdo-operator deployment render --plan FILE`; it excludes uncaptured edits.

Capture each independent fix as its own patch, one reason each. Later patches apply on top of earlier ones.

## Rejections

- `patch.uncaptured`: an app file differs from what the patches produce. Capture it with `patch create`, or undo
  your edit by hand (`git checkout` restores the unpatched base, which differs from your checkout's patched view). A deployer publish and a responder closure both
  wait on this; an experiment you leave behind blocks them.
- `patch.invalid`, `patch.owner`: see [rules](references/rules.md). Never hand-write headers; use `patch create`.
- `patch check` kinds: `conflict` (context no longer matches: re-create the patch against current files),
  `upstreamed` (the change is already in the base: delete your own patch), `missing_file` (the file is gone upstream).
- Only the owner changes or deletes a patch; responders add new patches and never rewrite earlier ones. To supersede
  a responder patch, add a later patch.

Never commit app files with git; a local commit of them changes nothing Go publishes.
