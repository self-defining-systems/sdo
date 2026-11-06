---
name: sdo-source-advance
description: DRAFT. Resolve an upstream advance that Go held because a patch no longer applies. Not yet assigned to an agent role; no task launches it today.
---

# Source advance holds (draft)

Status: draft. No agent task runs this skill yet; holds wait for a human or a newer upstream revision. This page
fixes the vocabulary so the later agent route (and the human reading a hold) share it.

Go follows the upstream repository. For each new revision it replaces the application files with upstream's, then
re-applies the patch series ([sdo-patching](../sdo-patching/SKILL.md)). A patch that fails to apply holds the advance
before any cluster effect; nothing is deployed and the running version stays. The hold names the patch, its owner,
its paths, the rejecting detail and up to five upstream commits touching those paths.

## Holds

| Hold | Meaning | Resolution |
|---|---|---|
| `patch_conflict` | context no longer matches (`conflict`) or the file is gone (`missing_file`) | read the upstream commits named; re-create the patch against the new files, or drop it if upstream fixed the problem differently |
| `patch_upstreamed` | the change is already in upstream | delete the patch (its owner does); retiring it needs no replacement |
| `patch_invalid` | a stored patch is malformed or forbidden (should not happen) | human inspection |

## Resolving a conflict (target flow)

1. Read the held patch's `Reason` header: it says what the patch fixes and why. The intent is the contract, not the diff.
2. Read the named upstream commits and the new file contents. If upstream now has the fix, retire the patch.
3. Otherwise edit the files at the new revision to restore the intent, then `sdo-operator patch create` a replacement
   with a reason that carries over the old one, and delete the stale patch. Only the owner deletes or changes a
   patch: deployer patches by the deployer route; responder patches are refreshed by Go or a human, never rewritten
   by a deployer.
4. `sdo-operator patch check` must be clean and `sdo-operator deployment render` must succeed before you finish.
