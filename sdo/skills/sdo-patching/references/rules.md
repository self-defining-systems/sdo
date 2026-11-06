# Patch rules

Every rule names the validator rule IDs that enforce it.

- A patch file is `NNNN-<slug>.patch` (four digits, lowercase slug) with a strict header (`SDO-Patch`, `Owner`, `Reason`,
  `Incident` for responders, `Upstream-Base`, optional `Upstream-Fix`), then a git diff of text files outside `.sdo/`.
  The reason is 40 to 2000 characters, numbers are unique, and a series has at most 64 patches (256 KiB each, 2 MiB
  total). No binary hunks, symlinks, submodules or paths with spaces. Create patches with
  `sdo-operator patch create`. Enforced by: `patch.invalid`.
- The `Owner` header equals the writing actor (`deployer` or `responder`), and only the owner modifies or deletes a
  patch. Responders only add patches. Enforced by: `patch.owner`.
- An app file differing from what the series produces is rejected at publish. Capture it or revert it. Enforced by:
  `patch.uncaptured`.
