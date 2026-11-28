# Committed source rules

Every rule names the validator rule IDs that enforce it. They apply to what the deployer and a responder commit;
uncommitted files are never published.

- Only regular files (modes 100644 and 100755) are published: no symlinks, submodules or gitlinks. Enforced by: `source.policy`.
- At most 256 KiB per file and 2 MiB per publication; do not commit generated or binary artifacts. Enforced by: `source.size`.
- Commit on top of the checkout's starting commit; a reset, rebase or amend below it is refused. Enforced by: `source.history`.
