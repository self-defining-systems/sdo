# Remote shared-memory synchronization

For branch identity and row interpretation, read [concurrent shared-memory lanes](fastloop-results.md#concurrent-shared-memory-lanes).

Offline catch-up merges only disjoint authored paths. The playbook index is derived from all playbook READMEs: synchronization regenerates it from the combined tree, as local proposal replay does. Both lanes changing only that index is never an authored overlap. A conflict in an actual playbook still preserves local history on the remote unmerged branch.

Bound incident workspaces use their opening accepted revision without upstream synchronization. Ordinary lifecycle proposal creation retains synchronization; later publication rebases and validates against current accepted memory.
