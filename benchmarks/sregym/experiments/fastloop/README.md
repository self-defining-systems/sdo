The smoke stream names the two original mixed-fault problems (i06 and i10). The explicit stream injects their component faults with `inject_composite`, using the single-fault IDs from the playbook trial's repeat config.

TODO: The SREGym fork checked out in this worktree still needs the `mf__hotel_reservation__n2__s1773744812__v_` and `mf__hotel_reservation__n2__s1085951852__v_` composite specs and the hotel `target_port` variants for `reservation` and `geo`. Until those are registered, run `pbprog_explicit.json` after the target-port variants land.
