package bench

import "time"

// Bounds on what the manager waits for on a host. Vars so tests can shorten them.
var (
	probeBound    = 45 * time.Second
	hostCallBound = 2 * time.Minute
	hostGrace     = 3 * time.Minute
)
