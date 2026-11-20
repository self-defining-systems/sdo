// Package deploy carries the lane platform's host scripts inside the lanes binary. They are installed per host from the
// binary (internal/bench/platform.go); a workload checkout never supplies them.
package deploy

import "embed"

// Files is the platform tree: the lane, registry and image scripts, the docker shim and the host helper files.
//
//go:embed *.sh lane-docker-shim host eks
var Files embed.FS
