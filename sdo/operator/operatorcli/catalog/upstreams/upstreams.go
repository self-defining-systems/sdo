// Package upstreams is the single wiring point for upstream watchers. The
// controller parent process and the operator CLI import it; no other package
// names a watcher implementation.
package upstreams

import (
	"fmt"

	"sdo.dev/operator/upstream"
	"sdo.dev/operator/upstream/gitpoll"
)

// Catalog returns a catalog holding every built-in watcher: git. Registration
// of a built-in cannot fail, so a failure is a programming error and panics.
func Catalog() *upstream.Catalog {
	c := upstream.NewCatalog()
	for _, register := range []func(*upstream.Catalog) error{gitpoll.Register} {
		if err := register(c); err != nil {
			panic(fmt.Sprintf("upstreams: register built-in watcher: %v", err))
		}
	}
	return c
}
