package rendered

import (
	"context"

	"sdo.dev/operator/patchset"
	"sdo.dev/operator/rendered/internal/renderio"
)

// composeGit is the production ComposeFunc: patchset.Apply into a private
// object directory (the accepted store is only read), then the projection
// written to the same directory. Close removes it.
func composeGit(ctx context.Context, dir, base string, series patchset.Series, drop []string) (Composed, error) {
	applied, err := patchset.Apply(ctx, patchset.Store{Dir: dir}, base, series)
	if err != nil {
		return Composed{}, err
	}
	tree, err := renderio.Project(ctx, dir, applied.Env(), applied.Tree, drop)
	if err != nil {
		applied.Close()
		return Composed{}, err
	}
	return Composed{Tree: tree, Env: applied.Env(), Close: applied.Close}, nil
}
