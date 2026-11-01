package hostio

import (
	"context"
	"sdo.dev/operator/memoryhost"
)

// SetRecovery restarts only this immutable artifact. Manager generations fence
// a late recovery when a newer accepted revision has already activated.
func SetRecovery(manager *memoryhost.Manager, config LaunchConfig) {
	manager.SetRecovery(func(ctx context.Context) (memoryhost.Host, string, string, uint64, error) {
		child, err := Launch(config)
		if err != nil {
			return nil, "", "", 0, err
		}
		if _, err = child.Ready(ctx); err == nil {
			err = manager.Prepare(ctx, child)
		}
		if err != nil {
			_ = child.Close()
			return nil, "", "", 0, err
		}
		ready, err := child.Ready(ctx)
		return child, config.Commit, config.Digest, ready.GetMirrorSequence(), err
	})
}
