package builder

import (
	"context"
	"log/slog"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/toolchain"
)

// seedToolchainBeside starts the responder's offline Go toolchain copy now and
// returns a wait that blocks until that copy has finished. The controller needs
// the seed before it accepts incidents (it seeds again on start, and finds the
// published copy), but the copy only needs the controller image and the
// repository volume, not the accepted memory, so the launch build can run
// beside it instead of the controller start paying for it after the build.
//
// The copy's result is not this start's to judge: a failed copy is logged and
// the controller's own seed on start repeats it and fails there, where a
// missing toolchain has always been reported. Waiting before the controller
// launches keeps its seed from racing a copy still in flight.
func seedToolchainBeside(ctx context.Context, launch *contractsv1alpha1.ControllerConfig, seed func(ctx context.Context, repositoryMount, image string) error) (wait func()) {
	mount, image := launch.GetRepositoryMountPath(), launch.GetToolchainImage()
	if mount == "" || image == "" || toolchain.Seeded(mount, image) {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := seed(ctx, mount, image); err != nil {
			slog.Warn("toolchain_seed_deferred", "error", err)
			return
		}
		slog.Info("toolchain_seed_finished")
	}()
	return func() { <-done }
}
