//go:build integration

package runtime

func init() {
	// Seeds 8 and 48 fail without informer sync retrying: list errors during
	// startup exhausted the old three tries and the controller exited.
	registerPRSeed(8, "informer sync at startup gave up under list errors (controller exited)")
	registerPRSeed(48, "informer sync at startup gave up under list errors (controller exited)")
}
