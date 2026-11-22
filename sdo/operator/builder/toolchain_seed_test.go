package builder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	contractsv1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func seedLaunch() *contractsv1alpha1.ControllerConfig {
	return &contractsv1alpha1.ControllerConfig{RepositoryMountPath: "/nonexistent-mount", ToolchainImage: "controller@sha256:abc"}
}

func TestToolchainSeedRunsBesideTheBuildAndTheLaunchWaitsForIt(t *testing.T) {
	release, entered := make(chan struct{}), make(chan struct{})
	var finished atomic.Bool
	wait := seedToolchainBeside(context.Background(), seedLaunch(), func(context.Context, string, string) error {
		close(entered)
		<-release
		finished.Store(true)
		return nil
	})
	// The copy is under way and the caller is free to build beside it.
	<-entered
	if finished.Load() {
		t.Fatal("the seed finished before the build could run beside it")
	}
	go close(release)
	wait()
	if !finished.Load() {
		t.Fatal("wait returned before the seed finished: the controller would race a copy in flight")
	}
}

func TestToolchainSeedFailureIsLeftToTheControllersOwnSeed(t *testing.T) {
	wait := seedToolchainBeside(context.Background(), seedLaunch(), func(context.Context, string, string) error {
		return errors.New("disk full")
	})
	wait() // must neither panic nor block; the controller's seed on start reports it
}

func TestToolchainSeedIsSkippedWithoutAMountOrImage(t *testing.T) {
	called := false
	seed := func(context.Context, string, string) error { called = true; return nil }
	seedToolchainBeside(context.Background(), &contractsv1alpha1.ControllerConfig{}, seed)()
	if called {
		t.Fatal("a launch with no repository mount or toolchain image seeded anyway")
	}
}
