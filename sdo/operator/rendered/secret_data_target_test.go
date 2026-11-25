package rendered_test

import (
	"context"
	"errors"
	"testing"

	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// A Secret restore follows the target phase by phase, as a manifest does.
func TestSecretDataFollowsTheDeploymentTargetPhaseByPhase(t *testing.T) {
	source := releasesSource(t)
	target := &rendered.DeploymentTarget{Commit: syntheticID("a", "2"), Previous: &rendered.DeploymentTarget{Commit: syntheticID("a", "1")}, Pins: rendered.ReceiptPins{EffectiveTree: syntheticID("b", "2")}}
	source.Target = func() (*rendered.DeploymentTarget, error) { return target, nil }
	read := func(name string) (string, error) {
		data, err := source.AcceptedSecretData(context.Background(), target.Commit, "app", sdk.ObjectRef{APIVersion: "v1", Kind: "Secret", Namespace: "app", Name: name})
		return string(data["release"]), err
	}
	if got, err := read("token"); err != nil || got != "2" {
		t.Fatalf("applied: %q %v", got, err)
	}
	target.Unreached = []string{"worker"}
	if got, err := read("token"); err != nil || got != "1" {
		t.Fatalf("unreached: %q %v", got, err)
	}
	if got, err := read("token-1"); err != nil || got != "1" {
		t.Fatalf("dropped by the target, its phases unreached: %q %v", got, err)
	}
	target.Unreached, target.Interrupted = nil, []string{"worker"}
	if _, err := read("token"); !errors.Is(err, rendered.ErrTargetInterrupted) {
		t.Fatalf("interrupted: %v", err)
	}
}
