package rendered

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"sdo.dev/operator/sdk"
)

// ErrTargetInterrupted refuses a restore of an object whose phase a head
// deployment started and did not finish: which revision it runs is not known.
var ErrTargetInterrupted = errors.New("the deployment that last wrote this object's phase was interrupted")

// DeploymentTarget is the revision the cluster was last moved to. It differs
// from the accepted deployment while a head that was applied is not accepted.
// Comparisons and restores use it; trust and acceptance never do.
type DeploymentTarget struct {
	Commit string `json:"commit"`
	// AcceptedCommit is the newest accepted deployment when the target was
	// recorded: a later acceptance supersedes the target.
	AcceptedCommit string `json:"accepted_commit"`
	// Pins are the images applied so far, bound to the target's effective tree.
	Pins ReceiptPins `json:"pins"`
	// Unreached names the phases no apply started; their objects keep Previous.
	Unreached []string `json:"unreached,omitempty"`
	// Interrupted names the phases whose apply started and did not succeed.
	Interrupted []string          `json:"interrupted,omitempty"`
	Previous    *DeploymentTarget `json:"previous,omitempty"`
}

// TargetWrite is one step of a head deployment: before a phase is applied
// (Applied false) and after its apply succeeded. Phase is empty for a hook
// phase, whose objects are not desired state; Later lists the non-hook phases
// after it.
type TargetWrite struct {
	Phase   string
	Later   []string
	Applied bool
	Pins    ReceiptPins
}

// Advance returns the target after write. The same effective tree applied
// again continues the recorded one: a phase reached once stays reached.
func (t *DeploymentTarget) Advance(write TargetWrite) {
	t.Unreached = slices.DeleteFunc(t.Unreached, func(name string) bool { return name == write.Phase || !slices.Contains(write.Later, name) })
	t.Interrupted = slices.DeleteFunc(t.Interrupted, func(name string) bool { return name == write.Phase })
	if !write.Applied && write.Phase != "" {
		t.Interrupted = append(t.Interrupted, write.Phase)
	}
	images := write.Pins.Images
	for _, old := range t.Pins.Images {
		if !slices.ContainsFunc(images, func(pin DeployedImage) bool {
			return pin.Ref == old.Ref && pin.Name == old.Name && pin.Init == old.Init
		}) {
			images = append(images[:len(images):len(images)], old)
		}
	}
	t.Pins = write.Pins
	t.Pins.Images = images
	if len(t.Unreached) == 0 && len(t.Interrupted) == 0 {
		t.Previous = nil
	}
}

func (t *DeploymentTarget) key() string {
	if t == nil {
		return ""
	}
	return fmt.Sprint(t.Commit, t.Pins, t.Unreached, t.Interrupted, t.Previous.key())
}

func (s Source) target() (*DeploymentTarget, error) {
	if s.Target == nil {
		return nil, nil
	}
	target, err := s.Target()
	if err != nil {
		return nil, sourceErrorf(SourceUnavailable, "read the deployment target: %w", err)
	}
	return target, nil
}

// layerOf names the layer of target (its render is set) that the cluster's ref
// comes from: target itself, or the one before it while ref's phase is
// unreached or, with phases unreached, target does not declare ref.
func (t *DeploymentTarget) layerOf(set DesiredSet, ref sdk.ObjectRef) (*DeploymentTarget, error) {
	for _, phase := range set.Phases {
		if !slices.ContainsFunc(phase.Objects, func(object DesiredObject) bool { return object.Ref == ref }) {
			continue
		}
		switch {
		case slices.Contains(t.Interrupted, phase.Name):
			return nil, fmt.Errorf("%w: %s/%s", ErrTargetInterrupted, ref.Kind, ref.Name)
		case slices.Contains(t.Unreached, phase.Name):
			return t.Previous, nil
		}
		return t, nil
	}
	if len(t.Unreached) > 0 {
		return t.Previous, nil
	}
	return t, nil
}

// composeTarget replaces the objects of target's unreached phases with the
// revision they still run. objects are the target's own render.
func (s Source) composeTarget(ctx context.Context, target *DeploymentTarget, set DesiredSet, objects []DesiredObject, namespace string) ([]DesiredObject, error) {
	phase := map[sdk.ObjectRef]string{}
	for _, p := range set.Phases {
		for _, object := range p.Objects {
			phase[object.Ref] = p.Name
		}
	}
	var out []DesiredObject
	seen := map[sdk.ObjectRef]bool{}
	for _, object := range objects {
		name := phase[object.Ref]
		if slices.Contains(target.Unreached, name) {
			continue
		}
		object.Interrupted = !object.Hook && slices.Contains(target.Interrupted, name)
		seen[object.Ref] = true
		out = append(out, object)
	}
	if len(target.Unreached) == 0 || target.Previous == nil {
		return out, nil
	}
	previous, err := s.loadManifests(ctx, target.Previous.Commit, namespace, target.Previous)
	if err != nil {
		return nil, fmt.Errorf("render the revision before the deployment target: %w", err)
	}
	for _, object := range previous {
		if !seen[object.Ref] {
			out = append(out, object)
		}
	}
	return out, nil
}
