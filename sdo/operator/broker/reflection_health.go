package broker

import (
	"context"
	"errors"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
)

type reflectionHealthKey struct{}

// WithReflectionHealth binds publication to the original incident revision even
// when a corrected proposal is recreated from a newer accepted head.
func WithReflectionHealth(ctx context.Context, revision string) context.Context {
	return context.WithValue(ctx, reflectionHealthKey{}, revision)
}

// ReflectionHealthDiagnostics also serves the controller's pre-supersession
// rejection record. Publication repeats this check under the broker lock.
func (b *Broker) ReflectionHealthDiagnostics(ctx context.Context, revision string) ([]*contracts.MemoryDiagnostic, error) {
	return b.reflectionHealthDiagnostics(ctx, revision, "HEAD")
}

func (b *Broker) reflectionHealthDiagnostics(ctx context.Context, frozen, current string) ([]*contracts.MemoryDiagnostic, error) {
	err := memory.CheckReflectionHealth(ctx, b.target, frozen, current)
	var rule string
	switch {
	case err == nil:
		return nil, nil
	case errors.Is(err, memory.ErrReflectionObjectiveChanged):
		rule = "reflection.objective-changed"
	case errors.Is(err, memory.ErrReflectionPolicyChanged):
		rule = "reflection.policy-changed"
	default:
		return nil, err
	}
	return []*contracts.MemoryDiagnostic{{Rule: rule, Message: err.Error()}}, nil
}
