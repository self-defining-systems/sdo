package sdk

import "time"

// DefaultSelfHealHold is how long a Kubernetes failure condition without its
// own give-up signal must hold before it opens an incident: the kubelet's
// CrashLoopBackOff maximum back-off, so a crash that Kubernetes still retries
// has had one full retry.
const DefaultSelfHealHold = 5 * time.Minute

// SelfHealPolicy is implemented by detection contexts that carry the
// controller's configured self-heal hold. Snapshot capture records it, so
// replay judges a held condition exactly as the controller did.
type SelfHealPolicy interface {
	SelfHealHold() time.Duration
}

// SelfHealHold returns the context's self-heal hold, or DefaultSelfHealHold
// when the context carries none.
func SelfHealHold(ctx DetectionContext) time.Duration {
	if policy, ok := ctx.(SelfHealPolicy); ok {
		if hold := policy.SelfHealHold(); hold > 0 {
			return hold
		}
	}
	return DefaultSelfHealHold
}

// HeldPastSelfHeal reports whether a condition that has held since since
// outlived Kubernetes' own self-healing: at least the context's self-heal hold
// passed by its observation time. since is a Kubernetes timestamp (a
// condition's lastTransitionTime, an object's creation); an unknown since or
// observation time never proves the hold elapsed.
func HeldPastSelfHeal(ctx DetectionContext, since time.Time) bool {
	now := ObservationTime(ctx)
	return !since.IsZero() && !now.IsZero() && now.Sub(since) >= SelfHealHold(ctx)
}
