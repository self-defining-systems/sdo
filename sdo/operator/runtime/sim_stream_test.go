package runtime

import (
	"fmt"

	"sdo.dev/operator/sdk"
)

// simStreamDefaultRuns keeps the default sweep to about half a minute; a run replays
// hours of virtual time through a durable store.
const simStreamDefaultRuns = 24

// simStreamServices is the number of Services the fault stream draws from.
const simStreamServices = 4

// simStreamMaxTicks bounds a run in virtual one-second ticks, so a stall fails
// loudly instead of hanging.
const simStreamMaxTicks = 6000

func simServiceRef(name string) sdk.ObjectRef {
	return sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: "app", Name: name}
}

func simServiceName(i int) string { return fmt.Sprintf("svc-%d", i) }

func unstructuredPorts(object map[string]any) ([]any, bool, error) {
	spec, _ := object["spec"].(map[string]any)
	ports, _ := spec["ports"].([]any)
	return ports, len(ports) > 0, nil
}

// simBehavior is what the scripted responder does with one dispatched incident.
type simBehavior int

const (
	simFixAll simBehavior = iota
	simFixNone
	simFixFirst
	simDispatchError
	simJobFailed
)

var simEpoch = newSimClock().Now()
