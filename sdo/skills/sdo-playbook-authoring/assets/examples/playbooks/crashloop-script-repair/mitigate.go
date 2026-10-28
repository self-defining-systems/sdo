package crashloopscript

import (
	"context"

	"sdo.dev/operator/sdk/program"
)

// script is byte-identical to the recorded hand command; labels.json script_origins maps
// its digest to that receipt. Names reach it only through Env, read from the bound object.
const script = `kubectl delete pod --namespace "$NAMESPACE" --selector "app=$APP" --wait=false`

func (Program) Mitigate(_ context.Context, _ program.ReadView, b program.Bindings) (program.Plan, error) {
	ref := b.Objects["deployment"]
	return program.Plan{Actions: []program.Action{program.RunScript{
		Script: script,
		Env: map[string]program.Value{
			"NAMESPACE": program.FromObserved(ref, "metadata.namespace"),
			"APP":       program.FromObserved(ref, "spec.selector.matchLabels.app"),
		},
		Reason: "replace crash-looping Pods after their dependency recovered",
	}}}, nil
}
