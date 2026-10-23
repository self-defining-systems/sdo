package runtime

import "sdo.dev/operator/sdk"

func observationGeneration(snapshot sdk.DetectionContext) string {
	if source, ok := snapshot.(sdk.ScopedObservationSource); ok {
		return source.ObservationGeneration()
	}
	return ""
}
