package runtime

import (
	"sdo.dev/operator/sdk"
)

func hasTargetPortChange(event *sdk.DriftEvent) bool {
	for _, field := range event.Fields {
		if field.Path == "spec.ports[0].targetPort" && field.New == "9999" {
			return true
		}
	}
	return false
}
