package routing

import (
	learned "fixture-diagnostics/playbooks/service-targetport-mismatch"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"time"
)

func New() sdk.Detector {
	return program.DetectorFromProgram(learned.Program{}, func(s sdk.DetectionContext) []program.Bindings {
		var result []program.Bindings
		for _, service := range s.Services() {
			result = append(result, program.Bindings{Objects: program.ObjectBindings{
				"service": {APIVersion: "v1", Kind: "Service", Namespace: service.Namespace, Name: service.Name},
			}})
		}
		return result
	}, sdk.DetectorSpec{
		ID: "service-targetport-mismatch", Class: sdk.DetectorClassIncident, Owner: sdk.DetectorOwnerResponder,
		Watches:  []sdk.WatchKind{{APIVersion: "v1", Kind: "Service"}, {APIVersion: "v1", Kind: "Pod"}},
		Interval: time.Second, Persistence: sdk.PersistencePolicy{Firing: 1, Clearing: 1},
		Batching:  sdk.BatchingPolicy{Severity: sdk.SeverityWarning, Debounce: 500 * time.Millisecond},
		Playbooks: []string{"service-targetport-mismatch"}, OriginatingCommit: "reflection",
		OriginatingIncident: "scripted-incident",
	})
}
