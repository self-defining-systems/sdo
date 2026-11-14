// Package generators is the toy application's user tests: one read journey per Service, grounded in main.go
// (/ answers the Service name).
package generators

import "sdo.dev/operator/sdk/traffic"

func read(id, description, service string, depends []string, path, want string) traffic.Scenario {
	return traffic.Scenario{
		ID: id, Description: description,
		Target: traffic.Target{Service: service, Port: 8080}, DependsOn: depends,
		SideEffect: traffic.SideEffectRead,
		Detects:    []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		Steps:      []traffic.Step{{Name: id, Endpoint: traffic.GET(path, traffic.Params{}).Contains(want)}},
	}
}

func Scenarios() traffic.Catalog {
	return traffic.Catalog{
		read("frontend-home", "the frontend answers", "frontend", nil, "/", "frontend"),
		read("backend-home", "the backend answers", "backend", nil, "/", "backend"),
		read("catalog-home", "the catalog answers", "catalog", nil, "/", "catalog"),
	}
}
