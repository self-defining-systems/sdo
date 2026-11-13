package generators

import "sdo.dev/operator/sdk/traffic"

func Scenarios() traffic.Catalog {
	return traffic.Catalog{
		{
			ID: "frontend-home", Description: "Read the frontend root.",
			Target:    traffic.Target{Service: "frontend", Port: 8080},
			DependsOn: []string{"frontend"}, SideEffect: traffic.SideEffectRead,
			Steps:   []traffic.Step{{Name: "get", Endpoint: traffic.GET("/", nil).Contains("frontend")}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "frontend-journey", Description: "Read the frontend journey, which calls the backend.",
			Target:    traffic.Target{Service: "frontend", Port: 8080},
			DependsOn: []string{"frontend", "backend"}, SideEffect: traffic.SideEffectRead,
			Steps:   []traffic.Step{{Name: "get", Endpoint: traffic.GET("/journey", nil).Contains("journey ok")}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "catalog-home", Description: "Read the catalog root.",
			Target:    traffic.Target{Service: "catalog", Port: 8080},
			DependsOn: []string{"catalog"}, SideEffect: traffic.SideEffectRead,
			Steps:   []traffic.Step{{Name: "get", Endpoint: traffic.GET("/", nil).Contains("catalog")}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
		},
	}
}
