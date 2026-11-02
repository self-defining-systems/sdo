package builder

import ()

const readOnlyShop = `package generators

import "sdo.dev/operator/sdk/traffic"

func Scenarios() traffic.Catalog {
	return traffic.Catalog{{
		ID: "browse", Target: traffic.Target{Service: "frontend", Port: 8080},
		DependsOn: []string{"frontend", "catalog"}, SideEffect: traffic.SideEffectRead,
		Steps: []traffic.Step{{Name: "list", Endpoint: traffic.GET("/products", nil)}},
	}}
}
`

const readOnlyHealth = `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: health
purpose: health-probe
ratePerSecond: 2
scenarios:
  - id: browse
`

const shopLinks = `apiVersion: sdo.dev/v1alpha1
kind: TrafficWorkload
name: links
purpose: link-probe
links:
  - {from: frontend, to: catalog, port: 8080}
  - {from: checkout, to: payment, port: 8080}
`
