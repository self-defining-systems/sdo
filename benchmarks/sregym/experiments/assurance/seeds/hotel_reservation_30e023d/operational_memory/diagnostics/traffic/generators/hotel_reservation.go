package generators

import "sdo.dev/operator/sdk/traffic"

func Scenarios() traffic.Catalog {
	return traffic.Catalog{
		{
			ID: "hotel-search",
			Description: "Search nearby hotels for a valid date range and render availability and hotel profiles.",
			Target: traffic.Target{Service: "frontend", Port: 5000},
			DependsOn: []string{"frontend", "search", "geo", "reservation", "profile", "mongodb-geo", "mongodb-reservation", "mongodb-profile"},
			SideEffect: traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "search", Endpoint: traffic.GET("/hotels", traffic.Params{"inDate": traffic.Const("2015-04-09"), "outDate": traffic.Const("2015-04-10"), "lat": traffic.Const("37.7867"), "lon": traffic.Const("-122.4112")}).Contains("FeatureCollection")}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID: "hotel-recommendations",
			Description: "Request distance-ranked hotel recommendations and their profiles.",
			Target: traffic.Target{Service: "frontend", Port: 5000},
			DependsOn: []string{"frontend", "recommendation", "profile", "mongodb-recommendation", "mongodb-profile"},
			SideEffect: traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "recommend", Endpoint: traffic.GET("/recommendations", traffic.Params{"require": traffic.Const("dis"), "lat": traffic.Const("37.7867"), "lon": traffic.Const("-122.4112")}).Contains("FeatureCollection")}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
		{
			ID: "user-login-check",
			Description: "Exercise the user credential lookup endpoint with a non-empty account pair.",
			Target: traffic.Target{Service: "frontend", Port: 5000},
			DependsOn: []string{"frontend", "user", "mongodb-user"},
			SideEffect: traffic.SideEffectRead,
			Steps: []traffic.Step{{Name: "login", Endpoint: traffic.GET("/user", traffic.Params{"username": traffic.Const("sdo-synthetic"), "password": traffic.Const("sdo-synthetic")}).Contains("message")}},
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
		},
	}
}
