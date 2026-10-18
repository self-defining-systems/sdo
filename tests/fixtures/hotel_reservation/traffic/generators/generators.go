// Package generators is a hand-written test fixture of what the health judge
// authors for the hotel-reservation source SREGym deploys
// (SREGym-applications/hotelReservation), grounded in
// services/frontend/server.go, cmd/user/db.go, and the wrk2 mixed workload.
// Production code never contains these routes.
package generators

import (
	"strconv"
	"strings"

	"sdo.dev/operator/sdk/traffic"
)

var frontend = traffic.Target{Service: "frontend", Port: 5000}

// cmd/user/db.go seeds users 0..500 as Cornell_<i> with the decimal digits
// of i repeated ten times as the password.
func seededUser(index int) string     { return "Cornell_" + strconv.Itoa(index) }
func seededPassword(index int) string { return strings.Repeat(strconv.Itoa(index), 10) }

// The wrk2 workload searches April 2015 around San Francisco.
func location() traffic.Params {
	return traffic.Params{
		"lat": traffic.FloatBetween(37.7835, 38.2635, 4),
		"lon": traffic.FloatBetween(-122.252, -121.927, 4),
	}
}

func Scenarios() traffic.Catalog {
	search := location()
	search["inDate"] = traffic.DateRange("2015-04-09", "2015-04-23")
	search["outDate"] = traffic.DaysAfter("inDate", 1, 3)

	recommend := location()
	recommend["require"] = traffic.OneOf("dis", "rate", "price")

	username, password := traffic.FromSeedPair("user", 501, seededUser, seededPassword)

	return traffic.Catalog{
		{
			ID: "search-hotels", Description: "search nearby hotels for a date range",
			Target: frontend, DependsOn: []string{"search", "geo", "rate", "profile"},
			SideEffect: traffic.SideEffectRead,
			Detects:    []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
			Steps:      []traffic.Step{{Name: "search", Endpoint: traffic.GET("/hotels", search).Contains("FeatureCollection")}},
		},
		{
			ID: "recommend", Description: "hotel recommendations by distance, rate, or price",
			Target: frontend, DependsOn: []string{"recommendation", "profile"},
			SideEffect: traffic.SideEffectRead,
			Detects:    []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
			Steps:      []traffic.Step{{Name: "recommend", Endpoint: traffic.GET("/recommendations", recommend).Contains("FeatureCollection")}},
		},
		{
			ID: "login", Description: "a seeded user logs in",
			Target: frontend, DependsOn: []string{"user"},
			SideEffect: traffic.SideEffectRead,
			Detects:    []traffic.FaultClass{traffic.FaultWrongBody, traffic.FaultSlow},
			Steps: []traffic.Step{{Name: "login", Endpoint: traffic.GET("/user", traffic.Params{
				"username": username, "password": password,
			}).Contains("Login successfully!")}},
		},
		{
			// The frontend has no cancel endpoint, so the reservation books
			// zero rooms in 2099 for a dedicated synthetic customer: it never
			// consumes capacity, but each call still stores a record. It runs
			// only in the bounded journey workload, not in continuous probes.
			ID: "reserve", Description: "a seeded user books zero rooms for the synthetic customer",
			Target: frontend, DependsOn: []string{"user", "reservation"},
			SideEffect: traffic.SideEffectIdempotentWrite, Marker: "sdo-synthetic",
			Detects: []traffic.FaultClass{traffic.FaultWrongBody},
			Steps: []traffic.Step{{Name: "reserve", Endpoint: traffic.POST("/reservation", traffic.Params{
				"inDate": traffic.Const("2099-01-01"), "outDate": traffic.Const("2099-01-02"),
				"hotelId": traffic.IntBetween(1, 80), "customerName": traffic.Const("sdo-synthetic"),
				"username": username, "password": password, "number": traffic.Const("0"),
			}).Contains("Reserve successfully!")}},
		},
	}
}
