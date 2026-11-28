package generators

import (
	"sdo.dev/operator/sdk/traffic"
)

const marker = "sdoprobe" // in every note a probe creates, and in its cleanup requests

var (
	notes = traffic.Target{Service: "notes", Port: 8080}
	path  = []string{"notes"}
)

// tagged carries the marker in every request's query (the app ignores it), as SDO requires of writes.
var tagged = traffic.Params{"probe": traffic.Const(marker)}

func step(name string, e traffic.Endpoint) traffic.Step { return traffic.Step{Name: name, Endpoint: e} }

// login signs in a seeded account and saves its bearer token for the following steps.
func login(user string) traffic.Step {
	return step("login", traffic.POST("/api/login", tagged).
		WithJSON(`{"user":"`+user+`","password":"pass-`+user[len(user)-2:]+`-synthetic"}`, nil).
		Status(200).JSONEquals("user", user).SaveJSON("token", "token"))
}

func auth(e *traffic.HTTPEndpoint) *traffic.HTTPEndpoint {
	return e.WithHeader("Authorization", "Bearer {token}")
}

func Scenarios() traffic.Catalog {
	return traffic.Catalog{
		{
			ID: "call-sign-in", Description: "a seeded account signs in; a wrong password is rejected", Target: notes,
			// Signing in is a POST that only issues a token: an idempotent write of bounded data.
			DependsOn: path, SideEffect: traffic.SideEffectIdempotentWrite, Marker: marker,
			Steps: []traffic.Step{
				login("user01"),
				step("wrong-password", traffic.POST("/api/login", tagged).
					WithJSON(`{"user":"user01","password":"not-the-password"}`, nil).Status(401)),
			},
			Detects: []traffic.FaultClass{traffic.FaultErrorStatus, traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "call-notes", Description: "a seeded user's notes list shows the seeded notes", Target: notes,
			DependsOn: path, SideEffect: traffic.SideEffectIdempotentWrite, Marker: marker,
			Steps: []traffic.Step{
				login("user02"),
				step("list", auth(traffic.GET("/api/notes", tagged)).Status(200).
					ExactSet("notes", "title", "welcome of user02", "groceries of user02", "ideas of user02")),
			},
			Detects: []traffic.FaultClass{traffic.FaultErrorStatus, traffic.FaultWrongBody, traffic.FaultSlow},
		},
		{
			ID: "write-note", Description: "create a note, read it back, delete it", Target: notes,
			DependsOn: path, SideEffect: traffic.SideEffectWriteWithCleanup, Marker: marker,
			Steps: []traffic.Step{
				login("user03"),
				step("create", auth(traffic.POST("/api/notes", tagged)).
					WithJSON(`{"title":"{title}","body":"probe"}`, traffic.Params{"title": traffic.Unique("sdoprobe-")}).
					Status(201).SaveJSON("id", "id").SaveJSON("title", "title")),
				step("read-back", auth(traffic.GET("/api/notes/{id}", tagged)).Status(200).JSONEquals("title", "{title}")),
			},
			Cleanup: []traffic.Step{
				step("delete", auth(traffic.DELETE("/api/notes/{id}", tagged)).Status(204)),
				step("gone", auth(traffic.GET("/api/notes/{id}", tagged)).Status(404)),
			},
			Detects: []traffic.FaultClass{traffic.FaultErrorStatus, traffic.FaultWrongBody, traffic.FaultSlow},
		},
	}
}
