//go:build integration

package runtime

import (
	"context"
	"testing"
)

func TestMenuProgramParametersBindOneOptionWithoutFindingCandidates(t *testing.T) {
	loop, _ := menuB3Loop(t)
	loop.Request.Findings = nil // imported memory with no fresh candidate corpus
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "routing-then-restart", Parameters: map[string]string{"service": "Service/app/chosen", "deployment": "Deployment/app/frontend", "port": "8081"}})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	count := 0
	for _, option := range response.Menu.Options {
		if option.ProgramId == "routing-then-restart" {
			count++
			for _, binding := range option.Bindings {
				if binding.Name == "service" && binding.Object.GetName() != "chosen" {
					t.Fatal("explicit service binding ignored")
				}
			}
			if len(option.Bindings) != 3 {
				t.Fatalf("explicit program parameters missing: %v", option.Bindings)
			}
		}
	}
	if count != 1 {
		t.Fatalf("program option count=%d", count)
	}
}

func TestMenuProgramParametersRejectUnknownAndIncompleteBindings(t *testing.T) {
	for _, parameters := range []map[string]string{
		{"service": "Service/app/backend"},
		{"service": "Service/app/backend", "deployment": "Deployment/app/frontend", "port": "invalid-int"},
		{"service": "Service/app/backend", "deployment": "Deployment/app/frontend", "unknown": "value"},
	} {
		loop, _ := menuB3Loop(t)
		response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: "routing-then-restart", Parameters: parameters})
		if response.Error == "" {
			t.Fatalf("invalid parameters created an option: %v", parameters)
		}
		if loop.LoopsUsed() != 0 {
			t.Fatal("menu binding consumed mitigation budget")
		}
	}
}
