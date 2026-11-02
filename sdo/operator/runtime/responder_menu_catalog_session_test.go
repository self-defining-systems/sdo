package runtime

import (
	"context"
	"testing"

)

func advertisedProgramOption(t *testing.T, loop *ResponderLoop, id string) string {
	t.Helper()
	response := loop.Handle(context.Background(), ToolRequest{Command: "menu", ProgramID: id})
	if response.Error != "" {
		t.Fatal(response.Error)
	}
	for _, option := range response.Menu.GetOptions() {
		if option.ProgramId == id {
			return option.OptionId
		}
	}
	t.Fatalf("program %s was not advertised", id)
	return ""
}

