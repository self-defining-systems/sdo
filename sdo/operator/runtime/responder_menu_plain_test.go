package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"testing"
)

func plainProgramOption(t *testing.T, menu *contracts.MitigationMenu, index int) string {
	t.Helper()
	ids := []string{}
	for _, option := range menu.Options {
		if option.ProgramId == "routing-then-restart" || option.ProgramId == "restart-only" {
			ids = append(ids, option.OptionId)
		}
	}
	if index >= len(ids) {
		t.Fatal("program option missing")
	}
	return ids[index]
}

