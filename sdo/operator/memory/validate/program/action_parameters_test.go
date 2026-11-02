package program

import (
	"fmt"
	"strings"
	"testing"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// Every built-in action uses the executor's generated descriptor, including
// newly added actions. No private object values or execution are needed.
func TestAuthoredActionParametersUseExecutorContract(t *testing.T) {
	actions := (&contracts.Action{}).ProtoReflect().Descriptor().Oneofs().Get(0).Fields()
	for i := 0; i < actions.Len(); i++ {
		action := actions.Get(i).Message()
		t.Run(string(action.Name()), func(t *testing.T) {
			command := "sdo mitigate " + string(action.Name())
			if err := checkProgramReadme(command + " --param definitely_unknown=value"); err == nil || !strings.Contains(err.Error(), "definitely_unknown") {
				t.Fatalf("unknown parameter accepted: %v", err)
			}
			fields := action.Fields()
			for j := 0; j < fields.Len(); j++ {
				field := fields.Get(j)
				for _, name := range []string{string(field.Name()), field.JSONName()} {
					if err := checkProgramReadme(fmt.Sprintf("`%s --param %s=reference`", command, name)); err != nil {
						t.Fatalf("published parameter %s rejected: %v", name, err)
					}
				}
			}
		})
	}
	for _, command := range []string{
		"sdo mitigate SyncWebhookCA --param namespace=app",
		"sdo --json plan SyncWebhookCA --param=namespace=app",
		"`sdo mitigate SyncWebhookCA --param 'namespace=app'`",
	} {
		if err := checkProgramReadme(command); err == nil || !strings.Contains(err.Error(), "namespace") {
			t.Fatalf("p008 parameter accepted: %v", err)
		}
	}
}
