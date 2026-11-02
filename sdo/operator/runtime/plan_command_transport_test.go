package runtime

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"testing/quick"

	"sdo.dev/operator/kubereads"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

func TestGeneralHTTPOnlySerializesCredentialReference(t *testing.T) {
	h := program.HTTPRequest{Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "api"}, Container: "app", Method: "POST", URL: "http://api.app.svc/admin", Body: "fixture", Bearer: &program.CredentialRef{Env: "ADMIN_TOKEN"}}
	property := func(secret uint64) bool {
		canary := fmt.Sprintf("synthetic-secret-%016x", secret)
		t.Setenv("ADMIN_TOKEN", canary)
		command := kubereads.HTTPCommand(h)
		raw, err := json.Marshal(command)
		return err == nil && !strings.Contains(string(raw), canary) && strings.Contains(string(raw), "ADMIN_TOKEN") && command.Stdin == "fixture" && strings.Contains(strings.Join(command.Args, " "), "--output /dev/null")
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 128}); err != nil {
		t.Fatal(err)
	}
}
