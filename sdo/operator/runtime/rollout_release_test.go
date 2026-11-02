package runtime

import (
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
)

// The mitigate response carries the release to the responder tool and back
// through recorded tool calls without losing the typed action.
func TestToolResponseRoundTripsRolloutRelease(t *testing.T) {
	release, err := planActionProto(program.RetireOldRevision{Ref: sdk.ObjectRef{APIVersion: "apps/v1", Kind: "ReplicaSet", Namespace: "app", Name: "web-old"}})
	if err != nil {
		t.Fatal(err)
	}
	pending := &contracts.RolloutPending{Reason: "stalled", Blockers: []*contracts.RolloutBlocker{{Ref: planRefProto(sdk.ObjectRef{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "app", Name: "web"}), Reason: "held", Release: []*contracts.Action{release}}}}
	encoded, err := json.Marshal(ToolResponse{Command: "mitigate", RolloutPending: pending})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"retire_old_revision":{"ref":{"api_version":"apps/v1","kind":"ReplicaSet","namespace":"app","name":"web-old"}}`) {
		t.Fatalf("release is not in the tool's JSON shape: %s", encoded)
	}
	var decoded ToolResponse
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(decoded.RolloutPending, pending) {
		t.Fatalf("round trip lost the release: %v", decoded.RolloutPending)
	}
}
