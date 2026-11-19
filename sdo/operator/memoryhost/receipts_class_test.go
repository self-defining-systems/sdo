package memoryhost

import (
	"encoding/json"
	"testing"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/program"
	"sdo.dev/operator/sdk/sdktest"
)

func TestReceiptsCarryTheReadErrorClassToReplay(t *testing.T) {
	target := program.DatabaseTarget{Engine: program.MongoDB, Pod: sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: "app", Name: "store-a-0"}, Container: "store", Database: "admin"}
	args, _ := json.Marshal([]any{target})
	calls := []sdktest.PrimitiveCall{{Method: "DatabaseUsers", Arguments: args, Result: json.RawMessage(`null`), Error: "database catalog observation unavailable", ErrorClass: sdk.ReadClassRefused}}
	receipts, err := EncodeReceipts(calls)
	if err != nil {
		t.Fatal(err)
	}
	if receipts[0].ErrorClass != sdk.ReadClassRefused {
		t.Fatalf("receipt class = %q", receipts[0].ErrorClass)
	}
	decoded, err := DecodeReceipts(receipts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sdktest.Snapshot{NamespaceName: "app", Readings: decoded}.DatabaseUsers(target)
	if got := sdk.ReadErrorClass(err); got != sdk.ReadClassRefused {
		t.Fatalf("replayed through a receipt, class = %q (%v)", got, err)
	}
}
