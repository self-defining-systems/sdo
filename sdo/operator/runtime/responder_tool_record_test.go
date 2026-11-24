package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestRejectedSecretToolCallDoesNotPersistPayload(t *testing.T) {
	const requestCanary = "synthetic-request-canary"
	const errorCanary = "synthetic-error-canary"
	loop := &ResponderLoop{gateError: errors.New("rejected body: " + errorCanary)}
	request := ToolRequest{Command: "mitigate", Parameters: map[string]string{
		"object": `{"apiVersion":"v1","kind":"Secret","stringData":{"password":"` + requestCanary + `"}}`,
	}}
	if response := loop.Handle(context.Background(), request); response.Error == "" {
		t.Fatal("request was not rejected")
	}
	calls := loop.ToolCalls()
	if len(calls) != 1 || calls[0].Error == "" {
		t.Fatalf("expected one rejected call, got %d", len(calls))
	}
	recorded, _ := json.Marshal(calls[0])
	result, err := incidentResultContract(IncidentResult{ProgramToolCalls: calls})
	if err != nil {
		t.Fatal(err)
	}
	published, err := protojson.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range [][]byte{recorded, published} {
		if strings.Contains(string(data), requestCanary) || strings.Contains(string(data), errorCanary) {
			t.Fatal("tool call evidence retained credential material")
		}
	}
}
