package runtime

import (
	"context"
	"encoding/json"
	"testing"
)

func multiCall(t *testing.T, loop *ResponderLoop, options string) ToolResponse {
	t.Helper()
	var request ToolRequest
	if err := json.Unmarshal([]byte(`{"command":"mitigate","selection_set":{"version":1,"options":`+options+`}}`), &request); err != nil {
		t.Fatal(err)
	}
	return loop.Handle(context.Background(), request)
}
