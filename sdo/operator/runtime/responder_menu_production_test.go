package runtime

import (
	"context"
	"strings"
	"testing"
)

func TestRunRejectsInvalidMenuLaunchInput(t *testing.T) {
	t.Setenv("SDO_RESPONDER_MENU", "yes")
	err := RunWithOptions(context.Background(), nil, RuntimeOptions{Args: []string{}})
	if err == nil || !strings.Contains(err.Error(), "SDO_RESPONDER_MENU") {
		t.Fatalf("invalid explicit input silently ignored: %v", err)
	}
}
