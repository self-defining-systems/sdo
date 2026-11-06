package memory

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/memory/layout"
)

// AcceptedHealthObjective reads the human health objective from the accepted
// goal of the repository at appRoot (the body under its "# Health objective"
// heading).
func AcceptedHealthObjective(appRoot string) (string, error) {
	data, err := os.ReadFile(filepath.Join(appRoot, filepath.FromSlash(layout.GoalFile)))
	if err != nil {
		return "", fmt.Errorf("read accepted goal: %w", err)
	}
	text := string(data)
	if _, body, found := strings.Cut(text, "# Health objective\n"); found {
		text = body
	}
	if strings.TrimSpace(text) == "" {
		return "", errors.New("accepted goal has no health objective")
	}
	return strings.TrimSpace(text), nil
}
