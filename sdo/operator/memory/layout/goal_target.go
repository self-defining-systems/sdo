package layout

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ReadGoalDetectionTarget reads the optional "Detection target: 30s" line
// of the human health objective. A missing target leaves cadence unchanged.
func ReadGoalDetectionTarget(root string) (time.Duration, error) {
	data, err := os.ReadFile(filepath.Join(root, GoalFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var target time.Duration
	for line := range strings.SplitSeq(string(data), "\n") {
		value, ok := strings.CutPrefix(strings.TrimSpace(line), "Detection target:")
		if !ok {
			continue
		}
		if target != 0 {
			return 0, fmt.Errorf("%s: specify Detection target once", GoalFile)
		}
		target, err = time.ParseDuration(strings.TrimSpace(value))
		if err != nil || target <= 0 {
			return 0, fmt.Errorf("%s: Detection target must be a positive duration such as 30s", GoalFile)
		}
	}
	return target, nil
}
