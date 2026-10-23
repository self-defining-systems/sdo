package operatorcli

import (
	"encoding/json"
	"fmt"
	"os"
)

// Only administrator launch parsing consumes this file. Workers receive neither
// its path nor the authority. Embedded JSON survives container/PVC boundaries.
func readSourcePolicyInput(path, raw string) (string, error) {
	if path != "" && raw != "" {
		return "", fmt.Errorf("source plan policy supplied twice")
	}
	if path == "" && raw == "" {
		path = os.Getenv("SDO_PLAN_SOURCE_POLICY_FILE")
	}
	if path != "" {
		bytes, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read source plan policy: %w", err)
		}
		raw = string(bytes)
	}
	if raw != "" && (len(raw) > 65536 || !json.Valid([]byte(raw))) {
		return "", fmt.Errorf("invalid source plan policy JSON")
	}
	return raw, nil
}
