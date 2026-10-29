// Package settings defines trusted run policy shared across operator seams.
package settings

import "fmt"

const ReplayEnvironment = "SDO_REPLAY"

// Replay resolves the default-on run policy; candidate memory never selects it.
func Replay(value string) (string, error) {
	switch value {
	case "", "on":
		return "on", nil
	case "off":
		return "off", nil
	}
	return "", fmt.Errorf("replay must be on or off, got %q", value)
}
