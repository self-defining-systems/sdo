package operatorcli

import (
	"fmt"
	"os"
)

func responderMenuEnabled() (bool, error) {
	value := os.Getenv("SDO_RESPONDER_MENU")
	if value != "" && value != "0" && value != "1" {
		return false, fmt.Errorf("SDO_RESPONDER_MENU must be 0 or 1")
	}
	return value != "0", nil
}
