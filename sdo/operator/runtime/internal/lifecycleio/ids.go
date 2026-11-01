package lifecycleio

import (
	"crypto/rand"
	"encoding/hex"
)

// NewSessionID creates the default private responder session namespace.
func NewSessionID() (string, error) {
	session := make([]byte, 16)
	if _, err := rand.Read(session); err != nil {
		return "", err
	}
	return hex.EncodeToString(session), nil
}

// NewReplyID supplies an unpredictable temporary reply filename component.
func NewReplyID() string { return rand.Text() }
