package memory

import "regexp"

var (
	pemPrivateKey = regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----`)
	// Well-known credential shapes: cloud access key IDs, forge and chat tokens, JWTs.
	tokenShapes = regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b|\bgh[pousr]_[A-Za-z0-9]{30,}|\bglpat-[A-Za-z0-9_-]{20,}|\bxox[abprs]-[A-Za-z0-9-]{10,}|\bsk-[A-Za-z0-9_-]{32,}|\beyJ[\w-]{8,}\.[\w-]{8,}\.[\w-]{8,}`)
)

// CredentialShape classifies credential-shaped bytes without returning their values.
func CredentialShape(data []byte) string {
	switch {
	case pemPrivateKey.Match(data):
		return "PEM private key"
	case tokenShapes.Match(data):
		return "access token or key"
	default:
		return ""
	}
}

func generatedCaseHasCredential(files ...[]byte) bool {
	for _, data := range files {
		if CredentialShape(data) != "" {
			return true
		}
	}
	return false
}
