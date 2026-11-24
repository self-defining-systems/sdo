package memory

import (
	"log/slog"
	"regexp"
)

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

func prepareCorpusCase(files map[string][]byte, incident, playbook string) bool {
	for name, data := range files {
		if name != "recordings.json" && CredentialShape(data) != "" {
			slog.Warn("corpus_case_not_recorded", "incident", incident, "playbook", playbook, "omitted_cases", 1)
			return false
		}
	}
	if data, omitted := corpusRecordings(files["recordings.json"]); omitted {
		slog.Warn("corpus_recordings_omitted", "incident", incident, "playbook", playbook, "omitted_files", 1)
		files["recordings.json"] = data
	}
	return true
}

// corpusRecordings drops the contaminated observation file's contents. An empty
// array keeps the case loadable without altering the captured snapshots.
func corpusRecordings(raw []byte) ([]byte, bool) {
	if CredentialShape(raw) != "" {
		return []byte("[]"), true
	}
	return raw, false
}
