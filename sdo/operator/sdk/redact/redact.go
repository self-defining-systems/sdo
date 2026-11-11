// Package redact removes secret-looking text before it reaches a sample, a
// finding, a capture or an agent: credentials named as such, bearer and basic
// tokens, JWTs, form fields named like secrets, and long opaque runs that mix
// letters and digits (keys and digests). It is the one redaction rule every
// SDK input that keeps text (traffic response excerpts, container log
// excerpts) applies.
package redact

import (
	"regexp"
	"strings"
)

// Marker replaces each redacted value.
const Marker = "[redacted]"

var (
	secretPatterns = []struct {
		re   *regexp.Regexp
		with string
	}{
		{regexp.MustCompile(`(?i)(<input[^>]*name=["'][^"']*(?:token|secret|pass|key|auth)[^"']*["'][^>]*value=["'])[^"']*`), "${1}" + Marker},
		{regexp.MustCompile(`(?i)(name=["'][^"']*(?:token|secret|pass|key|auth)[^"']*["']\s+value=["'])[^"']*`), "${1}" + Marker},
		{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), "${1} " + Marker},
		{regexp.MustCompile(`(?i)((?:pass(?:word|wd)?|secret|token|api[_-]?key|access[_-]?key(?:[_-]?id)?|authorization|credential|cookie)[\w-]*["']?\s*[:=]\s*["']?)[^\s"'<>&,;]+`), "${1}" + Marker},
		{regexp.MustCompile(`eyJ[\w-]+\.[\w-]+\.[\w-]+`), Marker},
		// A cloud access key ID: a fixed prefix and an upper-case run.
		{regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{12,}\b`), Marker},
	}
	opaquePattern = regexp.MustCompile(`[A-Za-z0-9+/_=-]{32,}`)
)

// Secrets replaces credentials, tokens and long opaque strings in text.
func Secrets(text string) string {
	for _, p := range secretPatterns {
		text = p.re.ReplaceAllString(text, p.with)
	}
	// A long run mixing letters and digits is a key or digest, not prose.
	return opaquePattern.ReplaceAllStringFunc(text, func(run string) string {
		if strings.ContainsAny(run, "0123456789") && strings.ContainsAny(run, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			return Marker
		}
		return run
	})
}
