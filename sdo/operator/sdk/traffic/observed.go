package traffic

import (
	"net/http"
	"regexp"
	"strings"

	"sdo.dev/operator/sdk/redact"
)

// maxObservedScan bounds how much of a response body is examined for an
// excerpt; the excerpt itself is bounded by MaxErrorExcerpt.
const maxObservedScan = 256 << 10

var (
	titlePattern  = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	hiddenPattern = regexp.MustCompile(`(?is)<(script|style|noscript|svg)\b.*?</(?:script|style|noscript|svg)>`)
	tagPattern    = regexp.MustCompile(`(?s)<[^>]*>`)
)

// redactSecrets replaces credentials, tokens and long opaque strings.
func redactSecrets(text string) string { return redact.Secrets(text) }

// observedExcerpt describes a response for an author whose assertion failed:
// the page title and the visible text near the expected phrase (or its start),
// with markup, scripts and secret-looking text removed, bounded by
// MaxErrorExcerpt. A raw prefix of an HTML page is mostly head markup and
// never reaches the part that disagrees with the assertion.
func observedExcerpt(body []byte, status int, expected string) string {
	raw := redactSecrets(string(body[:min(len(body), maxObservedScan)]))
	if strings.TrimSpace(raw) == "" {
		return bounded("(empty body) " + http.StatusText(status))
	}
	prefix := ""
	if match := titlePattern.FindStringSubmatch(raw); match != nil {
		title := strings.Join(strings.Fields(tagPattern.ReplaceAllString(match[1], "")), " ")
		if title != "" {
			prefix = "title " + quoteClip(title, 80) + "; "
		}
	}
	text := tagPattern.ReplaceAllString(hiddenPattern.ReplaceAllString(titlePattern.ReplaceAllString(raw, " "), " "), " ")
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return bounded(prefix + "(no visible text)")
	}
	text = redactSecrets(text)
	start := nearExpected(text, expected)
	room := max(MaxErrorExcerpt-len(prefix), 40)
	end := min(len(text), start+room)
	excerpt := text[start:end]
	if start > 0 {
		excerpt = "…" + excerpt
	}
	if end < len(text) {
		excerpt += "…"
	}
	return prefix + "text " + quoteClip(excerpt, MaxErrorExcerpt)
}

// nearExpected is where the visible text starts to be quoted: a little before
// the first word of the expected phrase that the page does contain.
func nearExpected(text, expected string) int {
	lower := strings.ToLower(text)
	for _, word := range strings.Fields(strings.ToLower(expected)) {
		if len(word) < 3 {
			continue
		}
		if i := strings.Index(lower, word); i >= 0 {
			return max(0, i-40)
		}
	}
	return 0
}

func quoteClip(text string, limit int) string {
	if len(text) > limit {
		text = text[:limit] + "…"
	}
	return `"` + strings.ReplaceAll(text, `"`, `'`) + `"`
}
