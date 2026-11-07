package traffic

import (
	"net/http"
	"regexp"
	"strings"
)

// maxObservedScan bounds how much of a response body is examined for an
// excerpt; the excerpt itself is bounded by MaxErrorExcerpt.
const maxObservedScan = 256 << 10

var (
	titlePattern  = regexp.MustCompile(`(?is)<title[^>]*>(.*?)</title>`)
	hiddenPattern = regexp.MustCompile(`(?is)<(script|style|noscript|svg)\b.*?</(?:script|style|noscript|svg)>`)
	tagPattern    = regexp.MustCompile(`(?s)<[^>]*>`)
	// Secret-looking text, replaced before a response reaches a sample, a
	// finding or an agent's feedback.
	secretPatterns = []struct {
		re   *regexp.Regexp
		with string
	}{
		{regexp.MustCompile(`(?i)(<input[^>]*name=["'][^"']*(?:token|secret|pass|key|auth)[^"']*["'][^>]*value=["'])[^"']*`), "${1}[redacted]"},
		{regexp.MustCompile(`(?i)(name=["'][^"']*(?:token|secret|pass|key|auth)[^"']*["']\s+value=["'])[^"']*`), "${1}[redacted]"},
		{regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`), "${1} [redacted]"},
		{regexp.MustCompile(`(?i)((?:pass(?:word|wd)?|secret|token|api[_-]?key|authorization|credential|cookie)[\w-]*["']?\s*[:=]\s*["']?)[^\s"'<>&,;]+`), "${1}[redacted]"},
		{regexp.MustCompile(`eyJ[\w-]+\.[\w-]+\.[\w-]+`), "[redacted]"},
	}
	opaquePattern = regexp.MustCompile(`[A-Za-z0-9+_=-]{40,}`)
)

// redactSecrets replaces credentials, tokens and long opaque strings.
func redactSecrets(text string) string {
	for _, p := range secretPatterns {
		text = p.re.ReplaceAllString(text, p.with)
	}
	// A long run mixing letters and digits is a key or digest, not prose.
	return opaquePattern.ReplaceAllStringFunc(text, func(run string) string {
		if strings.ContainsAny(run, "0123456789") && strings.ContainsAny(run, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") {
			return "[redacted]"
		}
		return run
	})
}

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
