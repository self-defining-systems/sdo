package deployment

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

// BuildFailureCause classifies why a BuildKit build failed, so the deployer
// is told what to fix instead of guessing from "backoff limit".
type BuildFailureCause string

const (
	BuildFailureMissingSource BuildFailureCause = "missing COPY source"
	BuildFailureExcluded      BuildFailureCause = "COPY source excluded by .dockerignore"
	BuildFailureNetwork       BuildFailureCause = "network or package-manager error"
	BuildFailureCompile       BuildFailureCause = "compile error"
	BuildFailureOther         BuildFailureCause = "build step failed"
)

const (
	maxBuildLogLines = 30
	maxBuildLogLine  = 240
	maxBuildLogTail  = 2400
	// maxBuildLogScan bounds the log bytes inspected; the failure is at the end.
	maxBuildLogScan = 256 << 10
)

// BuildFailureError carries the failing BuildKit step and a bounded tail of
// the build log. It is a build outcome, not a time limit.
type BuildFailureError struct {
	Image   string
	Job     string
	Cause   BuildFailureCause
	Step    string
	Message string
	// Missing lists COPY/ADD sources BuildKit could not find in the context.
	Missing []string
	// Excluded lists COPY sources that exist in the checkout but are
	// excluded from the context by a .dockerignore pattern.
	Excluded []string
	LogTail  string
}

func (e *BuildFailureError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "BuildKit Job %s for image %s failed (%s)", e.Job, e.Image, e.Cause)
	if e.Step != "" {
		fmt.Fprintf(&b, " at step %q", e.Step)
	}
	if e.Message != "" {
		fmt.Fprintf(&b, ": %s", e.Message)
	}
	b.WriteString(". ")
	switch e.Cause {
	case BuildFailureMissingSource:
		fmt.Fprintf(&b, "Sources %s do not exist under this build's context_path: COPY/ADD sources resolve relative to the context, so pick the directory that contains them (or fix the COPY path); other contexts and the manifests are not at fault. ", strings.Join(e.Missing, ", "))
	case BuildFailureExcluded:
		fmt.Fprintf(&b, "Sources %s exist in the checkout but a .dockerignore pattern excludes them from the build context: remove or narrow that pattern (or add a \"!path\" exception); do not change context_path. ", strings.Join(e.Excluded, ", "))
	case BuildFailureNetwork:
		b.WriteString("A download or package install failed: if the package or URL is wrong fix that line, otherwise this is a transient mirror/network failure and the same plan can be resubmitted; contexts and parallelism are not the cause. ")
	case BuildFailureCompile:
		b.WriteString("The source does not compile: fix the code or build target named in the log below (for example do not enable a service whose source is broken); the plan's paths are correct. ")
	}
	if e.LogTail != "" {
		b.WriteString("Last lines of the BuildKit log:\n" + e.LogTail)
	}
	return strings.TrimSpace(b.String())
}

var (
	ansiPattern      = regexp.MustCompile(`\x1b\[[0-9;?]*[A-Za-z]`)
	stepHeader       = regexp.MustCompile(`^#(\d+) (\[[^\]]*\d+/\d+\].*)$`)
	stepError        = regexp.MustCompile(`^#(\d+) ERROR: (.*)$`)
	vertexLine       = regexp.MustCompile(`^#(\d+) (\S.*)$`)
	cacheNoise       = regexp.MustCompile(`(?i)(importing|exporting) cache|cache (importer|exporter)`)
	missingSource    = regexp.MustCompile(`failed to calculate checksum of ref \S+: "([^"]+)": not found`)
	networkPattern   = regexp.MustCompile(`(?i)could not resolve|temporary failure resolving|failed to fetch|unable to locate package|dial tcp|i/o timeout|connection (refused|timed out|reset)|tls handshake|name or service not known|404\s+not found|no such host|received http code|couldn't connect|gnutls_handshake|hash sum mismatch|unable to fetch some archives`)
	compilePattern   = regexp.MustCompile(`(?i)\berror:|\berror\[E|undefined reference|compilation terminated|make(\[\d+\])?: \*\*\*|npm ERR!|cannot find package|no matching function|syntax error|SyntaxError|cannot find symbol|BUILD FAILURE|ld returned`)
	summaryLine      = regexp.MustCompile(`(?m)^(#\d+ ERROR: |error: failed to solve).*$`)
	solveFailurePart = regexp.MustCompile(`^error: failed to solve: (.*)$`)
)

// ClassifyBuildLog turns a (possibly huge) BuildKit plain-progress log into a
// typed, bounded failure. It never returns more than a few thousand
// characters, so it is safe to pass to the deployer prompt.
func ClassifyBuildLog(image, job, log string) *BuildFailureError {
	failure := &BuildFailureError{Image: image, Job: job, Cause: BuildFailureOther}
	if len(log) > maxBuildLogScan {
		log = log[len(log)-maxBuildLogScan:]
	}
	lines := strings.Split(strings.ReplaceAll(ansiPattern.ReplaceAllString(log, ""), "\r", "\n"), "\n")
	steps := map[string]string{}
	names := map[string]string{}
	failingID, noiseID, noiseMessage := "", "", ""
	for _, line := range lines {
		line = strings.TrimRight(line, " \t")
		if m := stepHeader.FindStringSubmatch(line); m != nil {
			steps[m[1]] = m[2]
		}
		if m := vertexLine.FindStringSubmatch(line); m != nil {
			if _, ok := names[m[1]]; !ok {
				names[m[1]] = m[2]
			}
		}
		if m := stepError.FindStringSubmatch(line); m != nil {
			// A cold registry cache fails its import before any step; that
			// vertex error is not the build failure, so the first error of a
			// real step wins and cache errors are only a fallback.
			if cacheNoise.MatchString(names[m[1]]) || cacheNoise.MatchString(m[2]) {
				if noiseID == "" {
					noiseID, noiseMessage = m[1], clip(m[2], maxBuildLogLine)
				}
			} else if failingID == "" {
				failingID, failure.Message = m[1], clip(m[2], maxBuildLogLine)
			}
		}
		if m := solveFailurePart.FindStringSubmatch(line); m != nil && failure.Message == "" {
			failure.Message = clip(m[1], maxBuildLogLine)
		}
	}
	if failingID == "" && failure.Message == "" {
		failingID, failure.Message = noiseID, noiseMessage
	}
	failure.Step = clip(steps[failingID], maxBuildLogLine)
	text := strings.Join(lines, "\n")
	// BuildKit's own summary lines ("ERROR: process ... did not complete",
	// "error: failed to solve") say nothing about the cause.
	body := summaryLine.ReplaceAllString(text, "")
	seen := map[string]bool{}
	for _, m := range missingSource.FindAllStringSubmatch(text, -1) {
		if !seen[m[1]] && len(failure.Missing) < 8 {
			seen[m[1]] = true
			failure.Missing = append(failure.Missing, m[1])
		}
	}
	switch {
	case len(failure.Missing) > 0:
		failure.Cause = BuildFailureMissingSource
	case networkPattern.MatchString(body) && !compilePattern.MatchString(tailLines(lines, 60)):
		failure.Cause = BuildFailureNetwork
	case compilePattern.MatchString(body):
		failure.Cause = BuildFailureCompile
	case networkPattern.MatchString(body):
		failure.Cause = BuildFailureNetwork
	}
	failure.LogTail = boundedTail(lines)
	return failure
}

// ExplainExclusions moves missing COPY sources that exist on disk but are
// excluded by .dockerignore out of Missing: they are not missing, and the
// advice to move context_path would be wrong.
func (e *BuildFailureError) ExplainExclusions(contextDir, dockerfileAbs string) {
	if len(e.Missing) == 0 {
		return
	}
	rules := loadDockerignore(contextDir, dockerfileAbs)
	var missing []string
	for _, source := range e.Missing {
		rel := strings.TrimPrefix(path.Clean(source), "/")
		if pattern, ok := rules.Excluded(rel); ok && existsUnder(contextDir, rel) {
			e.Excluded = append(e.Excluded, fmt.Sprintf("%q (excluded by %q in %s)", rel, pattern, rules.File))
		} else {
			missing = append(missing, source)
		}
	}
	e.Missing = missing
	if len(e.Missing) == 0 && len(e.Excluded) > 0 {
		e.Cause = BuildFailureExcluded
	}
}

func tailLines(lines []string, n int) string {
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func clip(s string, max int) string {
	if runes := []rune(s); len(runes) > max {
		return string(runes[:max]) + "..."
	}
	return s
}

// boundedTail keeps the last non-blank lines, each clipped, within the
// character cap, dropping the oldest lines first.
func boundedTail(lines []string) string {
	kept := []string{}
	total := 0
	for i := len(lines) - 1; i >= 0 && len(kept) < maxBuildLogLines; i-- {
		line := clip(strings.TrimSpace(lines[i]), maxBuildLogLine)
		if line == "" {
			continue
		}
		if total+len(line)+1 > maxBuildLogTail {
			break
		}
		total += len(line) + 1
		kept = append(kept, line)
	}
	for i, j := 0, len(kept)-1; i < j; i, j = i+1, j-1 {
		kept[i], kept[j] = kept[j], kept[i]
	}
	return strings.Join(kept, "\n")
}
