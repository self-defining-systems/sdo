// Package logsignal judges declared log lines of application components: a
// failure line that appears more than the healthy application logs it, and an
// activity line the healthy application always logs that stops appearing. A
// log signal is data, never code: the detection designer writes it as a YAML
// file naming application-map components and RE2 patterns, and Go installs a
// deterministic detector built from it.
//
// Reads are the shared bounded container-log collection (sdk/logs: a byte cap
// per container per cycle, a retained window, counts and at most three short
// redacted excerpts per pattern). A signal is judged only against the healthy
// history the controller samples for its query (logs.Result.History): until
// the application has been healthy for a settle period nothing is judged, so
// lines that are normal for this application never fire. Findings carry
// counts, the window and one short redacted sample line, never more.
package logsignal

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"sdo.dev/operator/sdk/logs"
)

// Bounds of one log signal file.
const (
	// MaxComponents bounds the components one file reads.
	MaxComponents = 8
	// MaxPatterns bounds the signal and expect patterns of one file.
	MaxPatterns = 8
	// MaxTotalPatterns bounds the patterns of every log signal file of a
	// tree together; the shared log collection matches each on every line.
	MaxTotalPatterns = 48
	// DefaultWindow is the judgement window when a file sets none.
	DefaultWindow = 2 * time.Minute
	// MinWindow and MaxWindow bound the judgement window; the log collection
	// retains logs.RetainSpan.
	MinWindow = 30 * time.Second
	MaxWindow = logs.RetainSpan
	// HealthyFactor is how many times the healthy maximum a failure line's
	// count must exceed.
	HealthyFactor = 2
)

// Spec is one log signal file: the components whose Pods' logs are read, the
// judgement window and the patterns. Its ID is its file's stem.
type Spec struct {
	// Components are application-map component names; each one's workload's
	// Pods (every container) are read.
	Components []string `yaml:"components"`
	// Window is the judgement window (a Go duration); default DefaultWindow.
	Window string `yaml:"window,omitempty"`
	// Signals are failure lines: one fires when the window's count across a
	// component's Pods reaches MinCount and exceeds HealthyFactor times the
	// most the healthy history logged in a window.
	Signals []Pattern `yaml:"signals,omitempty"`
	// Expect are activity lines: one fires when every healthy window logged
	// at least MinCount of them and a complete current window logged none.
	Expect []Pattern `yaml:"expect,omitempty"`
}

// Pattern is one RE2 expression matched against each log line.
type Pattern struct {
	ID       string `yaml:"id"`
	RE2      string `yaml:"re2"`
	MinCount int    `yaml:"min_count,omitempty"`
}

// Threshold is the pattern's MinCount, default 1.
func (p Pattern) Threshold() int { return max(p.MinCount, 1) }

var (
	idPattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,47}$`)
	patternIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,47}$`)
)

// ValidID reports whether id names a log signal file.
func ValidID(id string) bool { return idPattern.MatchString(id) }

// QueryID is the log query of the log signal id.
func QueryID(id string) string { return "log-signal-" + id }

// Parse decodes a log signal file, rejecting unknown fields.
func Parse(data []byte) (Spec, error) {
	var s Spec
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&s); err != nil {
		return Spec{}, fmt.Errorf("log signal does not parse: %w", err)
	}
	return s, nil
}

// Validate checks the file against its schema and bounds and names every
// problem it finds.
func (s Spec) Validate(id string) error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if !ValidID(id) {
		add("log signal id %q must be lowercase letters, digits and dashes, at most 48", id)
	}
	if len(s.Components) == 0 || len(s.Components) > MaxComponents {
		add("components must name 1 to %d application-map components", MaxComponents)
	}
	seenComponent := map[string]bool{}
	for _, c := range s.Components {
		if strings.TrimSpace(c) == "" || seenComponent[c] {
			add("component %q is empty or repeated", c)
		}
		seenComponent[c] = true
	}
	if _, err := s.WindowDuration(); err != nil {
		add("%v", err)
	}
	all := s.Patterns()
	if len(all) == 0 || len(all) > MaxPatterns {
		add("signals and expect must hold 1 to %d patterns together", MaxPatterns)
	}
	seen := map[string]bool{}
	for _, p := range all {
		if !patternIDPattern.MatchString(p.ID) || seen[p.ID] {
			add("pattern id %q must be unique lowercase letters, digits, dashes or underscores, at most 48", p.ID)
		}
		seen[p.ID] = true
		if strings.TrimSpace(p.RE2) == "" || len(p.RE2) > logs.MaxPatternBytes {
			add("pattern %s: re2 must be 1 to %d bytes", p.ID, logs.MaxPatternBytes)
		} else if re, err := regexp.Compile(p.RE2); err != nil {
			add("pattern %s: %v", p.ID, err)
		} else if re.MatchString("") {
			add("pattern %s matches every line; name the line it means", p.ID)
		}
		if p.MinCount < 0 || p.MinCount > 10000 {
			add("pattern %s: min_count must be 0 to 10000", p.ID)
		}
	}
	return errors.Join(errs...)
}

// WindowDuration is the judgement window.
func (s Spec) WindowDuration() (time.Duration, error) {
	if s.Window == "" {
		return DefaultWindow, nil
	}
	d, err := time.ParseDuration(s.Window)
	if err != nil || d < MinWindow || d > MaxWindow {
		return 0, fmt.Errorf("window %q must be a duration from %s to %s", s.Window, MinWindow, MaxWindow)
	}
	return d, nil
}

// Patterns lists the signal patterns, then the expect patterns.
func (s Spec) Patterns() []Pattern {
	return append(append([]Pattern(nil), s.Signals...), s.Expect...)
}

// Query is the log query of the file id.
func (s Spec) Query(id string) logs.Query {
	q := logs.Query{ID: QueryID(id)}
	for _, p := range s.Patterns() {
		q.Patterns = append(q.Patterns, logs.Pattern{ID: p.ID, RE2: p.RE2})
	}
	return q
}
