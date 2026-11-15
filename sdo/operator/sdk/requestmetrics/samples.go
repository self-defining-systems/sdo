package requestmetrics

import (
	"fmt"
	"regexp"
	"strings"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/logs"
)

// LogQueryID names the log query whose excerpts carry failing request lines.
const LogQueryID = "reqmetrics-failing-requests"

// MaxSamples is how many failing request samples a finding carries.
const MaxSamples = 3

// The two patterns select access-log lines whose status is 4xx or 5xx: JSON
// lines with a "status" field (production_json.log style) and common or
// combined text lines (nginx): `"GET /path HTTP/1.1" 500 12`.
var failingPatterns = []logs.Pattern{
	{ID: "json-failed", RE2: `"status"\s*:\s*"?[45]\d\d`},
	{ID: "text-failed", RE2: `"\s[45]\d\d\s`},
}

// LogQueries declares the access-log query that supplies failing request
// samples; the runtime collects it from the entry services' pod logs.
func (d *detector) LogQueries() []logs.Query {
	if len(d.cfg.Entries) == 0 {
		return nil
	}
	return []logs.Query{{ID: LogQueryID, Patterns: append([]logs.Pattern(nil), failingPatterns...)}}
}

// Sample is one failing request read from an access log.
type Sample struct {
	Method, Path, Status, Time, Pod string
}

func (s Sample) String() string {
	out := fmt.Sprintf("%s %s -> %s", s.Method, s.Path, s.Status)
	if s.Time != "" {
		out += " at " + s.Time
	}
	return out
}

func jsonField(name string) *regexp.Regexp {
	return regexp.MustCompile(`"` + regexp.QuoteMeta(name) + `"\s*:\s*(?:"((?:[^"\\]|\\.)*)"|(-?\d+))`)
}

var (
	jsonMethod = []*regexp.Regexp{jsonField("method"), jsonField("verb"), jsonField("request_method")}
	jsonPath   = []*regexp.Regexp{jsonField("path"), jsonField("fullpath"), jsonField("request_uri"), jsonField("uri"), jsonField("url")}
	jsonStatus = []*regexp.Regexp{jsonField("status"), jsonField("status_code")}
	jsonTime   = []*regexp.Regexp{jsonField("time"), jsonField("@timestamp"), jsonField("timestamp"), jsonField("ts")}
	textLine   = regexp.MustCompile(`"([A-Z]+) (\S+)[^"]*"\s+(\d{3})\b`)
	textTime   = regexp.MustCompile(`\[([^\]]+)\]`)
)

func firstField(res []*regexp.Regexp, line string) string {
	for _, re := range res {
		if m := re.FindStringSubmatch(line); m != nil {
			if m[1] != "" {
				return m[1]
			}
			return m[2]
		}
	}
	return ""
}

// parseLine reads method, path, status and time from a JSON or common/combined
// access-log line. Query strings are dropped (they may carry tokens). A JSON
// line cut at the excerpt limit still parses when the fields precede the cut.
func parseLine(line string) (Sample, bool) {
	line = strings.TrimSpace(line)
	var s Sample
	if strings.Contains(line, `"status`) {
		s = Sample{Method: firstField(jsonMethod, line), Path: firstField(jsonPath, line), Status: firstField(jsonStatus, line), Time: firstField(jsonTime, line)}
	} else if m := textLine.FindStringSubmatch(line); m != nil {
		s = Sample{Method: m[1], Path: m[2], Status: m[3]}
		if t := textTime.FindStringSubmatch(line); t != nil {
			s.Time = t[1]
		}
	}
	if i := strings.IndexAny(s.Path, "?#"); i >= 0 {
		s.Path = s.Path[:i]
	}
	if s.Method == "" || s.Path == "" || len(s.Status) != 3 || (s.Status[0] != '4' && s.Status[0] != '5') {
		return Sample{}, false
	}
	return s, true
}

// routeRegexp turns a route template (":id" segments, "*" tail) into a matcher
// for concrete request paths; a route without placeholders matches exactly.
func routeRegexp(route string) *regexp.Regexp {
	route = strings.TrimSpace(route)
	// Metrics may label the route "GET /path"; the path part is what logs show.
	if i := strings.Index(route, " "); i > 0 && strings.ToUpper(route[:i]) == route[:i] {
		route = route[i+1:]
	}
	var b strings.Builder
	b.WriteString("^")
	for _, seg := range strings.SplitAfter(route, "/") {
		name := strings.TrimSuffix(seg, "/")
		tail := seg[len(name):]
		switch {
		case strings.HasSuffix(name, "*"):
			b.WriteString(regexp.QuoteMeta(strings.TrimSuffix(name, "*")) + `.*`)
		case strings.HasPrefix(name, ":"):
			b.WriteString(`[^/]+`)
		default:
			b.WriteString(regexp.QuoteMeta(name))
		}
		b.WriteString(regexp.QuoteMeta(tail))
	}
	b.WriteString("/?$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return regexp.MustCompile(`^$`)
	}
	return re
}

// sampleResult is the samples for one operation and, when empty, why.
type sampleResult struct {
	samples []Sample
	why     string
	pods    []string
}

// failingSamples reads the newest failing requests of the operation from the
// frozen log result: pods of the entry service, newest cycle first.
func failingSamples(ctx sdk.DetectionContext, op Operation, class string) sampleResult {
	res, ok := logs.ResultFrom(ctx, LogQueryID)
	switch {
	case !ok:
		return sampleResult{why: "the detection context carries no access-log result, so request samples are unavailable"}
	case res.Error != "":
		return sampleResult{why: "access logs could not be read: " + res.Error}
	}
	matcher := routeRegexp(op.Route)
	var out sampleResult
	unparsed, seen := 0, 0
	for _, w := range res.Windows {
		if w.Pod != op.Service && !strings.HasPrefix(w.Pod, op.Service+"-") {
			continue
		}
		out.pods = append(out.pods, w.Pod)
		for i := len(w.Cycles) - 1; i >= 0; i-- {
			for _, p := range failingPatterns {
				ex := w.Cycles[i].Matches[p.RE2].Excerpts
				for j := len(ex) - 1; j >= 0; j-- {
					seen++
					s, ok := parseLine(ex[j])
					if !ok {
						unparsed++
						continue
					}
					if class == ClassServer && s.Status[0] != '5' || class == ClassClientJump && s.Status[0] != '4' {
						continue
					}
					if op.Method != "" && !strings.EqualFold(s.Method, op.Method) || !matcher.MatchString(s.Path) {
						continue
					}
					s.Pod = w.Pod
					out.samples = append(out.samples, s)
				}
			}
		}
	}
	if len(out.samples) > MaxSamples {
		out.samples = out.samples[:MaxSamples]
	}
	if len(out.samples) > 0 {
		return out
	}
	switch {
	case len(out.pods) == 0:
		out.why = fmt.Sprintf("no pod log of service %s was collected", op.Service)
	case seen == 0:
		out.why = fmt.Sprintf("no failing access-log lines in the logs of %s", strings.Join(out.pods, ", "))
	case unparsed == seen:
		out.why = fmt.Sprintf("%d failing log lines of %s were not parseable as JSON or common/combined access logs", seen, strings.Join(out.pods, ", "))
	default:
		out.why = fmt.Sprintf("none of the %d failing access-log lines of %s belongs to this operation", seen, strings.Join(out.pods, ", "))
	}
	return out
}

func (r sampleResult) text() string {
	if len(r.samples) == 0 {
		return " No failing request samples: " + r.why + "."
	}
	parts := make([]string, len(r.samples))
	for i, s := range r.samples {
		parts[i] = s.String()
	}
	return " Recent failing requests from the access logs of " + strings.Join(r.pods, ", ") + ": " + strings.Join(parts, "; ") + "."
}

func (r sampleResult) metadata() []map[string]string {
	out := make([]map[string]string, 0, len(r.samples))
	for _, s := range r.samples {
		m := map[string]string{"method": s.Method, "path": s.Path, "status": s.Status, "pod": s.Pod}
		if s.Time != "" {
			m["time"] = s.Time
		}
		out = append(out, m)
	}
	return out
}

// ParseFailingLine reads a failing (4xx or 5xx) request from a JSON or
// common/combined access-log line; ok is false for any other line.
func ParseFailingLine(line string) (Sample, bool) { return parseLine(line) }
