package traffic

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/sdk"
)

// Link reachability is a per-edge counterpart of the scenario probes. A
// scenario drives a user journey through the application's entry point; if
// that entry point (or a client it owns) keeps a long-lived connection to a
// dependency, the journey can stay green while a fresh connection to the
// dependency can no longer be made, because connectivity rules are evaluated
// only when a connection is opened. A link probe therefore dials each declared
// service-to-service edge afresh, at every interval, from a pod of its own,
// and reports the edge after several consecutive failures. It knows nothing
// about why a dial fails and nothing about any application.

const (
	// LinkRuleIDPrefix prefixes the rule ID of each link finding
	// ("link-reachability.<from>.<to>.<port>").
	LinkRuleIDPrefix = "link-reachability."

	DefaultLinkInterval = 1 * time.Second
	DefaultLinkTimeout  = 1 * time.Second
	// DefaultLinkFailures is how many consecutive failed dials report an
	// edge. With the default interval and timeout it spans at least about
	// five seconds, which clears the roughly three-second data-plane stalls
	// a small cluster shows after a pod-network change with margin.
	DefaultLinkFailures = 5
	MinLinkFailures     = 2
	MaxLinkFailures     = 60
	MaxLinks            = 64
	MinLinkInterval     = 100 * time.Millisecond
	MaxLinkInterval     = 1 * time.Minute
)

// LinkProtocol is how an edge is probed.
type LinkProtocol string

// LinkProtocolTCP completes a TCP handshake to the target Service and port.
const LinkProtocolTCP LinkProtocol = "tcp"

// Link is one declared dependency edge: From calls To on Port.
type Link struct {
	From     string       `json:"from"`
	To       string       `json:"to"`
	Port     int          `json:"port"`
	Protocol LinkProtocol `json:"protocol,omitempty"`
}

// ID names the edge.
func (l Link) ID() string { return fmt.Sprintf("%s.%s.%d", l.From, l.To, l.Port) }

// String renders the edge for humans.
func (l Link) String() string { return fmt.Sprintf("%s -> %s:%d", l.From, l.To, l.Port) }

func (l Link) validate(workload string) error {
	if !namePattern.MatchString(l.From) || !namePattern.MatchString(l.To) {
		return fmt.Errorf("traffic workload %s: link %s -> %s must name Services", workload, l.From, l.To)
	}
	if l.From == l.To {
		return fmt.Errorf("traffic workload %s: link %s calls itself", workload, l.From)
	}
	if l.Port < 1 || l.Port > 65535 {
		return fmt.Errorf("traffic workload %s: link %s port %d is out of range", workload, l, l.Port)
	}
	if l.Protocol != "" && l.Protocol != LinkProtocolTCP {
		return fmt.Errorf("traffic workload %s: link %s protocol must be %q", workload, l, LinkProtocolTCP)
	}
	return nil
}

// LinkSample is one fresh dial of a link.
type LinkSample struct {
	At       time.Time     `json:"at"`
	Duration time.Duration `json:"durationNs"`
	OK       bool          `json:"ok"`
	Error    string        `json:"error,omitempty"`
}

// LinkObservations are one link's recent samples, oldest first.
type LinkObservations struct {
	Link    Link         `json:"link"`
	Samples []LinkSample `json:"samples"`
	// Qualified reports whether a dial of this link has ever succeeded. A
	// link that never did may simply not admit the prober, so it is not a
	// health signal; only a link that worked and stopped is.
	Qualified bool `json:"qualified"`
}

func (w Workload) validateLinks() error {
	if w.Purpose != PurposeLinkProbe {
		if len(w.Links) > 0 || w.Interval != 0 || w.Failures != 0 {
			return fmt.Errorf("traffic workload %s: links, interval and failures apply only to a %s workload", w.Name, PurposeLinkProbe)
		}
		return nil
	}
	if len(w.Scenarios) > 0 {
		return fmt.Errorf("traffic workload %s: a %s runs no scenarios", w.Name, PurposeLinkProbe)
	}
	if len(w.Links) == 0 || len(w.Links) > MaxLinks {
		return fmt.Errorf("traffic workload %s: a %s needs 1 to %d links", w.Name, PurposeLinkProbe, MaxLinks)
	}
	seen := make(map[string]bool, len(w.Links))
	for _, link := range w.Links {
		if err := link.validate(w.Name); err != nil {
			return err
		}
		if seen[link.ID()] {
			return fmt.Errorf("traffic workload %s: link %s is listed twice", w.Name, link)
		}
		seen[link.ID()] = true
	}
	if w.Interval.Duration() < MinLinkInterval || w.Interval.Duration() > MaxLinkInterval {
		return fmt.Errorf("traffic workload %s: interval must be in [%s, %s]", w.Name, MinLinkInterval, MaxLinkInterval)
	}
	if w.Failures < MinLinkFailures || w.Failures > MaxLinkFailures {
		return fmt.Errorf("traffic workload %s: failures must be in [%d, %d]", w.Name, MinLinkFailures, MaxLinkFailures)
	}
	return nil
}

type linkDetector struct {
	spec     sdk.DetectorSpec
	workload string
}

// NewLinkDetector returns a health detector that reports every qualified link
// of a link-probe workload whose last Failures dials all failed. It decides
// purely from the prober's samples; with no observations it reports nothing.
// The consecutive-failure count is the hysteresis, so unlike NewDetector it
// does not add DefaultHealthMinDuration.
func NewLinkDetector(spec sdk.DetectorSpec, workload string) sdk.Detector {
	return linkDetector{spec: spec, workload: workload}
}

func (d linkDetector) Spec() sdk.DetectorSpec { return d.spec }

func (d linkDetector) TrafficWorkloads() []string { return []string{d.workload} }

func (d linkDetector) Detect(_ context.Context, ctx sdk.DetectionContext) ([]sdk.Finding, error) {
	window, ok := WindowFrom(ctx, d.workload)
	if !ok {
		return nil, nil
	}
	if window.LoadError != "" {
		return nil, fmt.Errorf("traffic workload %s: %s", d.workload, window.LoadError)
	}
	findings := make([]sdk.Finding, 0)
	for _, observed := range window.Links {
		if finding, failing := LinkFinding(d.spec, ctx.Namespace(), window, observed); failing {
			findings = append(findings, finding)
		}
	}
	sort.Slice(findings, func(left int, right int) bool { return findings[left].RuleID < findings[right].RuleID })
	return findings, nil
}

// LinkFailing reports whether the link's most recent dials, at least the
// workload's Failures many and none older than the workload's MaxAge, all failed.
func LinkFailing(window Window, observed LinkObservations) ([]LinkSample, bool) {
	needed := window.Workload.Failures
	if needed < MinLinkFailures {
		needed = DefaultLinkFailures
	}
	samples := observed.Samples
	if !observed.Qualified || len(samples) < needed {
		return nil, false
	}
	run := samples[len(samples)-needed:]
	for _, sample := range run {
		if sample.OK {
			return nil, false
		}
	}
	maxAge := window.SLO().MaxAge.Duration()
	if window.ObservedAt.Sub(run[len(run)-1].At) > maxAge {
		return nil, false
	}
	return run, true
}

// LinkFinding explains one link whose fresh dials keep failing. The primary
// resource is the target Service; the calling Service is related.
func LinkFinding(spec sdk.DetectorSpec, namespace string, window Window, observed LinkObservations) (sdk.Finding, bool) {
	run, failing := LinkFailing(window, observed)
	if !failing {
		return sdk.Finding{}, false
	}
	link := observed.Link
	messages := make([]string, 0, 3)
	for _, sample := range run[max(0, len(run)-3):] {
		messages = append(messages, fmt.Sprintf("[%s] %s (%s)",
			sample.At.UTC().Format(time.RFC3339), sample.Error, sample.Duration.Round(time.Millisecond)))
	}
	evidence := fmt.Sprintf(
		"link %s: the last %d fresh TCP dials of Service %s port %d from the prober pod all failed over %s "+
			"(workload %s; interval %s, timeout %s), after the link had connected before. Existing long-lived "+
			"connections from %s to %s may still work, so user-facing probes can stay green. Recent failures: %s.",
		link, len(run), link.To, link.Port, run[len(run)-1].At.Sub(run[0].At).Round(time.Millisecond),
		window.Workload.Name, window.Workload.Interval.Duration(), window.Workload.Timeout.Duration(),
		link.From, link.To, strings.Join(messages, "; "),
	)
	return sdk.Finding{
		DetectorID: spec.ID,
		RuleID:     LinkRuleIDPrefix + link.ID(),
		Status:     sdk.FindingActive,
		Severity:   sdk.SeverityCritical,
		Summary: fmt.Sprintf("link %s is unreachable: %d consecutive fresh dials failed",
			link, len(run)),
		Evidence:         evidence,
		PrimaryResource:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: link.To},
		RelatedResources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: link.From}},
		Playbooks:        append([]string(nil), spec.Playbooks...),
		Metadata: map[string]any{
			"workload": window.Workload.Name, "from": link.From, "to": link.To, "port": link.Port,
			"consecutive_failures": len(run), "last_error": run[len(run)-1].Error,
		},
	}, true
}

// UndrivenLinks returns the edges declared by link-probe workloads that no
// scenario of a health-probe workload drives, sorted by ID. A scenario drives
// an edge when its Target or DependsOn names both ends. A link probe proves only
// that a fresh connection opens; a fault that answers the connection but fails
// the request (a broken payment call behind checkout) shows only through a
// continuously probed scenario whose request crosses the edge, so each edge
// returned here is one the health probes cannot see break.
func UndrivenLinks(catalog Catalog, workloads []Workload) []Link {
	var driving []Scenario
	for _, workload := range workloads {
		if workload.Purpose != PurposeHealthProbe {
			continue
		}
		for _, selected := range workload.Scenarios {
			if scenario, ok := catalog.Scenario(selected.ID); ok {
				driving = append(driving, scenario)
			}
		}
	}
	seen := map[string]bool{}
	var undriven []Link
	for _, workload := range workloads {
		if workload.Purpose != PurposeLinkProbe {
			continue
		}
		for _, link := range workload.Links {
			if seen[link.ID()] || drives(driving, link) {
				continue
			}
			seen[link.ID()] = true
			undriven = append(undriven, link)
		}
	}
	sort.Slice(undriven, func(i, j int) bool { return undriven[i].ID() < undriven[j].ID() })
	return undriven
}

func drives(scenarios []Scenario, link Link) bool {
	for _, scenario := range scenarios {
		on := map[string]bool{scenario.Target.Service: true}
		for _, service := range scenario.DependsOn {
			on[service] = true
		}
		if on[link.From] && on[link.To] {
			return true
		}
	}
	return false
}

// UndrivenLinkNote is the non-blocking memory-check note for an edge
// UndrivenLinks returns.
func UndrivenLinkNote(link Link) string {
	return fmt.Sprintf("traffic edge %s is not driven by any health-probe scenario: add a scenario whose requests "+
		"cross it (writes included), or note in links.yaml why none can", link)
}
