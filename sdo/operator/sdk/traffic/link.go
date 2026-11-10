package traffic

import (
	"context"
	"fmt"
	"slices"
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
// and reports the edge when it is unreachable (several consecutive failures),
// intermittent (a failure fraction over a window, not necessarily
// consecutive) or slow (median dial latency far above the link's own healthy
// baseline). Protocol handshakes prove the listener can serve without
// application credentials or queries.

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

	// LinkLossWindow is how many recent dials an intermittent judgement
	// needs; LinkLossPercent of them failing, consecutive or not, reports
	// the link. A single few-second data-plane stall stays below it.
	LinkLossWindow  = 20
	LinkLossPercent = 20
	// LinkLatencyFactor and MinLinkLatencyBound set the slow bound: the
	// median of the last Failures dials must exceed both LinkLatencyFactor
	// times the healthy baseline and MinLinkLatencyBound.
	LinkLatencyFactor   = 5
	MinLinkLatencyBound = 50 * time.Millisecond
	// LinkBaselineSamples is how many first successful dials the prober
	// takes the healthy baseline median from.
	LinkBaselineSamples = 10
)

// LinkCondition names how a reported link is unhealthy.
type LinkCondition string

const (
	// LinkUnreachable: the last Failures dials all failed.
	LinkUnreachable LinkCondition = "unreachable"
	// LinkIntermittent: at least LinkLossPercent of the last LinkLossWindow
	// dials failed.
	LinkIntermittent LinkCondition = "intermittent"
	// LinkSlow: the median latency of the last Failures dials is above the
	// bound derived from the healthy baseline.
	LinkSlow LinkCondition = "slow"
)

// LinkProtocol is how an edge is probed.
type LinkProtocol string

// Link protocols select the credential-free handshake sent on a fresh connection.
const (
	LinkProtocolTCP       LinkProtocol = "tcp"
	LinkProtocolHTTP      LinkProtocol = "http"
	LinkProtocolGRPC      LinkProtocol = "grpc"
	LinkProtocolRedis     LinkProtocol = "redis"
	LinkProtocolMemcached LinkProtocol = "memcached"
	LinkProtocolMongoDB   LinkProtocol = "mongodb"
	LinkProtocolPostgres  LinkProtocol = "postgres"
	LinkProtocolMySQL     LinkProtocol = "mysql"
)

// LinkHTTPOptions configures a GET; status bounds are inclusive (default 100..499).
type LinkHTTPOptions struct {
	Path      string `json:"path,omitempty"`
	StatusMin int    `json:"statusMin,omitempty"`
	StatusMax int    `json:"statusMax,omitempty"`
}

// LinkGRPCOptions enables TLS with system roots and target hostname verification.
type LinkGRPCOptions struct {
	TLS bool `json:"tls,omitempty"`
}

// EffectiveProtocol preserves the TCP default for older observations.
func (l Link) EffectiveProtocol() LinkProtocol {
	if l.Protocol == "" {
		return LinkProtocolTCP
	}
	return l.Protocol
}

// LinkFailureClass identifies a deterministic failure category.
type LinkFailureClass string

const (
	LinkFailureRefused    LinkFailureClass = "refused"
	LinkFailureTimeout    LinkFailureClass = "timeout"
	LinkFailureReset      LinkFailureClass = "reset"
	LinkFailureProtocol   LinkFailureClass = "protocol_error"
	LinkFailureNotServing LinkFailureClass = "not_serving"
	LinkFailureTransport  LinkFailureClass = "transport_error"
)

// MaxLinkExcerpt bounds response and error text stored in samples.
const MaxLinkExcerpt = 256

// Link is one declared dependency edge: From calls To on Port.
type Link struct {
	From     string           `json:"from"`
	To       string           `json:"to"`
	Port     int              `json:"port"`
	Protocol LinkProtocol     `json:"protocol,omitempty"`
	HTTP     *LinkHTTPOptions `json:"http,omitempty"`
	GRPC     *LinkGRPCOptions `json:"grpc,omitempty"`
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
	switch l.EffectiveProtocol() {
	case LinkProtocolTCP, LinkProtocolHTTP, LinkProtocolGRPC, LinkProtocolRedis, LinkProtocolMemcached, LinkProtocolMongoDB, LinkProtocolPostgres, LinkProtocolMySQL:
	default:
		return fmt.Errorf("traffic workload %s: link %s: unknown protocol %q (expected tcp, http, grpc, redis, memcached, mongodb, postgres or mysql)", workload, l, l.Protocol)
	}
	if l.HTTP != nil {
		if l.EffectiveProtocol() != LinkProtocolHTTP {
			return fmt.Errorf("link %s: http options require protocol http", l)
		}
		o := l.HTTP
		if !strings.HasPrefix(o.Path, "/") || strings.ContainsAny(o.Path, "\r\n") || strings.HasPrefix(o.Path, "//") {
			return fmt.Errorf("link %s: http path must be an absolute request path", l)
		}
		if o.StatusMin < 100 || o.StatusMax > 599 || o.StatusMax < o.StatusMin {
			return fmt.Errorf("link %s: http status range must be within 100..599 and ordered", l)
		}
	}
	if l.GRPC != nil && l.EffectiveProtocol() != LinkProtocolGRPC {
		return fmt.Errorf("link %s: grpc options require protocol grpc", l)
	}

	return nil
}

// LinkSample is one fresh connection and protocol handshake of a link.
type LinkSample struct {
	At       time.Time        `json:"at"`
	Duration time.Duration    `json:"durationNs"`
	OK       bool             `json:"ok"`
	Error    string           `json:"error,omitempty"`
	Class    LinkFailureClass `json:"class,omitempty"`
	Status   string           `json:"status,omitempty"`
	Excerpt  string           `json:"excerpt,omitempty"`
}

// LinkObservations are one link's recent samples, oldest first.
type LinkObservations struct {
	Link    Link         `json:"link"`
	Samples []LinkSample `json:"samples"`
	// Qualified reports whether a dial of this link has ever succeeded. A
	// link that never did may simply not admit the prober, so it is not a
	// health signal; only a link that worked and stopped is.
	Qualified bool `json:"qualified"`
	// BaselineLatency is the median latency of the link's first
	// LinkBaselineSamples successful dials, recorded once by the prober. Zero
	// means no baseline yet, and latency is then not judged.
	BaselineLatency time.Duration `json:"baselineLatencyNs,omitempty"`
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
// of a link-probe workload that JudgeLink finds unreachable, intermittent or
// slow. It decides purely from the prober's samples; with no observations it
// reports nothing. The sample counts are the hysteresis, so unlike NewDetector
// it does not add DefaultHealthMinDuration.
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

// LinkJudgement is why JudgeLink reports a link, with the samples judged.
type LinkJudgement struct {
	Condition LinkCondition
	// Samples are the judged dials, oldest first.
	Samples []LinkSample
	// Failed counts failed dials among Samples; Consecutive is the trailing
	// run of failed dials.
	Failed      int
	Consecutive int
	P50, P95    time.Duration
	// Baseline and Bound are set for latency judgements.
	Baseline time.Duration
	Bound    time.Duration
}

// JudgeLink reports a qualified link whose fresh dials show it unreachable
// (LinkFailing), intermittent (at least LinkLossPercent of the last
// LinkLossWindow dials failed, consecutive or not) or slow (the median latency
// of the last Failures dials exceeds LinkLatencyFactor times the healthy
// baseline and MinLinkLatencyBound). Only recent samples are judged: the
// workload's MaxAge bounds the slow window, and the loss window may reach back
// 2*LinkLossWindow probe periods.
func JudgeLink(window Window, observed LinkObservations) (LinkJudgement, bool) {
	if run, failing := LinkFailing(window, observed); failing {
		return linkJudgement(LinkUnreachable, run), true
	}
	if !observed.Qualified {
		return LinkJudgement{}, false
	}
	maxAge := window.SLO().MaxAge.Duration()
	period := max(window.Workload.Interval.Duration(), window.Workload.Timeout.Duration())
	if recent := freshLinkSamples(window, observed.Samples, max(maxAge, 2*LinkLossWindow*period), LinkLossWindow); len(recent) == LinkLossWindow {
		if judged := linkJudgement(LinkIntermittent, recent); judged.Failed*100 >= LinkLossPercent*len(recent) {
			return judged, true
		}
	}
	if observed.BaselineLatency <= 0 {
		return LinkJudgement{}, false
	}
	needed := window.Workload.Failures
	if needed < MinLinkFailures {
		needed = DefaultLinkFailures
	}
	recent := freshLinkSamples(window, observed.Samples, maxAge, needed)
	if len(recent) < needed {
		return LinkJudgement{}, false
	}
	judged := linkJudgement(LinkSlow, recent)
	judged.Baseline = observed.BaselineLatency
	judged.Bound = max(LinkLatencyFactor*observed.BaselineLatency, MinLinkLatencyBound)
	if judged.P50 <= judged.Bound {
		return LinkJudgement{}, false
	}
	return judged, true
}

// freshLinkSamples returns the newest count samples that are within horizon
// of the observation.
func freshLinkSamples(window Window, samples []LinkSample, horizon time.Duration, count int) []LinkSample {
	recent := samples[max(0, len(samples)-count):]
	for index, sample := range recent {
		if window.ObservedAt.Sub(sample.At) <= horizon {
			return recent[index:]
		}
	}
	return nil
}

func linkJudgement(condition LinkCondition, samples []LinkSample) LinkJudgement {
	judged := LinkJudgement{Condition: condition, Samples: samples}
	durations := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		durations = append(durations, sample.Duration)
		if sample.OK {
			judged.Consecutive = 0
		} else {
			judged.Failed++
			judged.Consecutive++
		}
	}
	slices.Sort(durations)
	if len(durations) > 0 {
		judged.P50 = durations[(len(durations)-1)/2]
		judged.P95 = durations[(len(durations)*95+99)/100-1]
	}
	return judged
}

// LinkFinding explains one link JudgeLink reports, with per-link p50/p95
// latency and failure classes. The primary resource is the target Service;
// the calling Service is related.
func LinkFinding(spec sdk.DetectorSpec, namespace string, window Window, observed LinkObservations) (sdk.Finding, bool) {
	judged, failing := JudgeLink(window, observed)
	if !failing {
		return sdk.Finding{}, false
	}
	run, link := judged.Samples, observed.Link
	classes := map[string]any{}
	messages := make([]string, 0, 3)
	for _, sample := range run {
		if sample.OK {
			continue
		}
		count, _ := classes[string(sample.Class)].(int)
		classes[string(sample.Class)] = count + 1
		if len(messages) == 3 {
			messages = messages[1:]
		}
		messages = append(messages, fmt.Sprintf("[%s] class=%s status=%s %s excerpt=%q (%s)",
			sample.At.UTC().Format(time.RFC3339), sample.Class, sample.Status, sample.Error, sample.Excerpt, sample.Duration.Round(time.Millisecond)))
	}
	span := run[len(run)-1].At.Sub(run[0].At).Round(time.Millisecond)
	probes := fmt.Sprintf("fresh %s probes of Service %s port %d from the prober pod", link.EffectiveProtocol(), link.To, link.Port)
	setting := fmt.Sprintf("(workload %s; interval %s, timeout %s)", window.Workload.Name, window.Workload.Interval.Duration(), window.Workload.Timeout.Duration())
	latency := fmt.Sprintf("dial latency p50 %s, p95 %s", judged.P50.Round(time.Millisecond), judged.P95.Round(time.Millisecond))
	var summary, evidence string
	switch judged.Condition {
	case LinkSlow:
		summary = fmt.Sprintf("link %s is slow: median fresh dial %s exceeds %s (healthy %s)",
			link, judged.P50.Round(time.Millisecond), judged.Bound.Round(time.Millisecond), judged.Baseline.Round(time.Millisecond))
		evidence = fmt.Sprintf(
			"link %s: the last %d %s over %s %s had %s, above the bound %s (%dx the healthy baseline %s, floor %s); "+
				"%d failed. The delay may sit on either end of the edge or between them.",
			link, len(run), probes, span, setting, latency, judged.Bound, LinkLatencyFactor, judged.Baseline, MinLinkLatencyBound, judged.Failed)
	case LinkIntermittent:
		summary = fmt.Sprintf("link %s is intermittent: %d of the last %d fresh dials failed", link, judged.Failed, len(run))
		evidence = fmt.Sprintf(
			"link %s: %d of the last %d %s failed over %s %s, not all in a row (threshold %d%%); %s. "+
				"Failure classes: %v. Recent failures: %s.",
			link, judged.Failed, len(run), probes, span, setting, LinkLossPercent, latency, classes, strings.Join(messages, "; "))
	default:
		summary = fmt.Sprintf("link %s is unreachable: %d consecutive fresh dials failed", link, len(run))
		evidence = fmt.Sprintf(
			"link %s: the last %d %s all failed over %s %s, after the link had connected before. Existing long-lived "+
				"connections from %s to %s may still work, so user-facing probes can stay green. Recent failures: %s.",
			link, len(run), probes, span, setting, link.From, link.To, strings.Join(messages, "; "))
	}
	last := run[len(run)-1]
	return sdk.Finding{
		DetectorID:       spec.ID,
		RuleID:           LinkRuleIDPrefix + link.ID(),
		Status:           sdk.FindingActive,
		Severity:         sdk.SeverityCritical,
		Summary:          summary,
		Evidence:         evidence,
		PrimaryResource:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: link.To},
		RelatedResources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: link.From}},
		Playbooks:        append([]string(nil), spec.Playbooks...),
		Metadata: map[string]any{
			"workload": window.Workload.Name, "from": link.From, "to": link.To, "port": link.Port,
			"protocol":  string(link.EffectiveProtocol()),
			"condition": string(judged.Condition), "samples": len(run), "failed": judged.Failed, "failure_classes": classes,
			"p50_ns": int64(judged.P50), "p95_ns": int64(judged.P95), "baseline_ns": int64(judged.Baseline), "bound_ns": int64(judged.Bound),
			"last_class":           string(last.Class),
			"last_status":          last.Status,
			"last_excerpt":         last.Excerpt,
			"last_latency_ns":      int64(last.Duration),
			"consecutive_failures": judged.Consecutive, "last_error": last.Error,
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
