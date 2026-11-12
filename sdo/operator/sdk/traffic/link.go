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
// intermittent (a failure fraction over a window, in recurring bursts, not
// necessarily consecutive) or slow (median dial latency far above the link's own healthy
// baseline and its spread, persistently, and beyond any slowdown every other
// link dialed from the same place shows at the same time). Protocol handshakes prove the listener can serve without
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
	// needs; LinkLossPercent of them failing, consecutive or not, in at least
	// LinkLossBursts separate bursts reports the link. A burst ends after
	// LinkBurstGap consecutive successful dials. One stall (a slow node, a
	// burst of DNS timeouts), even with a stray success inside it, is one
	// burst and stays below it; a stall long enough is unreachable instead.
	LinkLossWindow  = 20
	LinkLossPercent = 20
	LinkLossBursts  = 2
	LinkBurstGap    = 3
	// LinkLatencyFactor, LinkSpreadFactor and MinLinkLatencyBound set the
	// slow bound: a window's median dial must exceed LinkLatencyFactor times
	// the healthy baseline, the baseline plus LinkSpreadFactor times its
	// spread, and MinLinkLatencyBound. Baseline and spread are first scaled by
	// the slowdown the link's peers share (see JudgeLinkIn).
	LinkLatencyFactor   = 5
	LinkSpreadFactor    = 8
	MinLinkLatencyBound = 50 * time.Millisecond
	// LinkSlowWindows is how many consecutive windows of Failures dials must
	// each be slow before a link is reported slow.
	LinkSlowWindows = 3
	// LinkBaselineSamples is how many first successful dials the prober
	// takes the healthy baseline median and spread from.
	LinkBaselineSamples = 10
	// HostStallBound is the host CPU stall (the percent of time some runnable
	// task waited for a CPU, PSI "some" avg10) at which a dial is taken on a
	// stalled host. A calm host stays below 10%; on an oversubscribed one
	// heavy handshakes (a database backend, an HTTP worker) time out and slow
	// down at 35% and more while light targets stay fast, so the peers'
	// shared slowdown does not show it. A dial timeout or slowness on a
	// stalled host is evidence of the host, not of one link (see JudgeLinkIn).
	HostStallBound = 25.0
)

// LinkCondition names how a reported link is unhealthy.
type LinkCondition string

const (
	// LinkUnreachable: the last Failures dials all failed.
	LinkUnreachable LinkCondition = "unreachable"
	// LinkIntermittent: at least LinkLossPercent of the last LinkLossWindow
	// dials failed.
	LinkIntermittent LinkCondition = "intermittent"
	// LinkSlow: the median latency of each of the last LinkSlowWindows
	// windows of Failures dials is above the bound derived from the healthy
	// baseline, its spread and the slowdown the link's peers share.
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
	// HostCPUStall is the host-wide CPU stall where the dial was made, PSI
	// "some" avg10 in percent; zero when unknown.
	HostCPUStall float64 `json:"hostCpuStall,omitempty"`
}

// hostStalled reports whether the dial was made on a stalled host.
func (s LinkSample) hostStalled() bool { return s.HostCPUStall >= HostStallBound }

// hostExplained reports whether a stalled host explains the dial's failure:
// a timeout. A refusal, reset or protocol failure is never the host's.
func (s LinkSample) hostExplained() bool {
	return !s.OK && s.Class == LinkFailureTimeout && s.hostStalled()
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
	// BaselineSpread is the median absolute deviation of those same dials
	// from BaselineLatency.
	BaselineSpread time.Duration `json:"baselineSpreadNs,omitempty"`
	// Vantage is where the dials were made from; empty means the prober.
	Vantage LinkVantage `json:"vantage,omitempty"`
	// ClientPod names the client Pod a client-vantage series was dialed from.
	ClientPod string `json:"clientPod,omitempty"`
}

// EffectiveVantage is the observation's vantage, the prober by default.
func (o LinkObservations) EffectiveVantage() LinkVantage {
	if o.Vantage == "" {
		return LinkVantageProber
	}
	return o.Vantage
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
// it does not add DefaultHealthMinDuration. Unreachable and intermittent links
// are critical; a slow link still serves, so it is a warning: batch warnings
// for slow links to open incidents.
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

// LinkVantage names where a link's dials were made from.
type LinkVantage string

const (
	// LinkVantageProber: the link-probe workload's own pod dialed the edge.
	LinkVantageProber LinkVantage = "prober"
	// LinkVantageClient: a vantage probe dialed the edge from inside a Pod
	// of the calling workload, through that Pod's own network namespace,
	// so impairment on the client's egress (traffic shaping, a full
	// connection table, a DNS override) shows.
	LinkVantageClient LinkVantage = "client"
)

// LinkSetting is how a series of fresh dials is judged: the time it is
// judged at, how many consecutive failures report the link, how old the
// newest judged dial may be, and the dial period.
type LinkSetting struct {
	ObservedAt time.Time
	Failures   int
	MaxAge     time.Duration
	Period     time.Duration
	// Source names who dialed, for evidence: a link-probe workload, or a
	// vantage probe and the Pod it ran in.
	Source string
	// Detail is the human-readable setting, for evidence.
	Detail string
	// Peers are the other links dialed from the same place over the same
	// time. Slowness they all share is that place's (a loaded node), not
	// one link's; without peers nothing is discounted.
	Peers []LinkObservations
}

// LinkSetting returns the setting of the window's link-probe workload.
func (w Window) LinkSetting() LinkSetting {
	needed := w.Workload.Failures
	if needed < MinLinkFailures {
		needed = DefaultLinkFailures
	}
	return LinkSetting{
		ObservedAt: w.ObservedAt, Failures: needed, MaxAge: w.SLO().MaxAge.Duration(),
		Period: max(w.Workload.Interval.Duration(), w.Workload.Timeout.Duration()), Source: w.Workload.Name,
		Detail: fmt.Sprintf("(workload %s; interval %s, timeout %s)", w.Workload.Name, w.Workload.Interval.Duration(), w.Workload.Timeout.Duration()),
		Peers:  w.Links,
	}
}

// LinkFailing reports whether the link's most recent dials, at least the
// workload's Failures many and none older than the workload's MaxAge, all failed.
func LinkFailing(window Window, observed LinkObservations) ([]LinkSample, bool) {
	return linkFailing(window.LinkSetting(), observed)
}

func linkFailing(setting LinkSetting, observed LinkObservations) ([]LinkSample, bool) {
	needed := max(setting.Failures, MinLinkFailures)
	samples := observed.Samples
	if !observed.Qualified || len(samples) < needed {
		return nil, false
	}
	run := samples[len(samples)-needed:]
	explained := true
	for _, sample := range run {
		if sample.OK {
			return nil, false
		}
		explained = explained && sample.hostExplained()
	}
	// Timeouts on a stalled host are the host's until they outlast a whole
	// loss window: a heavy handshake can stall for seconds, a dead target
	// keeps failing.
	if explained && needed < LinkLossWindow {
		return linkFailing(LinkSetting{ObservedAt: setting.ObservedAt, Failures: LinkLossWindow, MaxAge: setting.MaxAge}, observed)
	}
	if setting.ObservedAt.Sub(run[len(run)-1].At) > setting.MaxAge {
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
	// Baseline, Spread, Shared and Bound are set for latency judgements.
	// Shared is the median slowdown (recent median over baseline) of the
	// link's peers, at least 1.
	Baseline time.Duration
	Spread   time.Duration
	Shared   float64
	Bound    time.Duration
}

// JudgeLink reports a qualified link whose fresh dials show it unreachable
// (LinkFailing), intermittent (at least LinkLossPercent of the last
// LinkLossWindow dials failed, consecutive or not) or slow. Slow needs each of
// the last LinkSlowWindows windows of Failures dials to have a median above
// the bound: LinkLatencyFactor times the baseline, the baseline plus
// LinkSpreadFactor times its spread, and MinLinkLatencyBound, where baseline
// and spread are first multiplied by the slowdown the link's peers (the
// setting's Peers) share. A loaded prober node slows every dial alike, so it
// never makes one link slow. A stalled host (HostStallBound) explains dial
// timeouts and slowness everywhere at once, even where the peers stay fast:
// slowness is not judged while any judged dial was made on a stalled host,
// its timeouts do not count toward intermittent, and a run of them is
// unreachable only once it fills LinkLossWindow dials. Refusals, resets and
// protocol failures are judged as usual. Only recent samples are judged: the slow and loss
// windows reach back at most the larger of MaxAge and twice their length in
// probe periods.
func JudgeLink(window Window, observed LinkObservations) (LinkJudgement, bool) {
	return JudgeLinkIn(window.LinkSetting(), observed)
}

// JudgeLinkIn judges dials with an explicit setting, so the same rules judge
// a link-probe workload's dials and a vantage probe's dials from a client Pod.
func JudgeLinkIn(setting LinkSetting, observed LinkObservations) (LinkJudgement, bool) {
	if run, failing := linkFailing(setting, observed); failing {
		return linkJudgement(LinkUnreachable, run), true
	}
	if !observed.Qualified {
		return LinkJudgement{}, false
	}
	if recent := freshLinkSamples(setting.ObservedAt, observed.Samples, max(setting.MaxAge, 2*LinkLossWindow*setting.Period), LinkLossWindow); len(recent) == LinkLossWindow {
		if judged := linkJudgement(LinkIntermittent, discountHostStall(recent)); judged.Failed*100 >= LinkLossPercent*len(recent) && failureBursts(recent) >= LinkLossBursts {
			return judged, true
		}
	}
	if observed.BaselineLatency <= 0 {
		return LinkJudgement{}, false
	}
	size := max(setting.Failures, MinLinkFailures)
	recent, ok := slowLinkSamples(setting, observed)
	if !ok || slices.ContainsFunc(recent, LinkSample.hostStalled) {
		return LinkJudgement{}, false
	}
	shared := sharedLinkSlowdown(setting, observed)
	baseline := time.Duration(float64(observed.BaselineLatency) * shared)
	spread := time.Duration(float64(observed.BaselineSpread) * shared)
	bound := max(LinkLatencyFactor*baseline, baseline+LinkSpreadFactor*spread, MinLinkLatencyBound)
	for start := 0; start < len(recent); start += size {
		if linkJudgement(LinkSlow, recent[start:start+size]).P50 <= bound {
			return LinkJudgement{}, false
		}
	}
	judged := linkJudgement(LinkSlow, recent)
	judged.Baseline, judged.Spread, judged.Shared, judged.Bound = observed.BaselineLatency, observed.BaselineSpread, shared, bound
	return judged, true
}

// discountHostStall returns samples with each timeout a stalled host explains
// counted as served, so recurring timeouts of an oversubscribed host never make
// a link intermittent while other failures still do.
func discountHostStall(samples []LinkSample) []LinkSample {
	if !slices.ContainsFunc(samples, LinkSample.hostExplained) {
		return samples
	}
	out := slices.Clone(samples)
	for index, sample := range out {
		if sample.hostExplained() {
			out[index].OK, out[index].Class, out[index].Error = true, "", ""
		}
	}
	return out
}

// failureBursts counts the separate runs of failed dials in samples: a run
// ends after LinkBurstGap consecutive successful dials.
func failureBursts(samples []LinkSample) int {
	bursts, successes := 0, LinkBurstGap
	for _, sample := range samples {
		if sample.OK {
			successes++
			continue
		}
		if successes >= LinkBurstGap {
			bursts++
		}
		successes = 0
	}
	return bursts
}

// slowLinkSamples returns the LinkSlowWindows windows of fresh dials the slow
// judgement reads, or false while there are not enough.
func slowLinkSamples(setting LinkSetting, observed LinkObservations) ([]LinkSample, bool) {
	count := LinkSlowWindows * max(setting.Failures, MinLinkFailures)
	horizon := max(setting.MaxAge, time.Duration(2*count)*setting.Period)
	recent := freshLinkSamples(setting.ObservedAt, observed.Samples, horizon, count)
	return recent, len(recent) == count
}

// sharedLinkSlowdown is the median, over the link's qualified peers with a
// baseline and enough fresh successful dials, of each peer's recent median
// successful dial over its baseline; at least 1. Slowness every link shows at
// once sits where the dials are made, not in one edge. A median ignores a
// minority of genuinely slow peers, and failed dials (a peer that is down) say
// nothing about the node.
func sharedLinkSlowdown(setting LinkSetting, observed LinkObservations) float64 {
	var ratios []float64
	for _, peer := range setting.Peers {
		if peer.Link.ID() == observed.Link.ID() || !peer.Qualified || peer.BaselineLatency <= 0 {
			continue
		}
		recent, ok := slowLinkSamples(setting, peer)
		if !ok {
			continue
		}
		var served []LinkSample
		for _, sample := range recent {
			if sample.OK {
				served = append(served, sample)
			}
		}
		if 2*len(served) >= len(recent) {
			ratios = append(ratios, float64(linkJudgement(LinkSlow, served).P50)/float64(peer.BaselineLatency))
		}
	}
	if len(ratios) == 0 {
		return 1
	}
	slices.Sort(ratios)
	return max(1, ratios[(len(ratios)-1)/2])
}

// freshLinkSamples returns the newest count samples that are within horizon
// of the observation.
func freshLinkSamples(observedAt time.Time, samples []LinkSample, horizon time.Duration, count int) []LinkSample {
	recent := samples[max(0, len(samples)-count):]
	for index, sample := range recent {
		if observedAt.Sub(sample.At) <= horizon {
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
	return LinkFindingIn(spec, namespace, window.LinkSetting(), observed)
}

// LinkFindingIn explains a link JudgeLinkIn reports. A client-vantage
// finding has its own rule ID (the prober's with a ".client" suffix) and
// names the client Pod, so it stands next to the prober's judgement of the
// same edge: slow from the client while fast from the prober places the
// impairment on the client's egress.
func LinkFindingIn(spec sdk.DetectorSpec, namespace string, setting LinkSetting, observed LinkObservations) (sdk.Finding, bool) {
	judged, failing := JudgeLinkIn(setting, observed)
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
	vantage := observed.EffectiveVantage()
	span := run[len(run)-1].At.Sub(run[0].At).Round(time.Millisecond)
	from := "from the prober pod"
	ruleID := LinkRuleIDPrefix + link.ID()
	if vantage == LinkVantageClient {
		from = fmt.Sprintf("from inside client Pod %s", observed.ClientPod)
		ruleID += ".client"
	}
	probes := fmt.Sprintf("fresh %s probes of Service %s port %d %s", link.EffectiveProtocol(), link.To, link.Port, from)
	latency := fmt.Sprintf("dial latency p50 %s, p95 %s", judged.P50.Round(time.Millisecond), judged.P95.Round(time.Millisecond))
	var summary, evidence string
	switch judged.Condition {
	case LinkSlow:
		summary = fmt.Sprintf("link %s is slow: median fresh dial %s exceeds %s (healthy %s)",
			link, judged.P50.Round(time.Millisecond), judged.Bound.Round(time.Millisecond), judged.Baseline.Round(time.Millisecond))
		evidence = fmt.Sprintf(
			"link %s: the last %d %s over %s %s had %s; each of %d consecutive windows had a median above the bound %s "+
				"(the larger of %dx the healthy baseline %s and the baseline plus %dx its spread %s, after the %.1fx slowdown "+
				"the link's peers share; floor %s); %d failed. The delay may sit on either end of the edge or between them.",
			link, len(run), probes, span, setting.Detail, latency, LinkSlowWindows, judged.Bound, LinkLatencyFactor, judged.Baseline,
			LinkSpreadFactor, judged.Spread, judged.Shared, MinLinkLatencyBound, judged.Failed)
	case LinkIntermittent:
		summary = fmt.Sprintf("link %s is intermittent: %d of the last %d fresh dials failed", link, judged.Failed, len(run))
		evidence = fmt.Sprintf(
			"link %s: %d of the last %d %s failed over %s %s, not all in a row (threshold %d%%); %s. "+
				"Failure classes: %v. Recent failures: %s.",
			link, judged.Failed, len(run), probes, span, setting.Detail, LinkLossPercent, latency, classes, strings.Join(messages, "; "))
	default:
		summary = fmt.Sprintf("link %s is unreachable: %d consecutive fresh dials failed", link, len(run))
		evidence = fmt.Sprintf(
			"link %s: the last %d %s all failed over %s %s, after the link had connected before. Existing long-lived "+
				"connections from %s to %s may still work, so user-facing probes can stay green. Recent failures: %s.",
			link, len(run), probes, span, setting.Detail, link.From, link.To, strings.Join(messages, "; "))
	}
	if vantage == LinkVantageClient {
		summary += " (client vantage " + observed.ClientPod + ")"
	}
	last := run[len(run)-1]
	severity := sdk.SeverityCritical
	if judged.Condition == LinkSlow {
		severity = sdk.SeverityWarning
	}
	finding := sdk.Finding{
		DetectorID:       spec.ID,
		RuleID:           ruleID,
		Status:           sdk.FindingActive,
		Severity:         severity,
		Summary:          summary,
		Evidence:         evidence,
		PrimaryResource:  sdk.ObjectRef{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: link.To},
		RelatedResources: []sdk.ObjectRef{{APIVersion: "v1", Kind: "Service", Namespace: namespace, Name: link.From}},
		Playbooks:        append([]string(nil), spec.Playbooks...),
		Metadata: map[string]any{
			"workload": setting.Source, "from": link.From, "to": link.To, "port": link.Port,
			"protocol":  string(link.EffectiveProtocol()),
			"vantage":   string(vantage),
			"condition": string(judged.Condition), "samples": len(run), "failed": judged.Failed, "failure_classes": classes,
			"p50_ns": int64(judged.P50), "p95_ns": int64(judged.P95), "baseline_ns": int64(judged.Baseline), "bound_ns": int64(judged.Bound),
			"spread_ns": int64(judged.Spread), "shared_slowdown": judged.Shared,
			"last_class":           string(last.Class),
			"last_status":          last.Status,
			"last_excerpt":         last.Excerpt,
			"last_latency_ns":      int64(last.Duration),
			"consecutive_failures": judged.Consecutive, "last_error": last.Error,
		},
	}
	if vantage == LinkVantageClient {
		finding.Metadata["client_pod"] = observed.ClientPod
		finding.RelatedResources = append(finding.RelatedResources, sdk.ObjectRef{APIVersion: "v1", Kind: "Pod", Namespace: namespace, Name: observed.ClientPod})
	}
	return finding, true
}
