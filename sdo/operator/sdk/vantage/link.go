package vantage

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"sdo.dev/operator/sdk"
	"sdo.dev/operator/sdk/podread"
	"sdo.dev/operator/sdk/traffic"
)

// ClientLinks turns a dialing probe's readings into one client-vantage link
// series per Pod, for traffic.JudgeLinkIn. The healthy latency is the median
// of the successful baseline dials, so a slow client egress is judged against
// the client's own healthy dials, not the prober's. A Pod's link is
// qualified once one of its dials, or a baseline dial, succeeded.
func ClientLinks(result Result, link traffic.Link) []traffic.LinkObservations {
	var baseline []time.Duration
	for _, reading := range result.Baseline {
		for _, dial := range reading.Dials {
			if dial.OK {
				baseline = append(baseline, dial.Duration)
			}
		}
	}
	slices.Sort(baseline)
	var healthy time.Duration
	if len(baseline) > 0 {
		healthy = baseline[(len(baseline)-1)/2]
	}
	var out []traffic.LinkObservations
	for _, reading := range result.Readings {
		if reading.Error != "" || len(reading.Dials) == 0 {
			continue
		}
		observed := traffic.LinkObservations{Link: link, Vantage: traffic.LinkVantageClient, ClientPod: reading.Pod, BaselineLatency: healthy, Qualified: len(baseline) > 0}
		for _, dial := range reading.Dials {
			observed.Qualified = observed.Qualified || dial.OK
			observed.Samples = append(observed.Samples, traffic.LinkSample{
				At: dial.At, Duration: dial.Duration, OK: dial.OK, Error: dial.Error,
				Class: traffic.LinkFailureClass(dial.Class), Status: dial.Status,
			})
		}
		out = append(out, observed)
	}
	return out
}

// ClientSetting is how a probe's dials are judged: DialsPerProbe consecutive
// failures report a link, and the newest dial may be up to two intervals old.
func ClientSetting(probe Probe, pod string, observedAt time.Time) traffic.LinkSetting {
	return traffic.LinkSetting{
		ObservedAt: observedAt, Failures: DialsPerProbe, MaxAge: 2 * probe.Interval, Period: probe.Interval / DialsPerProbe,
		Source: probe.ID,
		Detail: fmt.Sprintf("(vantage probe %s in Pod %s; %d dials every %s, timeout %s each)", probe.ID, pod, DialsPerProbe, probe.Interval, DialTimeout),
	}
}

type clientLinkDetector struct {
	spec  sdk.DetectorSpec
	probe Probe
	link  traffic.Link
}

// NewClientLinkDetector returns a detector that dials link from inside the
// Pods holding the from role (a TCP connect, or an HTTP GET when the link's
// protocol is http) and reports each Pod whose dials traffic.JudgeLinkIn
// finds unreachable, intermittent or slow, as a client-vantage link finding.
// When a link-probe workload dials the same edge, the finding records the
// prober's judgement: "prober_condition" is "healthy" when the prober sees
// the edge fine, which places the impairment on the client side
// ("impairment": "client-egress").
func NewClientLinkDetector(spec sdk.DetectorSpec, id string, from podread.Target, link traffic.Link, interval time.Duration) (sdk.Detector, error) {
	probe := Probe{ID: id, Kind: TCPConnect, From: from, To: &Peer{Host: link.To, Port: link.Port}, Interval: interval}
	if link.EffectiveProtocol() == traffic.LinkProtocolHTTP {
		path := "/"
		if link.HTTP != nil && link.HTTP.Path != "" {
			path = link.HTTP.Path
		}
		probe.Kind, probe.To.Path = HTTPGet, path
	}
	d := clientLinkDetector{spec: spec, probe: probe, link: link}
	return d, ValidateDetector(d)
}

func (d clientLinkDetector) Spec() sdk.DetectorSpec { return d.spec }
func (d clientLinkDetector) VantageProbes() []Probe { return []Probe{d.probe} }

func (d clientLinkDetector) Detect(_ context.Context, view sdk.DetectionContext) ([]sdk.Finding, error) {
	result, err := Require(view, d.probe.ID)
	if err != nil {
		return nil, err
	}
	prober := proberJudgement(view, d.link)
	var findings []sdk.Finding
	for _, observed := range ClientLinks(result, d.link) {
		at := result.Readings[0].ObservedAt
		for _, reading := range result.Readings {
			if reading.Pod == observed.ClientPod {
				at = reading.ObservedAt
			}
		}
		finding, failing := traffic.LinkFindingIn(d.spec, view.Namespace(), ClientSetting(d.probe, observed.ClientPod, at), observed)
		if !failing {
			continue
		}
		finding.Fingerprint = d.spec.ID + "/" + finding.RuleID + "/" + observed.ClientPod
		if prober != "" {
			finding.Metadata["prober_condition"] = prober
			if prober == "healthy" {
				finding.Metadata["impairment"] = "client-egress"
				finding.Evidence += " The link-probe workload dials the same edge without fault, so the impairment is on the client Pod's side (its egress, network namespace or DNS)."
			}
		}
		findings = append(findings, finding)
	}
	sort.Slice(findings, func(i, j int) bool { return findings[i].Fingerprint < findings[j].Fingerprint })
	return findings, nil
}

// proberJudgement judges the same edge from every carried link-probe window:
// its condition, "healthy", or "" when no prober dials the edge.
func proberJudgement(view sdk.DetectionContext, link traffic.Link) string {
	set, ok := view.(traffic.WindowSet)
	if !ok {
		return ""
	}
	windows := set.TrafficWindows()
	names := make([]string, 0, len(windows))
	for name := range windows {
		names = append(names, name)
	}
	sort.Strings(names)
	verdict := ""
	for _, name := range names {
		window := windows[name]
		for _, observed := range window.Links {
			if observed.Link.From != link.From || observed.Link.To != link.To || observed.Link.Port != link.Port || !observed.Qualified {
				continue
			}
			if judged, failing := traffic.JudgeLink(window, observed); failing {
				return string(judged.Condition)
			}
			verdict = "healthy"
		}
	}
	return verdict
}
