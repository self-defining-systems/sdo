package prober

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

// LinkAddress addresses a link's target Service through cluster DNS.
func LinkAddress(link traffic.Link, namespace string, clusterDomain string) string {
	return net.JoinHostPort(fmt.Sprintf("%s.%s.svc.%s", link.To, namespace, clusterDomain), strconv.Itoa(link.Port))
}

type linkState struct {
	link      traffic.Link
	samples   []traffic.LinkSample
	qualified bool
	healthy   []time.Duration
	baseline  time.Duration
}

type linkProbe struct {
	workload traffic.Workload
	links    []*linkState
}

func newLinkProbe(workload traffic.Workload) *linkProbe {
	probe := &linkProbe{workload: workload}
	for _, link := range workload.Links {
		probe.links = append(probe.links, &linkState{link: link})
	}
	return probe
}

// runLinks probes every link of a link-probe workload until ctx ends. Each
// link has its own loop, so one blocked edge (its dial lasts the whole
// timeout) never delays the others. Probes start on a fixed cadence (start
// to start), and a probe of a link starts only after the previous one of the
// same link finished, so a blocked link is sampled every max(interval,
// timeout), not every interval plus timeout.
func (p *Prober) runLinks(ctx context.Context, probe *linkProbe) {
	var running sync.WaitGroup
	for _, state := range probe.links {
		running.Add(1)
		go func(state *linkState) {
			defer running.Done()
			interval := probe.workload.Interval.Duration()
			for {
				started := p.config.Clock.Now()
				p.dialLink(ctx, probe.workload, state)
				wait := interval - p.config.Clock.Now().Sub(started)
				if wait <= 0 {
					if ctx.Err() != nil {
						return
					}
					continue
				}
				select {
				case <-ctx.Done():
					return
				case <-p.config.Clock.After(wait):
				}
			}
		}(state)
	}
	running.Wait()
}

// dialLink opens one fresh connection and checks its declared protocol within
// one timeout. No pooled or established connection can answer for the link.
func (p *Prober) dialLink(ctx context.Context, workload traffic.Workload, state *linkState) {
	started := p.config.Clock.Now()
	dialCtx, cancel := context.WithTimeout(ctx, workload.Timeout.Duration())
	connection, err := p.config.DialContext(dialCtx, "tcp", p.config.LinkAddress(state.link))
	defer cancel()
	sample := traffic.LinkSample{At: started.UTC()}
	if err == nil {
		deadline, _ := dialCtx.Deadline()
		_ = connection.SetDeadline(deadline)
		stop := context.AfterFunc(dialCtx, func() { _ = connection.Close() })
		observed := &linkConnection{Conn: connection}
		sample.Status, sample.Excerpt, sample.Class, err = probeProtocol(dialCtx, observed, state.link)
		if err != nil && observed.failureClass() != "" {
			sample.Class = observed.failureClass()
		}
		stop()
		_ = connection.Close()
	}
	if ctx.Err() != nil {
		return
	}
	sample.Status = bounded(sample.Status)
	sample.Excerpt = bounded(sample.Excerpt)
	sample.Duration = p.config.Clock.Now().Sub(started)
	sample.OK = err == nil
	if err != nil {
		sample.Error = bounded(err.Error())
		if sample.Class == "" {
			sample.Class = linkErrorClass(err)
			// A clean EOF or malformed reply after connecting is a protocol failure.
			if connection != nil && sample.Class == traffic.LinkFailureTransport {
				sample.Class = traffic.LinkFailureProtocol
			}
		}
		if dialCtx.Err() != nil {
			sample.Class = traffic.LinkFailureTimeout
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state.record(sample, p.config.Capacity)
}

// record keeps the newest capacity samples. The healthy baseline is the
// median of the first LinkBaselineSamples successful dials and never moves
// afterwards, so a later slow period cannot become the norm it is judged by.
func (state *linkState) record(sample traffic.LinkSample, capacity int) {
	state.samples = append(state.samples, sample)
	if len(state.samples) > capacity {
		state.samples = append([]traffic.LinkSample(nil), state.samples[len(state.samples)-capacity:]...)
	}
	if !sample.OK {
		return
	}
	state.qualified = true
	if state.baseline == 0 {
		state.healthy = append(state.healthy, sample.Duration)
		if len(state.healthy) == traffic.LinkBaselineSamples {
			slices.Sort(state.healthy)
			state.baseline = max(state.healthy[(len(state.healthy)-1)/2], time.Nanosecond)
			state.healthy = nil
		}
	}
}

// linkWindowLocked snapshots a link-probe workload; p.mu must be held.
func (p *Prober) linkWindowLocked(probe *linkProbe, now time.Time) traffic.Window {
	links := make([]traffic.LinkObservations, 0, len(probe.links))
	for _, state := range probe.links {
		links = append(links, traffic.LinkObservations{
			Link: state.link, Samples: append([]traffic.LinkSample(nil), state.samples...), Qualified: state.qualified,
			BaselineLatency: state.baseline,
		})
	}
	return traffic.Window{Workload: probe.workload, ObservedAt: now, Links: links}
}
