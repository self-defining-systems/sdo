package prober

import (
	"context"
	"fmt"
	"net"
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

// dialLink makes one fresh TCP connection to the link's target and closes
// it at once: no pooled or established connection can answer for the link.
func (p *Prober) dialLink(ctx context.Context, workload traffic.Workload, state *linkState) {
	started := p.config.Clock.Now()
	dialCtx, cancel := context.WithTimeout(ctx, workload.Timeout.Duration())
	connection, err := p.config.DialContext(dialCtx, "tcp", p.config.LinkAddress(state.link))
	cancel()
	if ctx.Err() != nil {
		return // shutting down: an aborted dial says nothing about the link
	}
	sample := traffic.LinkSample{At: started.UTC(), Duration: p.config.Clock.Now().Sub(started), OK: err == nil}
	if err != nil {
		sample.Error = err.Error()
	} else {
		_ = connection.Close()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	state.samples = append(state.samples, sample)
	if len(state.samples) > p.config.Capacity {
		state.samples = append([]traffic.LinkSample(nil), state.samples[len(state.samples)-p.config.Capacity:]...)
	}
	if sample.OK {
		state.qualified = true
	}
}

// linkWindowLocked snapshots a link-probe workload; p.mu must be held.
func (p *Prober) linkWindowLocked(probe *linkProbe, now time.Time) traffic.Window {
	links := make([]traffic.LinkObservations, 0, len(probe.links))
	for _, state := range probe.links {
		links = append(links, traffic.LinkObservations{
			Link: state.link, Samples: append([]traffic.LinkSample(nil), state.samples...), Qualified: state.qualified,
		})
	}
	return traffic.Window{Workload: probe.workload, ObservedAt: now, Links: links}
}
