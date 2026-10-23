package prober

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"sync"
	"syscall"
	"time"

	"sdo.dev/operator/sdk/servicehealth"
	"sdo.dev/operator/sdk/traffic"
)

// PathDials serves one bounded round of Service reachability dials. The
// prober keeps no reachability state and lists nothing: the controller
// derives the targets from its namespace snapshot, sends them each round,
// and keeps the history. The pod therefore needs no Kubernetes credentials.
const PathDials = "/v1/dials"

const (
	// MaxDialConcurrency bounds simultaneous dials in one round, so a round
	// of MaxReachabilityTargets blocked targets lasts at most four timeouts.
	MaxDialConcurrency = 16
	// MaxDialTimeout bounds one dial of a round.
	MaxDialTimeout = 5 * time.Second
)

var serviceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// DialRequest asks for one fresh TCP dial of each target.
type DialRequest struct {
	Targets []servicehealth.ReachabilityTarget `json:"targets"`
	// Timeout bounds each dial in nanoseconds; zero selects
	// servicehealth.DefaultReachabilityTimeout.
	Timeout time.Duration `json:"timeout,omitempty"`
}

// DialResult is one target's sample.
type DialResult struct {
	Target servicehealth.ReachabilityTarget `json:"target"`
	Sample servicehealth.DialSample         `json:"sample"`
}

// DialResponse holds one result per requested target, in request order.
type DialResponse struct {
	Results []DialResult `json:"results"`
}

// DialTargets dials every target once through cluster DNS, concurrently and
// with a fresh connection that is closed at once.
func (p *Prober) DialTargets(ctx context.Context, request DialRequest) (DialResponse, error) {
	if len(request.Targets) == 0 || len(request.Targets) > servicehealth.MaxReachabilityTargets {
		return DialResponse{}, fmt.Errorf("a dial round needs 1 to %d targets", servicehealth.MaxReachabilityTargets)
	}
	for _, target := range request.Targets {
		if !serviceNamePattern.MatchString(target.Service) || target.Port < 1 || target.Port > 65535 {
			return DialResponse{}, fmt.Errorf("dial target %s must name a Service and a valid port", target.ID())
		}
	}
	timeout := request.Timeout
	if timeout == 0 {
		timeout = servicehealth.DefaultReachabilityTimeout
	}
	if timeout < 0 || timeout > MaxDialTimeout {
		return DialResponse{}, fmt.Errorf("dial timeout must be in (0, %s]", MaxDialTimeout)
	}
	results := make([]DialResult, len(request.Targets))
	slots := make(chan struct{}, MaxDialConcurrency)
	var running sync.WaitGroup
	for index, target := range request.Targets {
		running.Add(1)
		go func(index int, target servicehealth.ReachabilityTarget) {
			defer running.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			results[index] = DialResult{Target: target, Sample: p.dialTarget(ctx, target, timeout)}
		}(index, target)
	}
	running.Wait()
	if err := ctx.Err(); err != nil {
		return DialResponse{}, err // an aborted round says nothing about the targets
	}
	return DialResponse{Results: results}, nil
}

func (p *Prober) dialTarget(ctx context.Context, target servicehealth.ReachabilityTarget, timeout time.Duration) servicehealth.DialSample {
	started := p.config.Clock.Now()
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	connection, err := p.config.DialContext(dialCtx, "tcp", p.config.LinkAddress(traffic.Link{To: target.Service, Port: int(target.Port)}))
	sample := servicehealth.DialSample{At: started.UTC(), Duration: p.config.Clock.Now().Sub(started), OK: err == nil}
	if err != nil {
		sample.Class, sample.Error = ClassifyDialError(err), err.Error()
	} else {
		_ = connection.Close()
	}
	return sample
}

// ClassifyDialError names why a dial failed: refused (nothing accepts on the
// port, or kube-proxy rejects it), timeout (dropped or unanswered), dns, reset,
// unreachable, or other.
func ClassifyDialError(err error) servicehealth.DialErrorClass {
	var dnsError *net.DNSError
	var netError net.Error
	switch {
	case errors.As(err, &dnsError):
		return servicehealth.DialDNS
	case errors.Is(err, syscall.ECONNREFUSED):
		return servicehealth.DialRefused
	case errors.Is(err, syscall.ECONNRESET):
		return servicehealth.DialReset
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH):
		return servicehealth.DialUnreachable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded),
		errors.As(err, &netError) && netError.Timeout():
		return servicehealth.DialTimeout
	}
	return servicehealth.DialOther
}
