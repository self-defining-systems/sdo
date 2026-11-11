package prober

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"sdo.dev/operator/sdk/traffic"
)

func TestParseCPUPressureReadsSomeAvg10(t *testing.T) {
	got, ok := ParseCPUPressure("some avg10=38.61 avg60=12.70 avg300=15.19 total=76412393994\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n")
	if !ok || got != 38.61 {
		t.Fatalf("stall = %v, %v; want 38.61", got, ok)
	}
	for _, text := range []string{"", "full avg10=1.00\n", "some avg10=x\n"} {
		if _, ok := ParseCPUPressure(text); ok {
			t.Errorf("%q parsed", text)
		}
	}
}

type stoppedClock struct{ now time.Time }

func (c stoppedClock) Now() time.Time                       { return c.now }
func (c stoppedClock) After(time.Duration) <-chan time.Time { return make(chan time.Time) }

// Each link dial records the host-wide CPU stall the prober reads, so the
// judgement can tell a stalled host from a failing link.
func TestLinkDialRecordsHostCPUStall(t *testing.T) {
	workload := traffic.Workload{Name: "links", Purpose: traffic.PurposeLinkProbe, Timeout: traffic.Duration(time.Second)}
	p, err := New(Config{
		Namespace: "app", Clock: stoppedClock{time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)},
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.Join(syscall.ECONNREFUSED)}
		},
		HostCPUStall: func() (float64, bool) { return 41.5, true },
	})
	if err != nil {
		t.Fatal(err)
	}
	state := &linkState{link: traffic.Link{From: "web", To: "store", Port: 5432}}
	p.dialLink(context.Background(), workload, state)
	if len(state.samples) != 1 || state.samples[0].HostCPUStall != 41.5 {
		t.Fatalf("samples = %+v, want one dial with host stall 41.5", state.samples)
	}
}
