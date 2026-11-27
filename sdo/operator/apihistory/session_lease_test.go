package apihistory

import (
	"errors"
	"testing"
	"time"
)

// An exec or attach stream that stays open after its upgrade never blocks
// observation: the controller sees it as an open session, not a pending write,
// and may fence. Its client bytes are refused from the fence on.
func TestAnEstablishedSessionDoesNotBlockTheFence(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	subject := LeaseSubject{Verb: "exec", Object: "pods/web-0"}
	session, err := gate.BeginSession(subject)
	if err != nil {
		t.Fatal(err)
	}
	if observation, err := gate.TryObserve(); err != nil || observation != nil {
		t.Fatalf("the handshake is a write in flight: %v %v", observation, err)
	}
	if entries := gate.InFlight(); len(entries) != 1 || entries[0].Subject != subject || !entries[0].Session || entries[0].Started.IsZero() {
		t.Fatalf("in flight: %+v", entries)
	}
	session.Established()
	if n, err := session.Forward(func() (int, error) { return 3, nil }); n != 3 || err != nil {
		t.Fatal(n, err)
	}
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("an established session blocked observation: %v", err)
	}
	if observation.Pending || len(observation.OpenSessions) != 1 || observation.OpenSessions[0].Subject != subject {
		t.Fatalf("observation: %+v", observation)
	}
	if err := observation.Fence(time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	observation.Close()
	// The proxy closes the stream when Fenced fires (kubeproxy's integration test).
	called := false
	if _, err := session.Forward(func() (int, error) { called = true; return 1, nil }); !errors.Is(err, ErrSessionFenced) || called {
		t.Fatalf("a fenced session forwarded bytes: %v", err)
	}
	if err := session.Finish(false); err != nil {
		t.Fatal(err)
	}
	observation, err = gate.TryObserve()
	if err != nil || observation == nil || observation.Pending || len(observation.OpenSessions) != 0 {
		t.Fatalf("after finish: %+v %v", observation, err)
	}
	observation.Close()
}

// A write lease names what it covers, so a blocked observation can say why.
func TestAWriteInFlightIsNamed(t *testing.T) {
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := gate.BeginWrite(LeaseSubject{Verb: "patch", Object: "deployments/web"})
	if err != nil {
		t.Fatal(err)
	}
	entries := gate.InFlight()
	if len(entries) != 1 || entries[0].Subject.Object != "deployments/web" || entries[0].Session || !entries[0].Pending {
		t.Fatalf("%+v", entries)
	}
	if err := lease.Finish(false); err != nil {
		t.Fatal(err)
	}
	if entries := gate.InFlight(); len(entries) != 0 {
		t.Fatalf("%+v", entries)
	}
}
