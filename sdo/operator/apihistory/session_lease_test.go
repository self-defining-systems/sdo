package apihistory

import (
	"errors"
	"testing"
	"time"
)

// testClock is a settable clock for session activity.
type testClock struct{ now time.Time }

func (c *testClock) Now() time.Time { return c.now }

func sessionGate(t *testing.T) (*MutationGate, *testClock) {
	t.Helper()
	gate, err := PrepareMutationGate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	clock := &testClock{now: time.Unix(1000, 0).UTC()}
	gate.Clock = clock.Now
	return gate, clock
}

func observeOpen(t *testing.T, gate *MutationGate) *MutationObservation {
	t.Helper()
	observation, err := gate.TryObserve()
	if err != nil || observation == nil {
		t.Fatalf("an established session blocked observation: %v", err)
	}
	return observation
}

// An exec or attach stream that stays open after its upgrade never blocks
// observation: the controller sees it as an open session with its last
// activity, not as a pending write.
func TestAnEstablishedSessionDoesNotBlockTheFence(t *testing.T) {
	gate, clock := sessionGate(t)
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
	t.Cleanup(func() { _ = session.Finish(false) })
	observation := observeOpen(t, gate)
	defer observation.Close()
	if observation.Pending || len(observation.OpenSessions) != 1 || observation.OpenSessions[0].Subject != subject {
		t.Fatalf("observation: %+v", observation)
	}
	if active, _, ok := observation.LastSessionActivity(); !ok || !active.Equal(clock.now) {
		t.Fatalf("established at %s, last activity %s %v", clock.now, active, ok)
	}
}

// Bytes in either direction are activity: the journal keeps the newest, and
// activity alone never changes the journal revision (it is not a new write).
func TestSessionActivityIsJournalled(t *testing.T) {
	gate, clock := sessionGate(t)
	session, err := gate.BeginSession(LeaseSubject{Verb: "exec", Object: "pods/web-0"})
	if err != nil {
		t.Fatal(err)
	}
	session.Established()
	t.Cleanup(func() { _ = session.Finish(false) })
	observation := observeOpen(t, gate)
	revision := observation.Revision
	observation.Close()

	clock.now = clock.now.Add(20 * time.Second)
	if n, err := session.Forward(nil, func() (int, error) { return 3, nil }); n != 3 || err != nil {
		t.Fatal(n, err)
	}
	observation = observeOpen(t, gate)
	if active, _, _ := observation.LastSessionActivity(); !active.Equal(clock.now) {
		t.Fatalf("client bytes at %s, last activity %s", clock.now, active)
	}
	if observation.Revision != revision {
		t.Fatal("session activity changed the journal revision")
	}
	observation.Close()

	clock.now = clock.now.Add(20 * time.Second)
	session.Touch()
	observation = observeOpen(t, gate)
	defer observation.Close()
	if active, entry, _ := observation.LastSessionActivity(); !active.Equal(clock.now) || entry.Subject.Object != "pods/web-0" {
		t.Fatalf("output at %s, last activity %s of %+v", clock.now, active, entry)
	}
}

// A verification fence is provisional: a withdrawn recovery reopens writes, so
// client bytes sent under the fence are held, not refused, and go through once
// it is withdrawn. Only a terminal seal ends the stream.
func TestAFencedSessionIsHeldUntilTheFenceIsWithdrawnOrSealed(t *testing.T) {
	gate, _ := sessionGate(t)
	session, err := gate.BeginSession(LeaseSubject{Verb: "exec", Object: "pods/web-0"})
	if err != nil {
		t.Fatal(err)
	}
	poll := make(chan time.Time)
	session.poll = func() <-chan time.Time { return poll }
	session.Established()
	t.Cleanup(func() { _ = session.Finish(false) })
	fence := func() {
		observation := observeOpen(t, gate)
		if err := observation.Fence(time.Unix(1, 0)); err != nil {
			t.Fatal(err)
		}
		observation.Close()
	}
	// tick feeds the poll (Forward's and the watcher's) until done. The two
	// share it, so the one awaited may get few of the ticks: expecting done
	// takes a bound that only a stuck session can use up.
	const untilDone = 1 << 20
	tick := func(done <-chan struct{}, most int) bool {
		for range most {
			select {
			case poll <- time.Time{}:
			case <-done:
				return true
			}
		}
		return false
	}
	fence()
	forwarded, wrote := make(chan struct{}), make(chan struct{}, 1)
	var forwardErr error
	go func() {
		_, forwardErr = session.Forward(nil, func() (int, error) { wrote <- struct{}{}; return 1, nil })
		close(forwarded)
	}()
	if tick(forwarded, 5) || len(wrote) > 0 {
		t.Fatal("a fenced session forwarded bytes")
	}
	select {
	case <-session.Ended():
		t.Fatal("a provisional fence ended the session")
	default:
	}
	if err := gate.Reopen(); err != nil {
		t.Fatal(err)
	}
	if !tick(forwarded, untilDone) || forwardErr != nil || len(wrote) != 1 {
		t.Fatalf("a withdrawn fence still holds the session: %v", forwardErr)
	}

	fence()
	sealing := observeOpen(t, gate)
	if err := sealing.Seal(); err != nil {
		t.Fatal(err)
	}
	sealing.Close()
	called := false
	if _, err := session.Forward(nil, func() (int, error) { called = true; return 1, nil }); !errors.Is(err, ErrSessionFenced) || called {
		t.Fatalf("a sealed session forwarded bytes: %v", err)
	}
	if !tick(session.Ended(), untilDone) {
		t.Fatal("the seal did not end the session")
	}
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
