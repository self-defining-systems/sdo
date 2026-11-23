package deployment

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"sdo.dev/operator/evaluation"
	"sdo.dev/operator/lifecycle"
	"sdo.dev/operator/rendered"
)

// sessionVerdictBound is the longest a verdict waits for the watch to judge; past it the session verifies cold.
const sessionVerdictBound = 5 * time.Minute

// sessionWatchInterval is how often a session's watch judges the live application.
const sessionWatchInterval = 5 * time.Second

// sessionProbe is a probe that can compile before the deployment is known and then watch it
// (AcceptedDetectorProbe). A probe without it verifies after the rollout, as before.
type sessionProbe interface {
	Compile(ctx context.Context, acceptedRoot, operatorRoot string) (*CompiledProbe, error)
}

// BeginCandidate starts verifying commit while it rolls out: the accepted detectors compile at once, and
// once the deployer reports the candidate applied, a watch evaluation starts under its image pins, so the
// samples a verdict needs accumulate during the rollout. Verdict then reads the first evaluation judged after
// the rollout instead of waiting out a cold one. It implements lifecycle.SessionVerifier.
func (v IndependentHealthVerifier) BeginCandidate(ctx context.Context, root, commit, namespace, objective string) (lifecycle.CandidateSession, error) {
	probe := v.Probe
	if probe == nil {
		probe = AcceptedDetectorProbe{}
	}
	compiling, ok := probe.(sessionProbe)
	if !ok {
		return nil, lifecycle.ErrNoSession
	}
	if root == "" || v.OperatorRoot == "" || v.Readiness == nil || namespace == "" || commit == "" {
		return nil, lifecycle.ErrNoSession
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &candidateSession{verifier: v, root: root, commit: commit, namespace: namespace, objective: objective, cancel: cancel, compiled: make(chan struct{}), watchReady: make(chan struct{})}
	go func() {
		defer close(s.compiled)
		s.compiledProbe, s.compileErr = compiling.Compile(runCtx, root, v.OperatorRoot)
	}()
	s.runCtx = runCtx
	return s, nil
}

type candidateSession struct {
	verifier                           IndependentHealthVerifier
	root, commit, namespace, objective string
	runCtx                             context.Context
	cancel                             context.CancelFunc

	compiled      chan struct{}
	compiledProbe *CompiledProbe
	compileErr    error

	mu         sync.Mutex
	started    bool
	consumed   bool // Prepare took the compiled probe's workspace
	watchReady chan struct{}
	prepared   *PreparedProbe
	watcher    *Watcher
	watchErr   error
	lastJudged time.Time
}

// Applied starts the watch under pins once the detectors are compiled; it never waits for them.
func (s *candidateSession) Applied(_ context.Context, pins rendered.ReceiptPins) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()
	go func() {
		defer close(s.watchReady)
		<-s.compiled
		if s.compileErr != nil {
			s.setWatchErr(s.compileErr)
			return
		}
		s.mu.Lock()
		s.consumed = true
		s.mu.Unlock()
		prepared, err := s.compiledProbe.Prepare(s.namespace, HealthScope{Classes: evaluation.GatingClasses, Pins: pins})
		if err != nil {
			s.setWatchErr(err)
			return
		}
		s.mu.Lock()
		s.prepared = prepared
		s.watcher = prepared.Watch(s.runCtx, sessionWatchInterval)
		s.mu.Unlock()
	}()
	return nil
}

func (s *candidateSession) setWatchErr(err error) {
	s.mu.Lock()
	s.watchErr = err
	s.mu.Unlock()
}

// Verdict judges the rolled-out candidate from the watch: the same checks as VerifyCandidate around an
// evaluation that began after readyAt (and after the last verdict this session gave, so a re-evaluation is a
// new one). A session whose watch never ran or ended verifies cold, as VerifyCandidate does.
func (s *candidateSession) Verdict(ctx context.Context, source lifecycle.DeploymentReceipt, readyAt time.Time) (lifecycle.HealthVerdict, error) {
	checked, err := s.verifier.checkCandidate(ctx, s.root, s.commit, s.namespace, s.objective, source)
	if err != nil {
		return lifecycle.HealthVerdict{}, err
	}
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		// The deployment never said it applied (a deployer without the hook): nothing watched.
		return s.verifier.VerifyCandidate(ctx, s.root, s.commit, s.namespace, s.objective, source)
	}
	select {
	case <-s.watchReady:
	case <-ctx.Done():
		return lifecycle.HealthVerdict{}, ctx.Err()
	}
	s.mu.Lock()
	watcher, watchErr, notBefore := s.watcher, s.watchErr, s.lastJudged
	s.mu.Unlock()
	if watcher == nil {
		return s.verifier.VerifyCandidate(ctx, s.root, s.commit, s.namespace, s.objective, source)
	}
	if notBefore.Before(readyAt) {
		notBefore = readyAt
	}
	waiting, cancel := context.WithTimeout(ctx, sessionVerdictBound)
	defer cancel()
	judged, err := watcher.Next(waiting, notBefore)
	if err != nil {
		if ctx.Err() == nil {
			return s.verifier.VerifyCandidate(ctx, s.root, s.commit, s.namespace, s.objective, source)
		}
		return lifecycle.HealthVerdict{}, fmt.Errorf("session watch: %w", errors.Join(err, watchErr))
	}
	s.mu.Lock()
	s.lastJudged = judged.StartedAt.Add(time.Nanosecond)
	s.mu.Unlock()
	return checked.verdict(healthEvaluationOf(judged))
}

// Close stops the watch and releases the build workspace.
func (s *candidateSession) Close() {
	s.cancel()
	<-s.compiled
	select {
	case <-s.watchReady:
	default:
		s.mu.Lock()
		started := s.started
		s.mu.Unlock()
		if started {
			<-s.watchReady
		}
	}
	s.mu.Lock()
	watcher, prepared, consumed := s.watcher, s.prepared, s.consumed
	s.mu.Unlock()
	switch {
	case watcher != nil:
		watcher.Close()
		prepared.Close()
	case prepared != nil:
		prepared.Close()
	case s.compiledProbe != nil && !consumed:
		s.compiledProbe.Close()
	}
}
