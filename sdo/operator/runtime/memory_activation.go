package runtime

import (
	"context"
	"fmt"
	"sort"
	"time"

	"sdo.dev/operator/core"
	"sdo.dev/operator/memoryhost"
	"sdo.dev/operator/sdk"
)

// ReconfigureMemory changes future evaluations only. Finding persistence,
// open incidents, retained bindings and in-flight responder loops survive.
func (c *Controller) ReconfigureMemory(remote *RemoteMemory, provider SnapshotProvider, at time.Time) error {
	if err := core.ValidateDetectors(remote.Detectors); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := NewScheduler(remote.Detectors, at)
	byID := map[string]sdk.Detector{}
	specs := map[string]sdk.DetectorSpec{}
	var health []string
	for _, d := range remote.Detectors {
		spec := d.Spec()
		byID[spec.ID] = d
		specs[spec.ID] = spec
		if old := c.scheduler.entries[spec.ID]; old != nil && old.nextRun.Before(at) {
			entries.entries[spec.ID].nextRun = old.nextRun
		}
		delete(c.tracker.policies, spec.ID)
		if spec.Persistence.Firing > 0 && spec.Persistence.Clearing > 0 {
			c.tracker.SetPolicy(spec.ID, spec.Persistence.Firing, spec.Persistence.Clearing, spec.Persistence.MinDuration)
		}
		if spec.Class == sdk.DetectorClassHealth {
			health = append(health, spec.ID)
		}
	}
	sort.Strings(health)
	c.scheduler = entries
	c.detectors = byID
	c.detectorSpecs = specs
	c.healthDetectorIDs = health
	c.provider = provider
	if c.drift != nil {
		c.drift.Programs = remote.Programs
	}
	return nil
}

type memoryCandidate struct {
	host           memoryhost.Host
	commit, digest string
	binary         string
	started        time.Time
	close          func()
	err            error
}

// memoryActivation compiles outside the controller loop. Only the loop installs
// catalogs and acknowledges the durable activation, after the mirror barrier.
type memoryActivation struct {
	manager *memoryhost.Manager
	active  string
	pending string
	results chan memoryCandidate
	notify  chan struct{}
	current *memoryCandidate
	cancel  context.CancelFunc
	done    chan struct{}
	build   func(context.Context, string) memoryCandidate
	install func(context.Context, memoryCandidate) error
}

func (a *memoryActivation) refresh(ctx context.Context, commit string) (bool, error) {
	if a.pending != "" {
		select {
		case candidate := <-a.results:
			a.pending = ""
			if candidate.commit != commit {
				if candidate.host != nil {
					_ = candidate.host.Close()
				}
				if candidate.close != nil {
					candidate.close()
				}
				break
			}
			if candidate.err != nil {
				if candidate.close != nil {
					candidate.close()
				}
				return false, candidate.err
			}

			if err := a.manager.Prepare(ctx, candidate.host); err != nil {
				candidate.close()
				return false, err
			}
			if err := a.install(ctx, candidate); err != nil {
				_ = candidate.host.Close()
				candidate.close()
				return false, fmt.Errorf("activate memory %s: %w", commit, err)
			}
			if a.current != nil {
				a.current.close()
			}
			a.current = &candidate
			a.active = commit
			// The process has loaded the executable. Recovery retains its immutable
			// workspace until the next activation or controller shutdown.
		default:
			return false, nil
		}
	}
	if a.active == commit {
		return true, nil
	}
	a.pending = commit
	buildCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	a.cancel = cancel
	done := make(chan struct{})
	a.done = done
	go func() {
		defer close(done)
		candidate := a.build(buildCtx, commit)

		select {
		case a.results <- candidate:
			select {
			case a.notify <- struct{}{}:
			default:
			}
		case <-ctx.Done():
			if candidate.host != nil {
				_ = candidate.host.Close()
			}
			if candidate.close != nil {
				candidate.close()
			}
		}
	}()
	return false, nil
}

func memoryActivationNotifications(a *memoryActivation) <-chan struct{} {
	if a == nil {
		return nil
	}
	return a.notify
}
func (a *memoryActivation) close() {
	if a.cancel != nil {
		a.cancel()
	}
	if a.done != nil {
		<-a.done
	}
	select {
	case candidate := <-a.results:
		if candidate.host != nil {
			_ = candidate.host.Close()
		}
		if candidate.close != nil {
			candidate.close()
		}
	default:
	}
	if a.current != nil {
		a.current.close()
	}
}
func (a *memoryActivation) acceptedCommit() string {
	if a.current != nil {
		return a.current.commit
	}
	return a.active
}
func (a *memoryActivation) acceptedDigest(initial string) string {
	if a.current != nil {
		return a.current.digest
	}
	return initial
}
