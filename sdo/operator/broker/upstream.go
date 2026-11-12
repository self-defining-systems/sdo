package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	"sdo.dev/operator/broker/internal/processio"
)

// upstreamRef records the last fetched head of the shared remote branch in
// the lane's accepted repository.
const upstreamRef = "refs/sdo/upstream"

// upstreamTimeout bounds one fetch or push so an unreachable remote never
// holds the publishing lock for long.
const upstreamTimeout = 45 * time.Second

// maxUpstreamRetries bounds sync rounds when the remote moves while this
// lane pushes local history.
const maxUpstreamRetries = 5

// upstreamRaceDeadline bounds re-replays of one publication that keeps losing
// the push race to peers. Jittered backoff between attempts lets a lane whose
// round trip is slower than its peers' publication cadence win eventually.
var upstreamRaceDeadline = 3 * time.Minute

// Upstream is a shared Git remote branch that the lane's accepted repository
// replicates. Lanes of one application on any host publish through their own
// broker; the remote branch orders their publications. Env carries transport
// settings (for example GIT_SSH_COMMAND) and is never persisted.
type Upstream struct {
	URL    string
	Branch string
	Env    []string
}

// SSHEnv selects the identity and known hosts in a read-only directory, such
// as a mounted copy of the host's existing key. Nothing else from the user's
// ssh configuration applies.
func SSHEnv(dir string) []string {
	if dir == "" {
		return nil
	}
	identity, hosts := filepath.Join(dir, "identity"), filepath.Join(dir, "known_hosts")
	return []string{"GIT_SSH_COMMAND=ssh -F /dev/null -i " + identity + " -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=10 -o ServerAliveCountMax=3 -o StrictHostKeyChecking=yes -o UserKnownHostsFile=" + hosts}
}

func (u Upstream) validate() error {
	if strings.TrimSpace(u.URL) == "" || strings.TrimSpace(u.Branch) == "" {
		return errors.New("memory upstream requires a URL and a branch")
	}
	if strings.HasPrefix(u.URL, "-") {
		return fmt.Errorf("memory upstream URL %q must not start with '-'", u.URL)
	}
	if u.Branch == "main" || u.Branch == "master" {
		return fmt.Errorf("memory upstream branch %q is reserved; use one branch per run", u.Branch)
	}
	if _, err := gitText(context.Background(), "", "check-ref-format", "refs/heads/"+u.Branch); err != nil {
		return fmt.Errorf("memory upstream branch %q is not a valid branch name", u.Branch)
	}
	return nil
}

// errUpstreamMoved means a peer published to the remote branch after this
// publication synced; the publication is replayed onto the new head.
var errUpstreamMoved = errors.New("memory upstream branch moved during publish")

// SetUpstream makes the accepted repository replicate one remote branch. The
// remote branch must already exist: the run creates it from the lane seed.
func (b *Broker) SetUpstream(upstream Upstream) error {
	if err := upstream.validate(); err != nil {
		return err
	}
	upstream.Env = append([]string{"GIT_TERMINAL_PROMPT=0"}, upstream.Env...)
	b.upstream = &upstream
	return nil
}

// SyncUpstream exchanges accepted memory with the remote branch: it pushes
// local publications the remote lacks and fast-forwards to peers' ones. An
// unreachable remote is not an error; the lane keeps its local memory and the
// next broker operation retries.
func (b *Broker) SyncUpstream(ctx context.Context) error {
	if b.upstream == nil {
		return nil
	}
	return b.locked(func() error { return b.syncUpstream(ctx) })
}

// syncUpstream runs under the broker lock.
func (b *Broker) syncUpstream(ctx context.Context) error {
	if b.upstream == nil {
		return nil
	}
	for range maxUpstreamRetries {
		remote, err := b.fetchUpstream(ctx)
		if err != nil {
			b.upstreamUnavailable("fetch", err)
			return nil
		}
		local, err := gitText(ctx, b.target, "rev-parse", "HEAD")
		if err != nil {
			return err
		}
		switch {
		case remote == local:
			return nil
		case b.isAncestor(ctx, local, remote):
			if err := b.requireClean(ctx); err != nil {
				return err
			}
			if _, err := runGit(ctx, b.target, "merge", "--ff-only", "--quiet", remote); err != nil {
				return fmt.Errorf("fast-forward accepted memory to upstream: %w", err)
			}
			return nil
		case b.isAncestor(ctx, remote, local):
			// Local publications accepted while the remote was unreachable.
			if err := b.pushUpstream(ctx, b.target, local); errors.Is(err, errUpstreamMoved) {
				continue
			} else if err != nil {
				b.upstreamUnavailable("push", err)
			}
			return nil
		default:
			b.holdDiverged(ctx, local, remote)
			return nil
		}
	}
	return nil
}

// holdDiverged handles a local history and a remote branch that both moved
// since they last agreed (local publications made while the remote was
// unreachable, and a peer's or an engineer's push meanwhile). SDO never merges
// them: the local history stays this controller's accepted state, it is
// preserved on the remote side branch sdo-held/diverged-<commit> for a person,
// and the source branch is not touched.
func (b *Broker) holdDiverged(ctx context.Context, local, remote string) {
	side := "sdo-held/diverged-" + local[:12]
	if err := b.pushRef(ctx, b.target, local, side); err != nil {
		b.upstreamUnavailable("preserve diverged memory", err)
	}
	log.Printf("memory upstream: local accepted history %s and upstream %s diverged; kept locally and on branch %s for a person to merge", local, remote, side)
}

// fetchUpstream returns the remote branch head and records it at upstreamRef.
func (b *Broker) fetchUpstream(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	if _, err := runGitEnv(ctx, b.target, b.upstream.Env, "fetch", "--no-tags", "--quiet", "--", b.upstream.URL, "+refs/heads/"+b.upstream.Branch+":"+upstreamRef); err != nil {
		return "", err
	}
	return gitText(ctx, b.target, "rev-parse", upstreamRef)
}

// pushUpstream publishes commit from repository to the remote branch without
// force. A rejection because the branch moved returns errUpstreamMoved.
func (b *Broker) pushUpstream(ctx context.Context, repository, commit string) error {
	err := b.pushRef(ctx, repository, commit, b.upstream.Branch)
	if err == nil {
		return nil
	}
	// Rejection wording differs by transport and race (non-fast-forward, a
	// lost ref lock). A remote that still answers moved; one that does not is
	// unavailable.
	if remote, fetchErr := b.fetchUpstream(ctx); fetchErr == nil && remote != commit {
		return errUpstreamMoved
	}
	return err
}

func (b *Broker) pushRef(ctx context.Context, repository, commit, branch string) error {
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	_, err := runGitEnv(ctx, repository, b.upstream.Env, "push", "--quiet", "--", b.upstream.URL, commit+":refs/heads/"+branch)
	return err
}

// withUpstream runs one publication against the newest remote head and
// replays it when a peer's push wins the race, through the same replay and
// shared check as a concurrent local publication.
func (b *Broker) withUpstream(ctx context.Context, publish func() error) func() error {
	return func() error {
		deadline := processio.Now().Add(upstreamRaceDeadline)
		for attempt := 0; ; attempt++ {
			if err := b.syncUpstream(ctx); err != nil {
				return err
			}
			err := publish()
			if !errors.Is(err, errUpstreamMoved) || processio.Now().After(deadline) {
				return err
			}
			backoff := min(250*time.Millisecond<<min(attempt, 5), 8*time.Second)
			if err := processio.Backoff(ctx, backoff); err != nil {
				return err
			}
		}
	}
}

// replicate pushes a staged accepted commit before the local fast-forward.
// A moved remote branch aborts this attempt; an unreachable remote leaves the
// commit local until the next sync.
func (b *Broker) replicate(ctx context.Context, stage, commit string) error {
	if b.upstream == nil {
		return nil
	}
	err := b.pushUpstream(ctx, stage, commit)
	if errors.Is(err, errUpstreamMoved) {
		return err
	}
	if err != nil {
		b.upstreamUnavailable("push", err)
	}
	return nil
}

func (b *Broker) isAncestor(ctx context.Context, ancestor, descendant string) bool {
	_, err := runGit(ctx, b.target, "merge-base", "--is-ancestor", ancestor, descendant)
	return err == nil
}

func (b *Broker) upstreamUnavailable(operation string, err error) {
	log.Printf("memory upstream %s unavailable (%s); accepted memory stays local and the next broker operation retries: %v", b.upstream.Branch, operation, err)
}
