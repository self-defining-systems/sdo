// Package upstream is the source-following seam of deployment. A Watcher
// reports the current revision of one followed upstream and makes a chosen
// revision available as a commit in a Git object store; the Catalog owns the
// ID and config-type routing around those calls. What to do with a new
// revision (follow, hold, advance) is core's decision, not the watcher's.
//
// The package imports only the standard library and protobuf. Implementations
// (gitpoll) live in subpackages and are wired in operatorcli/catalog.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// ErrUnavailable marks a transient failure to reach the upstream. The core
// retries it; every other error holds the source with a typed reason.
var ErrUnavailable = errors.New("upstream unavailable")

// ErrUnknownWatcher marks a watcher ID that is not registered.
var ErrUnknownWatcher = errors.New("unknown upstream watcher")

// Revision is one immutable upstream state (gitpoll: a commit ID).
type Revision struct {
	ID  string
	Ref string
}

// Watcher follows one kind of upstream, configured by C.
type Watcher[C proto.Message] interface {
	// Latest returns the current revision of the followed ref. Transient
	// failures wrap ErrUnavailable.
	Latest(ctx context.Context, config C) (Revision, error)
	// Materialize makes rev available as a commit in the Git object store at
	// gitDir and returns its ID. gitpoll fetches; a later chart-repo or
	// image-tag watcher would synthesize a vendored commit. Transient failures
	// wrap ErrUnavailable.
	Materialize(ctx context.Context, config C, rev Revision, gitDir string) (commit string, err error)
}

var watcherID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Catalog maps watcher IDs to watchers. It is built explicitly by the wiring
// package; there is no global registry.
type Catalog struct {
	entries map[string]entry
	types   map[string]string // config type URL -> id
}

type entry struct {
	typeURL     string
	newConfig   func() proto.Message
	latest      func(context.Context, proto.Message) (Revision, error)
	materialize func(context.Context, proto.Message, Revision, string) (string, error)
}

// NewCatalog returns an empty catalog.
func NewCatalog() *Catalog {
	return &Catalog{entries: map[string]entry{}, types: map[string]string{}}
}

// Register binds id to w with config type C. C's message type is the only
// config the id accepts; Any payloads of another type are rejected. The single
// type assertion to C lives here, so no other package asserts a config type.
func Register[C proto.Message](c *Catalog, id string, w Watcher[C]) error {
	if !watcherID.MatchString(id) {
		return fmt.Errorf("upstream watcher id %q must match %s", id, watcherID)
	}
	if w == nil {
		return fmt.Errorf("upstream watcher %q is nil", id)
	}
	var zero C
	if any(zero) == nil {
		return fmt.Errorf("upstream watcher %q: config type must be a concrete message pointer, not an interface", id)
	}
	mt := zero.ProtoReflect().Type()
	typeURL := "type.googleapis.com/" + string(mt.Descriptor().FullName())
	if _, dup := c.entries[id]; dup {
		return fmt.Errorf("upstream watcher %q already registered", id)
	}
	if other, dup := c.types[typeURL]; dup {
		return fmt.Errorf("upstream watcher %q: config type %s already belongs to %q", id, mt.Descriptor().FullName(), other)
	}
	c.entries[id] = entry{
		typeURL:   typeURL,
		newConfig: func() proto.Message { return mt.New().Interface() },
		latest: func(ctx context.Context, m proto.Message) (Revision, error) {
			cfg, ok := m.(C)
			if !ok {
				return Revision{}, fmt.Errorf("upstream watcher %q: config is %T", id, m)
			}
			return w.Latest(ctx, cfg)
		},
		materialize: func(ctx context.Context, m proto.Message, rev Revision, gitDir string) (string, error) {
			cfg, ok := m.(C)
			if !ok {
				return "", fmt.Errorf("upstream watcher %q: config is %T", id, m)
			}
			return w.Materialize(ctx, cfg, rev, gitDir)
		},
	}
	c.types[typeURL] = id
	return nil
}

// IDs lists registered watcher IDs, sorted.
func (c *Catalog) IDs() []string {
	ids := make([]string, 0, len(c.entries))
	for id := range c.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Decode checks that id is registered and config unpacks to its type. It is
// the static validation used when a followed source is read from memory.
func (c *Catalog) Decode(id string, config *anypb.Any) (proto.Message, error) {
	e, ok := c.entries[id]
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownWatcher, id, c.IDs())
	}
	if config == nil {
		return nil, fmt.Errorf("upstream watcher %q: config is required (%s)", id, e.typeURL)
	}
	if config.GetTypeUrl() != e.typeURL {
		return nil, fmt.Errorf("upstream watcher %q takes config %s, got %q", id, e.typeURL, config.GetTypeUrl())
	}
	msg := e.newConfig()
	if err := config.UnmarshalTo(msg); err != nil {
		return nil, fmt.Errorf("upstream watcher %q: decode config: %w", id, err)
	}
	return msg, nil
}

// Latest returns the current revision of the upstream id follows.
func (c *Catalog) Latest(ctx context.Context, id string, config *anypb.Any) (Revision, error) {
	msg, err := c.Decode(id, config)
	if err != nil {
		return Revision{}, err
	}
	rev, err := c.entries[id].latest(ctx, msg)
	if err != nil {
		return Revision{}, fmt.Errorf("upstream %s latest: %w", id, err)
	}
	if rev.ID == "" {
		return Revision{}, fmt.Errorf("upstream %s latest: watcher returned an empty revision ID", id)
	}
	return rev, nil
}

// Materialize makes rev a commit in the object store at gitDir and returns its
// ID.
func (c *Catalog) Materialize(ctx context.Context, id string, config *anypb.Any, rev Revision, gitDir string) (string, error) {
	msg, err := c.Decode(id, config)
	if err != nil {
		return "", err
	}
	commit, err := c.entries[id].materialize(ctx, msg, rev, gitDir)
	if err != nil {
		return "", fmt.Errorf("upstream %s materialize %s: %w", id, rev.ID, err)
	}
	if commit == "" {
		return "", fmt.Errorf("upstream %s materialize %s: watcher returned an empty commit ID", id, rev.ID)
	}
	return commit, nil
}
