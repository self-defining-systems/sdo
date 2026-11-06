package imagesrc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var sourceID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// Options are the catalog-enforced properties of one source.
type Options struct {
	// Budget bounds all resolutions of this source in one ResolveAll call. Zero
	// means no budget beyond the caller's context.
	Budget time.Duration
	// SameRepository requires every Ref to be in the repository of the
	// requested image (a builder pushes to the target it was given). A source
	// that pins tags, or rewrites to another repository, leaves it false.
	SameRepository bool
}

// Catalog maps source IDs to registered sources. Build it once at wiring time;
// it is read-only after that and safe for concurrent ResolveAll calls.
type Catalog struct {
	entries   map[string]entry
	defaultID string
	pushHost  string
}

type entry struct {
	typeURL        string
	newConfig      func() proto.Message
	resolve        func(context.Context, Request, proto.Message) (Resolved, error)
	budget         time.Duration
	sameRepository bool
}

// NewCatalog returns an empty catalog.
func NewCatalog() *Catalog { return &Catalog{entries: map[string]entry{}} }

// Register binds id to s with config type C. C is the only config the id
// accepts; Any payloads of another type are rejected. The single type
// assertion to C lives here so no other package asserts a config type.
func Register[C proto.Message](c *Catalog, id string, s Source[C], o Options) error {
	if c == nil || c.entries == nil {
		return errors.New("imagesrc: register into a catalog made by NewCatalog")
	}
	if !sourceID.MatchString(id) {
		return fmt.Errorf("imagesrc: source id %q must match %s", id, sourceID)
	}
	if s == nil {
		return fmt.Errorf("imagesrc: source %q is nil", id)
	}
	if o.Budget < 0 {
		return fmt.Errorf("imagesrc: source %q has a negative budget", id)
	}
	var zero C
	if any(zero) == nil {
		return fmt.Errorf("imagesrc: source %q needs a concrete message type, not an interface", id)
	}
	mt := zero.ProtoReflect().Type()
	typeURL := "type.googleapis.com/" + string(mt.Descriptor().FullName())
	if _, dup := c.entries[id]; dup {
		return fmt.Errorf("imagesrc: source %q is already registered", id)
	}
	for other, e := range c.entries {
		if e.typeURL == typeURL {
			return fmt.Errorf("imagesrc: config type %s is already owned by source %q", typeURL, other)
		}
	}
	c.entries[id] = entry{
		typeURL:   typeURL,
		newConfig: func() proto.Message { return mt.New().Interface() },
		resolve: func(ctx context.Context, req Request, m proto.Message) (Resolved, error) {
			return s.Resolve(ctx, req, m.(C))
		},
		budget:         o.Budget,
		sameRepository: o.SameRepository,
	}
	return nil
}

// SetDefault names the source used for images no Declared entry covers.
func (c *Catalog) SetDefault(id string) error {
	if _, ok := c.entries[id]; !ok {
		return fmt.Errorf("imagesrc: default source %q is not registered", id)
	}
	c.defaultID = id
	return nil
}

// SetTrustedPushHost names the registry host the operator pushes built images
// to. ResolveAll then rejects an undeclared image under that host.
func (c *Catalog) SetTrustedPushHost(host string) error {
	if host == "" || strings.ContainsAny(host, "/@ ") {
		return fmt.Errorf("imagesrc: push host %q must be a bare registry host", host)
	}
	c.pushHost = host
	return nil
}

// Default returns the default source ID, or "" when unset.
func (c *Catalog) Default() string { return c.defaultID }

// Decode checks that id is registered and that config unpacks to its type. A
// nil config yields the empty config of that type. Used for static validation
// (worker result admission) as well as by ResolveAll.
func (c *Catalog) Decode(id string, config *anypb.Any) (proto.Message, error) {
	e, ok := c.entries[id]
	if !ok {
		return nil, fmt.Errorf("imagesrc: unknown source %q (registered: %v)", id, c.IDs())
	}
	m := e.newConfig()
	if config == nil {
		return m, nil
	}
	if config.GetTypeUrl() != e.typeURL {
		return nil, fmt.Errorf("imagesrc: source %q takes config %s, got %q", id, e.typeURL, config.GetTypeUrl())
	}
	if err := config.UnmarshalTo(m); err != nil {
		return nil, fmt.Errorf("imagesrc: source %q config: %w", id, err)
	}
	return m, nil
}

// IDs lists registered source IDs in sorted order.
func (c *Catalog) IDs() []string {
	ids := make([]string, 0, len(c.entries))
	for id := range c.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return slices.Clone(ids)
}
