package imagesrc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"sdo.dev/operator/plugindesc"
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
	// Concurrency is how many images of this source one ResolveAll call
	// resolves at once. Zero or one is one at a time, in the order given. A
	// source whose resolutions are independent and slow (image builds, each in
	// its own Job) sets more, bounded by what the cluster can run together.
	Concurrency int
}

// Catalog maps source IDs to registered sources. Build it once at wiring time;
// it is read-only after that and safe for concurrent ResolveAll calls.
type Catalog struct {
	entries   map[string]entry
	defaultID string
	pushHost  string
	// buildID and buildConfig make Derive's build declarations.
	buildID     string
	buildConfig func(contextPath, dockerfile string) (*anypb.Any, error)
}

type entry struct {
	desc           plugindesc.Descriptor
	typeURL        string
	newConfig      func() proto.Message
	resolve        func(context.Context, Request, proto.Message) (Resolved, error)
	budget         time.Duration
	sameRepository bool
	concurrency    int
	// inputs is the source's InputLister, when it implements one.
	inputs func(fsys fs.FS, m proto.Message) (BuildInputs, error)
}

// BuildInputs are the tree paths one image's bytes are made from: a changed
// file at or below any of them can change the image. A Path of "." is the
// whole tree. Paths are tree-relative and slash-form.
type BuildInputs struct {
	Paths []string
}

// Covers reports whether a change to the tree-relative file p can change the
// image.
func (b BuildInputs) Covers(p string) bool {
	for _, input := range b.Paths {
		if input == "." || p == input || strings.HasPrefix(p, input+"/") {
			return true
		}
	}
	return false
}

// InputLister is implemented by a source that builds images from the tree and
// can say which tree paths a build reads, before building. Sources that read
// nothing from the tree (a prebuilt pin) do not implement it.
type InputLister[C proto.Message] interface {
	BuildInputs(fsys fs.FS, config C) (BuildInputs, error)
}

// NewCatalog returns an empty catalog.
func NewCatalog() *Catalog { return &Catalog{entries: map[string]entry{}} }

// RegisterOption adjusts one Register call.
type RegisterOption func(*registration)

type registration struct{ doc string }

// WithDoc sets the usage doc an agent reads in Describe: what the source does
// and when to choose it. The implementation package exports it as Doc.
func WithDoc(doc string) RegisterOption { return func(r *registration) { r.doc = doc } }

// Register binds id to s with config type C. C is the only config the id
// accepts; Any payloads of another type are rejected. The single type
// assertion to C lives here so no other package asserts a config type.
func Register[C proto.Message](c *Catalog, id string, s Source[C], o Options, opts ...RegisterOption) error {
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
	if o.Concurrency < 0 {
		return fmt.Errorf("imagesrc: source %q has a negative concurrency", id)
	}
	var zero C
	if any(zero) == nil {
		return fmt.Errorf("imagesrc: source %q needs a concrete message type, not an interface", id)
	}
	var reg registration
	for _, opt := range opts {
		opt(&reg)
	}
	mt := zero.ProtoReflect().Type()
	desc, err := plugindesc.Describe(id, mt.Descriptor(), reg.doc)
	if err != nil {
		return fmt.Errorf("imagesrc: source %q: %w", id, err)
	}
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
		desc:      desc,
		typeURL:   typeURL,
		newConfig: func() proto.Message { return mt.New().Interface() },
		resolve: func(ctx context.Context, req Request, m proto.Message) (Resolved, error) {
			//assertcheck:ok C is the resolver's own generic message type
			return s.Resolve(ctx, req, m.(C))
		},
		budget:         o.Budget,
		sameRepository: o.SameRepository,
		concurrency:    max(1, o.Concurrency),
	}
	if lister, ok := any(s).(InputLister[C]); ok {
		e := c.entries[id]
		e.inputs = func(fsys fs.FS, m proto.Message) (BuildInputs, error) {
			//assertcheck:ok C is the lister's own generic message type
			return lister.BuildInputs(fsys, m.(C))
		}
		c.entries[id] = e
	}
	return nil
}

// BuildInputs returns the tree paths the declared image is built from, read
// from fsys. ok is false when the image's source builds nothing from the tree.
func (c *Catalog) BuildInputs(d Declared, fsys fs.FS) (inputs BuildInputs, ok bool, err error) {
	e, found := c.entries[d.SourceID]
	if !found || e.inputs == nil {
		return BuildInputs{}, false, nil
	}
	config, err := c.Decode(d.SourceID, d.Config)
	if err != nil {
		return BuildInputs{}, false, err
	}
	inputs, err = e.inputs(fsys, config)
	return inputs, err == nil, err
}

// SetDefault names the source used for images no Declared entry covers.
func (c *Catalog) SetDefault(id string) error {
	if _, ok := c.entries[id]; !ok {
		return fmt.Errorf("imagesrc: default source %q is not registered", id)
	}
	c.defaultID = id
	return nil
}

// SetTrustedPushHost names the push target the operator pushes built images
// to: a registry host with an optional repository path prefix
// (SplitRegistryTarget). ResolveAll then rejects an undeclared image under it.
func (c *Catalog) SetTrustedPushHost(host string) error {
	if host == "" || strings.ContainsAny(host, "@ ") || strings.HasPrefix(host, "/") || strings.HasSuffix(host, "/") || strings.Contains(host, "//") {
		return fmt.Errorf("imagesrc: push target %q must be a registry host with an optional path prefix", host)
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

// Describe lists what an agent may choose from, sorted by ID: each source's
// ID, config message, JSON Schema and doc. A nil catalog describes nothing.
func (c *Catalog) Describe() []plugindesc.Descriptor {
	if c == nil {
		return nil
	}
	out := make([]plugindesc.Descriptor, 0, len(c.entries))
	for _, id := range c.IDs() {
		out = append(out, c.entries[id].desc)
	}
	return out
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
