package render

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

var rendererID = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

// ErrUnknownRenderer marks a renderer ID that is not registered.
var ErrUnknownRenderer = errors.New("unknown renderer")

// Catalog maps renderer IDs to renderers. It is built explicitly by the
// wiring package; there is no global registry.
type Catalog struct {
	entries map[string]entry
	types   map[string]string // config type URL -> id
	limits  Limits
}

type entry struct {
	id        string
	typeURL   string
	newConfig func() proto.Message
	render    func(context.Context, FS, Env, proto.Message) (Result, error)
}

// NewCatalog returns an empty catalog with DefaultLimits.
func NewCatalog() *Catalog {
	return &Catalog{entries: map[string]entry{}, types: map[string]string{}, limits: DefaultLimits}
}

// WithLimits returns a catalog sharing c's entries with other read limits.
func (c *Catalog) WithLimits(l Limits) *Catalog {
	return &Catalog{entries: c.entries, types: c.types, limits: l.withDefaults()}
}

// Register binds id to r with config type C. C's message type is the only
// config the id accepts; Any payloads of another type are rejected. The single
// type assertion to C lives here, so no other package asserts a config type.
func Register[C proto.Message](c *Catalog, id string, r Renderer[C]) error {
	if !rendererID.MatchString(id) {
		return fmt.Errorf("renderer id %q must match %s", id, rendererID)
	}
	if r == nil {
		return fmt.Errorf("renderer %q is nil", id)
	}
	var zero C
	if any(zero) == nil {
		return fmt.Errorf("renderer %q: config type must be a concrete message pointer, not an interface", id)
	}
	mt := zero.ProtoReflect().Type()
	typeURL := "type.googleapis.com/" + string(mt.Descriptor().FullName())
	if _, dup := c.entries[id]; dup {
		return fmt.Errorf("renderer %q already registered", id)
	}
	if other, dup := c.types[typeURL]; dup {
		return fmt.Errorf("renderer %q: config type %s already belongs to %q", id, mt.Descriptor().FullName(), other)
	}
	c.entries[id] = entry{
		id:        id,
		typeURL:   typeURL,
		newConfig: func() proto.Message { return mt.New().Interface() },
		render: func(ctx context.Context, f FS, e Env, m proto.Message) (Result, error) {
			cfg, ok := m.(C)
			if !ok {
				return Result{}, fmt.Errorf("renderer %q: config is %T", id, m)
			}
			return r.Render(ctx, f, e, cfg)
		},
	}
	c.types[typeURL] = id
	return nil
}

// IDs lists registered renderer IDs, sorted.
func (c *Catalog) IDs() []string {
	ids := make([]string, 0, len(c.entries))
	for id := range c.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Decode checks that id is registered and config unpacks to its type. It is
// the static validation used for worker result admission.
func (c *Catalog) Decode(id string, config *anypb.Any) (proto.Message, error) {
	e, ok := c.entries[id]
	if !ok {
		return nil, fmt.Errorf("%w %q (registered: %v)", ErrUnknownRenderer, id, c.IDs())
	}
	if config == nil {
		return nil, fmt.Errorf("renderer %q: config is required (%s)", id, e.typeURL)
	}
	if config.GetTypeUrl() != e.typeURL {
		return nil, fmt.Errorf("renderer %q takes config %s, got %q", id, e.typeURL, config.GetTypeUrl())
	}
	msg := e.newConfig()
	if err := config.UnmarshalTo(msg); err != nil {
		return nil, fmt.Errorf("renderer %q: decode config: %w", id, err)
	}
	return msg, nil
}

// Render decodes the config, wraps fsys in a Recorder with the catalog's
// limits, calls the renderer, validates the result and sets Inputs.
func (c *Catalog) Render(ctx context.Context, fsys FS, env Env, id string, config *anypb.Any) (Result, error) {
	msg, err := c.Decode(id, config)
	if err != nil {
		return Result{}, err
	}
	rec := Record(fsys, c.limits)
	res, err := c.entries[id].render(ctx, rec, env, msg)
	if err != nil {
		return Result{}, fmt.Errorf("render %s: %w", id, err)
	}
	if err := validateResult(res, env); err != nil {
		return Result{}, fmt.Errorf("render %s: %w", id, err)
	}
	res.Inputs = rec.Inputs()
	return res, nil
}

func validateResult(res Result, env Env) error {
	if res.Inputs != nil {
		return errors.New("renderer set Inputs; the catalog owns them")
	}
	names := map[string]bool{}
	for i, p := range res.Phases {
		if p.Name == "" || names[p.Name] {
			return fmt.Errorf("phase %d name %q is empty or repeated", i, p.Name)
		}
		names[p.Name] = true
		for j, obj := range p.Objects {
			if obj == nil {
				return fmt.Errorf("phase %q object %d is nil", p.Name, j)
			}
			if err := validateObject(obj, env); err != nil {
				return fmt.Errorf("phase %q object %d: %w", p.Name, j, err)
			}
		}
	}
	return nil
}

func validateObject(obj *unstructured.Unstructured, env Env) error {
	if obj.GetAPIVersion() == "" || obj.GetKind() == "" || obj.GetName() == "" {
		return fmt.Errorf("needs apiVersion, kind and metadata.name (got %q %q %q)", obj.GetAPIVersion(), obj.GetKind(), obj.GetName())
	}
	if env.Secrets == SecretsOmit && obj.GetKind() == "Secret" && obj.GetAPIVersion() == "v1" {
		for _, field := range []string{"data", "stringData"} {
			if _, present := obj.Object[field]; present {
				return fmt.Errorf("Secret %s carries %s under SecretsOmit", obj.GetName(), field)
			}
		}
	}
	return nil
}
