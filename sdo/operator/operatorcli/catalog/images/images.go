// Package images is the single wiring point for image sources. Only the
// operator CLI imports it: the controller binary links renderers, not the
// BuildKit Kubernetes client or the registry client.
package images

import (
	"errors"
	"fmt"
	"time"

	v1alpha1 "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/imagesrc"
	"sdo.dev/operator/imagesrc/buildkit"
	"sdo.dev/operator/imagesrc/prebuilt"
)

// ResolveBudget bounds all prebuilt resolutions of one apply.
const ResolveBudget = 90 * time.Second

// Config is everything the built-in image sources need from the operator.
type Config struct {
	// Prebuilt resolves tags of images that already exist in a registry; nil
	// means a zero Source (guarded public client, no cluster, no insecure host).
	Prebuilt *prebuilt.Source
	// Builder builds images from the proposal checkout. Its AllowedRegistryHost
	// is the trusted push host: undeclared images under it are rejected.
	Builder buildkit.Builder
	// BuildBudget bounds all builds of one apply; zero means no budget beyond
	// the caller's context.
	BuildBudget time.Duration
}

// Catalog returns a catalog holding every built-in image source: prebuilt
// (the default) and buildkit. Unlike renderers, the sources are built from
// runtime configuration, so an invalid Config is an error, not a panic.
func Catalog(cfg Config) (*imagesrc.Catalog, error) {
	if cfg.Builder.AllowedRegistryHost == "" {
		return nil, errors.New("images: Config.Builder.AllowedRegistryHost (the trusted push host) is required")
	}
	if cfg.BuildBudget < 0 {
		return nil, errors.New("images: Config.BuildBudget must not be negative")
	}
	pre := cfg.Prebuilt
	if pre == nil {
		pre = &prebuilt.Source{}
	}
	c := imagesrc.NewCatalog()
	if err := imagesrc.Register[*v1alpha1.PrebuiltImageConfig](c, prebuilt.ID, pre, imagesrc.Options{Budget: ResolveBudget}); err != nil {
		return nil, fmt.Errorf("images: register %s: %w", prebuilt.ID, err)
	}
	if err := imagesrc.Register[*v1alpha1.BuildKitImageConfig](c, buildkit.ID, cfg.Builder, imagesrc.Options{Budget: cfg.BuildBudget, SameRepository: true}); err != nil {
		return nil, fmt.Errorf("images: register %s: %w", buildkit.ID, err)
	}
	if err := c.SetDefault(prebuilt.ID); err != nil {
		return nil, err
	}
	if err := c.SetTrustedPushHost(cfg.Builder.AllowedRegistryHost); err != nil {
		return nil, fmt.Errorf("images: %w", err)
	}
	return c, nil
}
