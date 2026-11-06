package memory

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/upstream"
)

// ReadSources loads the followed upstream sources from the accepted checkout.
// An absent file follows nothing. Every source is decoded into its watcher's
// registered config type, and its poll interval is read from that config, so a
// source the catalog cannot follow fails here rather than at the first poll.
func ReadSources(checkout string, catalog *upstream.Catalog) ([]upstream.Source, error) {
	if checkout == "" || catalog == nil {
		return nil, errors.New("sources checkout and upstream catalog are required")
	}
	data, err := os.ReadFile(filepath.Join(checkout, filepath.FromSlash(layout.SourcesFile)))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read accepted sources: %w", err)
	}
	return DecodeSources(data, catalog)
}

// DecodeSources decodes the content of the sources file as ReadSources does.
func DecodeSources(data []byte, catalog *upstream.Catalog) ([]upstream.Source, error) {
	parsed, err := layout.ParseSources(data)
	if err != nil {
		return nil, err
	}
	return TypeSources(parsed.Sources, catalog)
}

// TypeSources decodes parsed sources into followable upstream sources.
func TypeSources(sources []layout.Source, catalog *upstream.Catalog) ([]upstream.Source, error) {
	out := make([]upstream.Source, 0, len(sources))
	for _, source := range sources {
		configJSON, err := layout.SourceConfigJSON(source)
		if err != nil {
			return nil, err
		}
		config, err := catalog.ParseConfig(source.Kind, configJSON)
		if err != nil {
			return nil, fmt.Errorf("source at %q: %w", source.Path, err)
		}
		interval, err := catalog.PollInterval(source.Kind, config)
		if err != nil {
			return nil, fmt.Errorf("source at %q: %w", source.Path, err)
		}
		out = append(out, upstream.Source{Path: source.Path, Kind: source.Kind, Config: config, PollInterval: interval})
	}
	return out, nil
}
