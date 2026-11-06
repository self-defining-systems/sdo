package operatorcli

import (
	"errors"
	"fmt"

	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/operatorcli/catalog/upstreams"
)

// parseSourceFlags turns --source-kind and --source-config into the followed
// source published with the human goal. Both or neither. The config must decode
// into the kind's registered watcher config, so a typo fails at launch rather
// than at the first poll of the controller.
func parseSourceFlags(kind, config string) ([]layout.Source, error) {
	if kind == "" && config == "" {
		return nil, nil
	}
	if kind == "" || config == "" {
		return nil, errors.New("--source-kind and --source-config must be given together")
	}
	decoded, err := layout.ParseSourceConfigJSON(config)
	if err != nil {
		return nil, err
	}
	sources := []layout.Source{{Path: layout.SourcePathRoot, Kind: kind, Config: decoded}}
	if err := (layout.Sources{Sources: sources}).Validate(); err != nil {
		return nil, err
	}
	if _, err := memory.TypeSources(sources, upstreams.Catalog()); err != nil {
		return nil, fmt.Errorf("invalid followed source: %w", err)
	}
	return sources, nil
}
