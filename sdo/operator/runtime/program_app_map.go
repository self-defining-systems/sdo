package runtime

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/runtime/internal/lifecycleio"
	"sdo.dev/operator/sdk/program"
)

// acceptedRead returns the git reader, repository and accepted commit a live
// view reads accepted memory from.
func (v LiveReadView) acceptedRead() (gitRead, string, string, error) {
	source, ok := v.Source.(GitPlanSource)
	if !ok || source.Repository == "" {
		return nil, "", "", errors.New("accepted repository unavailable")
	}
	commit := v.SourceCommit
	if v.SourceRevision != nil {
		commit = v.SourceRevision()
	}
	if commit == "" {
		return nil, "", "", errors.New("accepted source commit unavailable")
	}
	read := gitRead(lifecycleio.GitOutput)
	if source.Git != nil {
		read = gitRead(source.Git)
	}
	return read, source.Repository, commit, nil
}

// AppMap reads the application map of the accepted architecture.
func (v LiveReadView) AppMap() (program.AppMap, error) {
	read, repository, commit, err := v.acceptedRead()
	if err != nil {
		return program.AppMap{}, fmt.Errorf("application map unavailable: %w", err)
	}
	return acceptedProgramMap(v.context(), read, repository, commit)
}

// ArchitectureText reads the accepted architecture document, which holds the
// application map region and the possible-edge lines `sdo map` reads.
func (v LiveReadView) ArchitectureText() (string, error) {
	read, repository, commit, err := v.acceptedRead()
	if err != nil {
		return "", err
	}
	document, err := read(v.context(), repository, "show", commit+":"+layout.ArchitectureFile)
	if err != nil {
		return "", fmt.Errorf("accepted architecture unreadable: %w", err)
	}
	return string(document), nil
}

// DataChecks reads the accepted data-check files by check ID.
func (v LiveReadView) DataChecks() (map[string]string, error) {
	read, repository, commit, err := v.acceptedRead()
	if err != nil {
		return nil, fmt.Errorf("data checks unavailable: %w", err)
	}
	return acceptedDataChecks(v.context(), read, repository, commit)
}

func acceptedProgramMap(ctx context.Context, read gitRead, repository, commit string) (program.AppMap, error) {
	document, err := read(ctx, repository, "show", commit+":"+layout.ArchitectureFile)
	if err != nil {
		return program.AppMap{}, fmt.Errorf("accepted architecture unreadable: %w", err)
	}
	region, found, err := appmap.Find(string(document))
	if err != nil {
		return program.AppMap{}, fmt.Errorf("accepted application map unreadable: %w", err)
	}
	if !found {
		return program.AppMap{}, errors.New("accepted architecture has no application map")
	}
	parsed, err := appmap.Parse(region.YAML)
	if err != nil {
		return program.AppMap{}, fmt.Errorf("accepted application map unreadable: %w", err)
	}
	return programMap(parsed), nil
}

func programMap(m appmap.Map) program.AppMap { return m.Program() }

func acceptedDataChecks(ctx context.Context, read gitRead, repository, commit string) (map[string]string, error) {
	checks := map[string]string{}
	listing, err := read(ctx, repository, "ls-tree", "--name-only", commit, layout.DataChecksDir+"/")
	if err != nil {
		return nil, fmt.Errorf("accepted data checks unreadable: %w", err)
	}
	for _, name := range strings.Split(strings.TrimSpace(string(listing)), "\n") {
		if name == "" {
			continue
		}
		kind, id, ok := layout.Classify(name)
		if !ok || kind != layout.KindDataCheck {
			continue
		}
		text, err := read(ctx, repository, "show", commit+":"+name)
		if err != nil {
			return nil, fmt.Errorf("accepted data check %s unreadable: %w", path.Base(name), err)
		}
		checks[id] = string(text)
	}
	return checks, nil
}

// AppMap reads the accepted map once per read cycle.
func (v *cachedReadView) AppMap() (program.AppMap, error) {
	var m program.AppMap
	err := v.read("AppMap", []any{}, &m, func() (any, error) { return v.ReadView.AppMap() })
	return m, err
}

// DataChecks reads the accepted data checks once per read cycle.
func (v *cachedReadView) DataChecks() (map[string]string, error) {
	var checks map[string]string
	err := v.read("DataChecks", []any{}, &checks, func() (any, error) { return v.ReadView.DataChecks() })
	return checks, err
}
