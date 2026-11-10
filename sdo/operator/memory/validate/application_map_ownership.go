package validate

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
)

// architectureRegionOwnership splits arch.md between its two owners: only the
// detection designer (wire actor health_judge) changes the application-map
// region, and it changes nothing outside it; Go upkeep rewrites the inventory
// outside the region and must carry the region verbatim, except that it seeds
// a region where the base has none.
func (s *run) architectureRegionOwnership(ctx context.Context) error {
	base, err := s.baseArchitecture(ctx)
	if err != nil {
		return err
	}
	candidate := ""
	if data, err := os.ReadFile(filepath.Join(s.req.Root, filepath.FromSlash(layout.ArchitectureFile))); err == nil {
		candidate = string(data)
	} else if !os.IsNotExist(err) {
		return err
	}
	switch s.req.Actor {
	case HealthJudge:
		if _, had, _ := appmap.Find(base); had {
			if _, has, _ := appmap.Find(candidate); !has {
				s.add(Ownership, Diagnostic{Rule: RuleMapOutsideRegion, Path: layout.ArchitectureFile, Message: "the detection designer removed the application-map markers; the region stays, and only its content changes", Hint: "restore " + appmap.BeginMarker + " and " + appmap.EndMarker + " around the map"})
				return nil
			}
		}
		if appmap.Outside(candidate) != appmap.Outside(base) {
			s.add(Ownership, Diagnostic{Rule: RuleMapOutsideRegion, Path: layout.ArchitectureFile, Message: "the detection designer changed text outside the application-map markers; the manifest inventory and everything else in arch.md is Go-owned", Hint: "restore everything outside " + appmap.BeginMarker + " ... " + appmap.EndMarker + " byte-for-byte and edit only the map between them"})
		}
	case Upkeep:
		// Go seeds a map from the rendered manifests where there is none;
		// once a region exists it is the designer's and Go carries it.
		if _, had, _ := appmap.Find(base); !had {
			return nil
		}
		if regionBlock(candidate) != regionBlock(base) {
			s.add(Ownership, Diagnostic{Rule: RuleMapRegionOwner, Path: layout.ArchitectureFile, Message: "Go upkeep changed the accepted application-map region, which only the detection designer may edit", Hint: "copy the accepted region between the markers verbatim when rewriting the inventory"})
		}
	default:
		if regionBlock(candidate) != regionBlock(base) {
			s.add(Ownership, Diagnostic{Rule: RuleMapRegionOwner, Path: layout.ArchitectureFile, Message: "actor " + string(s.req.Actor) + " changed the application-map region, which only the detection designer may edit", Hint: "copy the accepted region between the markers verbatim when rewriting the inventory"})
		}
	}
	return nil
}

// regionBlock is the region text of a document, empty without one.
func regionBlock(doc string) string {
	region, found, err := appmap.Find(doc)
	if !found || err != nil {
		// A malformed region is compared by the whole document tail so that
		// breaking or removing markers is still a change.
		if found {
			return doc
		}
		return ""
	}
	return region.Block
}

// baseArchitecture is arch.md at the base revision, empty when absent.
func (s *run) baseArchitecture(ctx context.Context) (string, error) {
	listing, err := git(ctx, s.req.Root, "ls-tree", "--name-only", s.req.Base, "--", layout.ArchitectureFile)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(listing)) == "" {
		return "", nil
	}
	out, err := git(ctx, s.req.Root, "show", s.req.Base+":"+layout.ArchitectureFile)
	return string(out), err
}
