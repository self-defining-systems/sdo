package validate

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"sdo.dev/operator/builder"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/memory/layout"
)

// assuranceSeeds is the directory of checked-in benchmark operational-memory
// seeds. Each <seed>/operational_memory is a complete .sdo tree.
var assuranceSeeds = filepath.Join("..", "..", "..", "..", "benchmarks", "sregym", "experiments", "assurance", "seeds")

// seedOverlays lists overlays that are only meaningful on top of one seed.
var seedOverlays = map[string][]string{
	"hotel_reservation_30e023d": {"links"},
}

// updateSeedsEnv regenerates the Go-owned generic health packages of every
// seed from the current installer (memory.PrepareHealth) instead of failing.
const updateSeedsEnv = "SDO_UPDATE_ASSURANCE_SEEDS"

type seedCase struct {
	name     string
	sources  []string // copied into .sdo/ in order; later trees overlay earlier ones
	writable string   // tree whose generic packages SDO_UPDATE_ASSURANCE_SEEDS rewrites
}

func seedCases(t *testing.T) []seedCase {
	t.Helper()
	entries, err := os.ReadDir(assuranceSeeds)
	if err != nil {
		t.Fatal(err)
	}
	var cases []seedCase
	for _, entry := range entries {
		memoryRoot := filepath.Join(assuranceSeeds, entry.Name(), "operational_memory")
		if _, err := os.Stat(memoryRoot); err != nil {
			continue
		}
		cases = append(cases, seedCase{name: entry.Name(), sources: []string{memoryRoot}, writable: memoryRoot})
		for _, overlay := range seedOverlays[entry.Name()] {
			overlayRoot := filepath.Join(assuranceSeeds, "overlays", overlay)
			cases = append(cases, seedCase{name: entry.Name() + "+" + overlay, sources: []string{memoryRoot, overlayRoot}, writable: overlayRoot})
		}
	}
	if len(cases) < 3 {
		t.Fatalf("expected the hotel, social network, and astronomy shop seeds, found %d cases", len(cases))
	}
	return cases
}

// TestAssuranceSeedsPassHealthJudgeCheck runs every checked-in seed through the
// complete acceptance pipeline a health judge's bootstrap must pass (static
// rules, Go-owned generic health preparation, publication gates, and the
// compile layer that builds and tests the diagnostics module and its traffic
// prober). Human and deployer documents form the base commit; the judge-owned
// diagnostics and playbooks are the proposed change. A seed must also already
// contain the installer's generic health packages byte for byte, because a
// seeded workspace is compiled into the controller without a judge round.
func TestAssuranceSeedsPassHealthJudgeCheck(t *testing.T) {
	operatorRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, seed := range seedCases(t) {
		t.Run(seed.name, func(t *testing.T) {
			if os.Getenv(updateSeedsEnv) != "" {
				regenerateGenericHealth(t, seed)
			}
			root := t.TempDir()
			sdo := filepath.Join(root, layout.Dir)
			for _, source := range seed.sources {
				copyTree(t, source, sdo)
			}
			judged := t.TempDir()
			for _, rel := range []string{layout.DiagnosticsDir, layout.PlaybooksDir} {
				moveTree(t, filepath.Join(root, rel), filepath.Join(judged, rel))
			}
			// The diagnostics module definition belongs to upkeep, not the judge.
			moveTree(t, filepath.Join(judged, layout.DiagnosticsGoModFile), filepath.Join(root, layout.DiagnosticsGoModFile))
			commit(t, root, "human goal, deployer architecture, and diagnostics module")
			copyTree(t, judged, root)
			before := snapshotTree(t, root)

			var stages []builder.StageTiming
			diagnostics, err := Check(context.Background(), Request{
				Root: root, Base: "HEAD", Actor: HealthJudge, Phase: InitialPhase, OperatorRoot: operatorRoot,
				StageTimings: &stages,
			}, CompileCandidate)
			for _, stage := range stages {
				t.Logf("seed compiler stage %s: %s", stage.Stage, stage.Duration)
			}
			if err != nil {
				t.Fatalf("health judge check: %v", err)
			}
			for _, d := range diagnostics {
				t.Errorf("%s %s:%d %s (%s)", d.Rule, d.Path, d.Line, d.Message, d.Hint)
			}
			if drift := treeDrift(before, snapshotTree(t, root)); len(drift) > 0 {
				t.Errorf("seed lacks the installer's generic health packages %v; rerun with %s=1", drift, updateSeedsEnv)
			}
		})
	}
}

// regenerateGenericHealth replaces every generic health package of the
// writable tree with the current installer output.
func regenerateGenericHealth(t *testing.T, seed seedCase) {
	t.Helper()
	scratch := t.TempDir()
	for _, source := range seed.sources {
		copyTree(t, source, scratch)
	}
	healthDir := filepath.Join(scratch, "diagnostics", "detectors", "health")
	entries, err := os.ReadDir(healthDir)
	if err != nil {
		t.Fatal(err)
	}
	generic := map[string]bool{}
	for _, entry := range entries {
		if isGenericHealthID(entry.Name()) {
			generic[entry.Name()] = true
			remove(t, healthDir, entry.Name())
		}
	}
	// PrepareHealth addresses the checkout root, which holds .sdo/.
	checkout := t.TempDir()
	copyTree(t, scratch, filepath.Join(checkout, layout.Dir))
	if err := memory.PrepareHealth(checkout); err != nil {
		t.Fatal(err)
	}
	prepared := filepath.Join(checkout, layout.Dir, "diagnostics", "detectors", "health")
	entries, err = os.ReadDir(prepared)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(seed.writable, "diagnostics", "detectors", "health")
	for id := range generic {
		remove(t, target, id)
	}
	ownedByBase := map[string]bool{}
	if seed.writable != seed.sources[0] {
		baseEntries, _ := os.ReadDir(filepath.Join(seed.sources[0], "diagnostics", "detectors", "health"))
		for _, entry := range baseEntries {
			ownedByBase[entry.Name()] = true
		}
	}
	for _, entry := range entries {
		if !isGenericHealthID(entry.Name()) || ownedByBase[entry.Name()] {
			continue
		}
		copyTree(t, filepath.Join(prepared, entry.Name()), filepath.Join(target, entry.Name()))
	}
}

// isGenericHealthID names the health packages the installer owns: everything
// except the judge-authored health-objective detector.
func isGenericHealthID(id string) bool { return id != "health-objective" }

func moveTree(t *testing.T, from, to string) {
	t.Helper()
	if _, err := os.Stat(from); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(from, to); err != nil {
		t.Fatal(err)
	}
}

func snapshotTree(t *testing.T, root string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	err := filepath.WalkDir(filepath.Join(root, layout.Dir), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		files[filepath.ToSlash(rel)] = data
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func treeDrift(before, after map[string][]byte) []string {
	var drift []string
	for path, data := range after {
		if prior, ok := before[path]; !ok || !bytes.Equal(prior, data) {
			drift = append(drift, path)
		}
	}
	for path := range before {
		if _, ok := after[path]; !ok {
			drift = append(drift, path)
		}
	}
	sort.Strings(drift)
	return drift
}
