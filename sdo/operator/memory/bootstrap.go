package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
	"sdo.dev/operator/memory/appmap"
	"sdo.dev/operator/memory/layout"
	"sdo.dev/operator/safefs"
)

func ReadArchitecture(checkout string) (string, error) {
	if checkout == "" {
		return "", fmt.Errorf("architecture checkout is required")
	}
	data, err := os.ReadFile(filepath.Join(checkout, layout.ArchitectureFile))
	if err != nil {
		return "", fmt.Errorf("read accepted architecture: %w", err)
	}
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 || parts[0] != "" || strings.TrimSpace(parts[2]) == "" {
		return "", fmt.Errorf("accepted architecture is invalid")
	}
	if layout.ContainsPhysicalPath(strings.ToLower(parts[2])) {
		return "", fmt.Errorf("accepted architecture contains a physical operational-memory path")
	}
	return strings.TrimSpace(parts[2]) + "\n", nil
}

// WriteGoal writes the human goal, and the followed sources beside it when
// there are any. The broker records the initial upstream revision on the
// commit that publishes a sources file.
func WriteGoal(checkout, application, objective string, sources []layout.Source) error {
	if checkout == "" || strings.TrimSpace(application) == "" {
		return fmt.Errorf("checkout and application are required")
	}
	// An objective that is itself a goal.md (front matter with an optional
	// journeys list) keeps its journeys in the human goal commit.
	journeys := ""
	if strings.HasPrefix(objective, "---\n") {
		doc, err := layout.ParseDocument([]byte(objective))
		if err != nil {
			return fmt.Errorf("goal front matter: %w", err)
		}
		list, err := appmap.ParseGoalJourneys([]byte(objective))
		if err != nil {
			return err
		}
		if len(list) > 0 {
			encoded, err := yaml.Marshal(map[string]any{"journeys": list})
			if err != nil {
				return err
			}
			journeys = string(encoded)
		}
		objective = renderJourneys(doc.Body, list)
	}
	body := fmt.Sprintf("---\nschema_version: 1\nowner: human\napplication: %s\n%s---\n# Health objective\n\n%s\n", strconv.Quote(application), journeys, strings.TrimSpace(objective))
	if len(sources) > 0 {
		encoded, err := layout.MarshalSources(layout.Sources{Sources: sources})
		if err != nil {
			return fmt.Errorf("write %s: %w", layout.SourcesFile, err)
		}
		if err := writeBootstrapFile(checkout, layout.SourcesFile, string(encoded)); err != nil {
			return err
		}
	}
	return writeBootstrapFile(checkout, layout.GoalFile, body)
}

// WriteSchema initializes or upgrades operational memory and refreshes installed request-metrics policy.
func WriteSchema(checkout string) error {
	if checkout == "" {
		return fmt.Errorf("checkout is required")
	}
	migrated, err := layout.Migrate(checkout)
	if err != nil {
		return fmt.Errorf("migrate operational memory: %w", err)
	}
	if migrated {
		// Go-installed generic detectors Go no longer plans would stop the
		// migrated tree compiling; upkeep drops them with the schema stamp.
		if err := PruneStrandedGenericDetectors(checkout); err != nil {
			return fmt.Errorf("migrate operational memory: %w", err)
		}
	}
	if err := refreshInstalledRequestMetrics(checkout); err != nil {
		return fmt.Errorf("refresh request-metrics: %w", err)
	}
	// The module name belongs to whoever authored the module; rewriting it
	// would change the accepted head under a pending install.
	if _, err := os.Stat(filepath.Join(checkout, filepath.FromSlash(layout.DiagnosticsGoModFile))); err == nil {
		return nil
	}
	if err := safefs.WriteFile(checkout, layout.DiagnosticsGoModFile, []byte("module app-diagnostics\n\ngo 1.24\n\nrequire sdo.dev/operator/sdk v0.0.0\n"), 0o644); err != nil {
		return fmt.Errorf("write diagnostics module: %w", err)
	}
	return nil
}

func WriteArchitecture(checkout, application, acceptedSourceCommit, summary string) error {
	for _, value := range []string{checkout, application, acceptedSourceCommit, summary} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("architecture requires checkout, application, accepted source and inventory")
		}
	}
	if layout.ContainsPhysicalPath(strings.ToLower(summary)) {
		return fmt.Errorf("architecture summary contains a physical operational-memory path")
	}
	content := fmt.Sprintf("---\nschema_version: 1\ngenerated_at_commit: %s\napplication: %s\n---\n%s\n",
		acceptedSourceCommit, strconv.Quote(application), strings.TrimSpace(summary))
	// The inventory is Go's; the application map between its markers is the
	// detection designer's, so a rewrite carries it over verbatim.
	if previous, err := os.ReadFile(filepath.Join(checkout, filepath.FromSlash(layout.ArchitectureFile))); err == nil {
		content = appmap.Carry(string(previous), content)
	}
	return writeBootstrapFile(checkout, layout.ArchitectureFile, content)
}

func writeBootstrapFile(checkout, relative, content string) error {
	if err := safefs.WriteFile(checkout, relative, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", relative, err)
	}
	return nil
}
