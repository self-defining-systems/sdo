package memory

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func ReadArchitecture(checkout string) (string, error) {
	if checkout == "" {
		return "", fmt.Errorf("architecture checkout is required")
	}
	data, err := os.ReadFile(filepath.Join(checkout, ".sdo", "arch.md"))
	if err != nil {
		return "", fmt.Errorf("read accepted architecture: %w", err)
	}
	parts := strings.SplitN(string(data), "---\n", 3)
	if len(parts) != 3 || parts[0] != "" || strings.TrimSpace(parts[2]) == "" {
		return "", fmt.Errorf("accepted architecture is invalid")
	}
	if strings.Contains(strings.ToLower(parts[2]), ".sdo/") {
		return "", fmt.Errorf("accepted architecture contains a physical operational-memory path")
	}
	return strings.TrimSpace(parts[2]) + "\n", nil
}

func WriteGoal(checkout, application, objective string) error {
	if checkout == "" || strings.TrimSpace(application) == "" || strings.TrimSpace(objective) == "" {
		return fmt.Errorf("checkout, application, and health objective are required")
	}
	body := fmt.Sprintf("---\nschema_version: 1\nowner: human\napplication: %s\n---\n# Health objective\n\n%s\n", strconv.Quote(application), strings.TrimSpace(objective))
	return writeBootstrapFile(checkout, "goal.md", body)
}

func WriteSchema(checkout string) error {
	if checkout == "" {
		return fmt.Errorf("checkout is required")
	}
	if err := writeBootstrapFile(checkout, "schema-version", "1\n"); err != nil {
		return err
	}
	module := filepath.Join(checkout, ".sdo", "diagnostics", "go.mod")
	if err := os.MkdirAll(filepath.Dir(module), 0o755); err != nil {
		return fmt.Errorf("create diagnostics module: %w", err)
	}
	if err := os.WriteFile(module, []byte("module app-diagnostics\n\ngo 1.24\n\nrequire sdo.dev/operator/sdk v0.0.0\n"), 0o644); err != nil {
		return fmt.Errorf("write diagnostics module: %w", err)
	}
	return nil
}

func WriteArchitecture(checkout, application, acceptedSourceCommit, fingerprint, summary string) error {
	for _, value := range []string{checkout, application, acceptedSourceCommit, fingerprint, summary} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("architecture requires checkout, application, accepted source, fingerprint, and summary")
		}
	}
	if strings.Contains(strings.ToLower(summary), ".sdo/") {
		return fmt.Errorf("architecture summary contains a physical operational-memory path")
	}
	content := fmt.Sprintf("---\nschema_version: 1\ngenerated_at_commit: %s\ngenerated_at: %s\napplication: %s\ntopology_fingerprint: %s\n---\n%s\n",
		acceptedSourceCommit, time.Now().UTC().Format(time.RFC3339Nano), strconv.Quote(application), fingerprint, strings.TrimSpace(summary))
	return writeBootstrapFile(checkout, "arch.md", content)
}

func writeBootstrapFile(checkout, name, content string) error {
	directory := filepath.Join(checkout, ".sdo")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create operational memory: %w", err)
	}
	if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}
