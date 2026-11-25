package builder

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// BuildJourneyCheck builds the program that judges the memory's probes against
// the live application (journeyhost.Run), from the staged diagnostics.
func (workspace *Workspace) BuildJourneyCheck(ctx context.Context) (string, error) {
	dir := filepath.Join(workspace.Directory, "cmd", "journey-check")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	main := fmt.Sprintf(`package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"sdo.dev/operator/runtime/journeyhost"
	%q
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := journeyhost.Run(ctx, generated.TrafficCatalog(), generated.TrafficWorkloads, generated.ApplicationMapYAML, os.Stdin, os.Stdout, os.Stderr); err != nil {
		log.Fatal(err)
	}
}
`, workspace.ModulePath+"/generated")
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(main), 0o644); err != nil {
		return "", err
	}
	path := filepath.Join(workspace.Directory, "journey-check")
	for _, args := range [][]string{{"mod", "tidy"}, lean("build", "-trimpath", "-buildvcs=false", "-o", path, "./cmd/journey-check")} {
		if err := workspace.runGo(ctx, args...); err != nil {
			return "", err
		}
	}
	return path, nil
}
