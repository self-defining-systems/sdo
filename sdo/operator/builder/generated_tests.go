package builder

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/builder/internal/buildio"
)

// These replay tests report evidence without blocking memory publication.
const (
	corpusTest        = "TestProgramCorpus"
	capturedPlansTest = "TestCapturedPlans"
)

// healthyBaselineTestName is the generated healthy-baseline replay. It runs as its
// own stage after every other generated test passed, so its violations never
// share output with, or hide, another gate's failure, and the detector IDs it
// names have already passed the registration and controller-start contracts.
const healthyBaselineTestName = "TestLearnedDetectorsStayQuietOnHealthyBaseline"

// separateStages are the generated tests the main generated stage skips.
const separateStages = "^(" + corpusTest + "|" + capturedPlansTest + "|" + healthyBaselineTestName + ")$"

// IsHealthyBaselineStage reports whether a rejected stage is the healthy-baseline
// replay alone, so its output holds only that test's violations.
func IsHealthyBaselineStage(stage *StageError) bool {
	return stage != nil && strings.HasSuffix(stage.Stage, healthyBaselineStageSuffix)
}

const healthyBaselineStageSuffix = "-run ^" + healthyBaselineTestName + "$ ./generated"

func (workspace *Workspace) runHealthyBaseline(ctx context.Context) error {
	if !workspace.healthyBaseline {
		return nil
	}
	return workspace.runGo(ctx, "test", "-timeout=30s", "-run", "^"+healthyBaselineTestName+"$", "./generated")
}

// runGeneratedTests links the generated test binary once and classifies its
// result per test. A failing replay becomes an advisory note; any other
// failing test, or a result that cannot be attributed to individual tests
// (build error, panic, timeout), rejects through the plain acceptance stage so
// its diagnostics keep their usual shape.
func (workspace *Workspace) runGeneratedTests(ctx context.Context) error {
	workspace.generatedClassified = true
	workspace.corpusAdvisory = ""
	args := []string{"test", "-json", "-timeout=30s", "-skip", "^" + healthyBaselineTestName + "$"}
	if _, err := os.Stat(filepath.Join(workspace.Directory, "generated", "captured_plans_test.go")); err == nil {
		// The check consumes a workspace-local report, which a cached test does
		// not rewrite.
		args = append(args, "-count=1")
	}
	err := workspace.runGo(ctx, append(args, "./generated")...)
	var stage *buildio.StageError
	if err == nil || !errors.As(err, &stage) {
		return err
	}
	text, verdict := classifyGeneratedRun(stage.Output)
	switch verdict {
	case generatedCorpusOnly:
		workspace.corpusAdvisory = text
		return nil
	case generatedRejected:
		return &buildio.StageError{Stage: stage.Stage, Output: text, Err: stage.Err}
	}
	// Not attributable to individual tests: rerun without the corpus so the
	// rejection (or pass) is what the unclassified acceptance stage reports.
	if err := workspace.runGo(ctx, "test", "-skip", separateStages, "./generated"); err != nil {
		return err
	}
	workspace.corpusAdvisory = text
	return nil
}

type generatedVerdict int

const (
	generatedUnclassified generatedVerdict = iota
	generatedCorpusOnly
	generatedRejected
)

type testEvent struct {
	Action string
	Test   string
	Output string
}

// classifyGeneratedRun reconstructs plain go test output from a -json stream
// and decides whether only the program corpus failed.
func classifyGeneratedRun(output string) (string, generatedVerdict) {
	var text strings.Builder
	started := map[string]bool{}
	failed := map[string]bool{}
	scanner := bufio.NewScanner(strings.NewReader(output))
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		var event testEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return strings.TrimSpace(output), generatedUnclassified
		}
		switch event.Action {
		case "output":
			text.WriteString(event.Output)
		case "run":
			started[event.Test] = true
		case "pass", "skip":
			delete(started, event.Test)
		case "fail":
			delete(started, event.Test)
			if event.Test != "" {
				failed[event.Test] = true
			}
		}
	}
	plain := strings.TrimSpace(text.String())
	if len(failed) == 0 || len(started) > 0 || strings.Contains(plain, "panic:") {
		return plain, generatedUnclassified
	}
	for name := range failed {
		if name != capturedPlansTest && name != corpusTest && !strings.HasPrefix(name, corpusTest+"/") {
			return plain, generatedRejected
		}
	}
	return plain, generatedCorpusOnly
}

// informationalPackages lists the patterns the informational pass runs after
// the generated package was already classified: every top-level source
// directory except generated.
func (workspace *Workspace) informationalPackages() []string {
	if !workspace.generatedClassified {
		return []string{"./..."}
	}
	patterns := []string{}
	entries, err := os.ReadDir(workspace.Directory)
	if err != nil {
		return []string{"./..."}
	}
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || name == "generated" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" {
			continue
		}
		patterns = append(patterns, "./"+name+"/...")
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".go") {
			patterns = append(patterns, ".")
			break
		}
	}
	return patterns
}
