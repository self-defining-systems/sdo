package builder

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sdo.dev/operator/internal/testutil"
)

func jsonRun(events ...string) string { return strings.Join(events, "\n") }

const (
	runCorpus   = `{"Action":"run","Test":"TestProgramCorpus"}`
	outCorpus   = `{"Action":"output","Test":"TestProgramCorpus","Output":"replay mismatch\n"}`
	failCorpus  = `{"Action":"fail","Test":"TestProgramCorpus"}`
	runSafety   = `{"Action":"run","Test":"TestGeneratedSafety"}`
	failSafety  = `{"Action":"fail","Test":"TestGeneratedSafety"}`
	outSafety   = `{"Action":"output","Test":"TestGeneratedSafety","Output":"safety broken\n"}`
	failPackage = `{"Action":"fail"}`
)

// The generated package's tests link and run once. Only a failing program
// corpus is informational; everything else is a rejection or is retried through
// the plain acceptance stage so its diagnostics keep their shape.
func TestGeneratedTestsAreClassifiedPerTest(t *testing.T) {
	for _, test := range []struct {
		name    string
		output  string
		verdict generatedVerdict
	}{
		{"corpus only", jsonRun(runCorpus, outCorpus, failCorpus, failPackage), generatedCorpusOnly},
		{"corpus subtest only", jsonRun(`{"Action":"run","Test":"TestProgramCorpus/a"}`, `{"Action":"fail","Test":"TestProgramCorpus/a"}`, failCorpus, failPackage), generatedCorpusOnly},
		{"safety and corpus", jsonRun(runSafety, outSafety, failSafety, runCorpus, failCorpus, failPackage), generatedRejected},
		{"safety only", jsonRun(runSafety, outSafety, failSafety, failPackage), generatedRejected},
		{"similarly named test", jsonRun(`{"Action":"run","Test":"TestProgramCorpusExtra"}`, `{"Action":"fail","Test":"TestProgramCorpusExtra"}`, failPackage), generatedRejected},
		{"panic in corpus", jsonRun(runCorpus, `{"Action":"output","Test":"TestProgramCorpus","Output":"panic: boom\n"}`, failCorpus, failPackage), generatedUnclassified},
		{"unfinished test", jsonRun(runCorpus, runSafety, failCorpus, failPackage), generatedUnclassified},
		{"build failure", jsonRun(`{"Action":"output","Output":"generated/x.go:1: undefined: y\n"}`, failPackage), generatedUnclassified},
		{"not json", "generated/x.go:1:1: syntax error", generatedUnclassified},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, got := classifyGeneratedRun(test.output); got != test.verdict {
				t.Fatalf("verdict %d, want %d", got, test.verdict)
			}
		})
	}
}

func TestCheckLinksTheGeneratedTestsOnce(t *testing.T) {
	generatedStages := func(log string) (count int, skipping bool, informationalIncludesGenerated bool) {
		for _, stage := range testutil.GoStages(t, log) {
			switch {
			case strings.HasPrefix(stage, "test") && strings.Contains(stage, "-timeout=30s ./") && !strings.Contains(stage, "-json"):
				informationalIncludesGenerated = informationalIncludesGenerated || strings.Contains(stage, "./...") || strings.Contains(stage, "./generated")
			case strings.HasPrefix(stage, "test") && strings.Contains(stage, "./generated"):
				count++
				skipping = skipping || strings.Contains(stage, "-skip")
			}
		}
		return
	}
	t.Run("pass", func(t *testing.T) {
		log := testutil.FakeGo(t)
		w := acceptanceFixture(t)
		if err := w.Check(context.Background()); err != nil {
			t.Fatal(err)
		}
		count, skipping, again := generatedStages(log)
		if count != 1 || skipping || again {
			t.Fatalf("generated test stages=%d skip=%v informational pass repeats generated=%v; stages: %q", count, skipping, again, testutil.GoStages(t, log))
		}
	})
	t.Run("corpus failure is advisory", func(t *testing.T) {
		log := testutil.FakeGo(t, testutil.GoRule{Verb: "test", ArgContains: "-json", Output: jsonRun(runCorpus, outCorpus, failCorpus, failPackage)})
		w := acceptanceFixture(t)
		if err := w.Check(context.Background()); err != nil {
			t.Fatalf("corpus failure rejected: %v", err)
		}
		if count, _, _ := generatedStages(log); count != 1 {
			t.Fatalf("generated test stages=%d: %q", count, testutil.GoStages(t, log))
		}
		if len(w.advisoryTestErrors) != 1 || !strings.Contains(w.advisoryTestErrors[0], "replay mismatch") {
			t.Fatalf("advisory notes %q", w.advisoryTestErrors)
		}
	})
	t.Run("safety failure rejects with plain output and no rerun", func(t *testing.T) {
		log := testutil.FakeGo(t, testutil.GoRule{Verb: "test", ArgContains: "-json", Output: jsonRun(runSafety, outSafety, failSafety, failPackage)})
		w := acceptanceFixture(t)
		err := w.Check(context.Background())
		var stage *StageError
		if !asStage(err, &stage) || stage.Output != "safety broken" {
			t.Fatalf("want rejection carrying plain test output, got %v", err)
		}
		if count, _, _ := generatedStages(log); count != 1 {
			t.Fatalf("rejection reran the generated tests: %q", testutil.GoStages(t, log))
		}
	})
	t.Run("unattributable failure retries without the corpus", func(t *testing.T) {
		log := testutil.FakeGo(t, testutil.GoRule{Verb: "test", ArgContains: "-json", Output: jsonRun(runCorpus, `{"Action":"output","Test":"TestProgramCorpus","Output":"panic: boom\n"}`, failCorpus, failPackage)})
		w := acceptanceFixture(t)
		if err := w.Check(context.Background()); err != nil {
			t.Fatalf("corpus panic rejected: %v", err)
		}
		if count, skipping, _ := generatedStages(log); count != 2 || !skipping {
			t.Fatalf("expected json stage then a corpus-skipping rerun: %q", testutil.GoStages(t, log))
		}
	})
	t.Run("acceptance alone skips the corpus", func(t *testing.T) {
		log := testutil.FakeGo(t)
		w := acceptanceFixture(t)
		if err := w.CheckAcceptance(context.Background()); err != nil {
			t.Fatal(err)
		}
		if count, skipping, _ := generatedStages(log); count != 1 || !skipping {
			t.Fatalf("stages: %q", testutil.GoStages(t, log))
		}
	})
}

func asStage(err error, target **StageError) bool { return errors.As(err, target) }
