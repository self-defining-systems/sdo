package runtime

import (
	"testing"
	"time"
)

func TestCompactDetectorHistoryKeepsPersistencePairAndCollapsesFurtherRepetition(t *testing.T) {
	first := time.Date(2026, 9, 13, 1, 0, 0, 0, time.UTC)
	latest := first.Add(time.Second)
	history := []DetectorEvaluation{
		{DetectorID: "health", Status: DetectorEvaluationFiring, Fingerprints: []string{"missing"}, EvaluatedAt: first},
		{DetectorID: "health", Status: DetectorEvaluationFiring, Fingerprints: []string{"missing"}, EvaluatedAt: latest},
		{DetectorID: "health", Status: DetectorEvaluationFiring, Fingerprints: []string{"missing"}, EvaluatedAt: latest.Add(time.Second)},
		{DetectorID: "health", Status: DetectorEvaluationClear, Fingerprints: []string{}, EvaluatedAt: latest.Add(2 * time.Second)},
	}

	got := compactDetectorHistory(history)
	if len(got) != 3 || !got[1].EvaluatedAt.Equal(latest.Add(time.Second)) || got[2].Status != DetectorEvaluationClear {
		t.Fatalf("unexpected compacted history: %#v", got)
	}
}

func TestCompactDetectorHistoryBoundsPromptHistory(t *testing.T) {
	history := make([]DetectorEvaluation, 20)
	for index := range history {
		history[index] = DetectorEvaluation{DetectorID: "health", Status: DetectorEvaluationStatus("state-" + string(rune('a'+index)))}
	}
	if got := compactDetectorHistory(history); len(got) != 12 {
		t.Fatalf("expected 12 history transitions, got %d", len(got))
	}
}
