package program

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// A busy pod's window: one violation line, then far more than 64 KiB of noise. The newest-64 KiB cut of Logs drops
// the match; the filtered scan finds it.
func TestScanLogMatchesFindsAMatchTheNewest64KiBMisses(t *testing.T) {
	var b strings.Builder
	b.WriteString("ERROR new row violates check constraint \"c1\"\n")
	for i := 0; i < 3000; i++ {
		fmt.Fprintf(&b, "request %d ok %s\n", i, strings.Repeat("x", 100))
	}
	window := b.String()
	if strings.Contains(NewestLogBytes(window, MaxLogBytes), "violates check constraint") {
		t.Fatal("test window is not busy enough: Logs would see the match")
	}
	got, err := ScanLogMatches(strings.NewReader(window), regexp.MustCompile(`violates check constraint`), 5)
	if err != nil || len(got.Lines) != 1 || got.Scanned != 3001 || got.ScanTruncated {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestScanLogMatchesKeepsTheNewestMatchesAndSaysWhenTheScanWasCut(t *testing.T) {
	var b strings.Builder
	for i := 0; i < MaxLogScanLines; i++ {
		fmt.Fprintf(&b, "hit %d\n", i)
	}
	got, err := ScanLogMatches(strings.NewReader(b.String()), regexp.MustCompile(`hit`), 3)
	if err != nil || len(got.Lines) != 3 || got.Lines[2] != fmt.Sprintf("hit %d", MaxLogScanLines-1) || !got.ScanTruncated {
		t.Fatalf("got %d lines %q truncated=%v, %v", len(got.Lines), got.Lines, got.ScanTruncated, err)
	}
}

func TestValidateLogMatchRequest(t *testing.T) {
	for _, c := range []struct {
		since   time.Duration
		pattern string
		max     int
	}{{0, "a", 1}, {time.Minute, "(", 1}, {time.Minute, "a", 0}, {time.Minute, "a", MaxLogMatchLines + 1}} {
		if _, err := ValidateLogMatchRequest(c.since, c.pattern, c.max); err == nil {
			t.Fatalf("%+v accepted", c)
		}
	}
	if _, err := ValidateLogMatchRequest(time.Minute, "a", 1); err != nil {
		t.Fatal(err)
	}
}
