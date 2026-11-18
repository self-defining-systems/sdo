package program

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sdo.dev/operator/sdk"
)

// MaxLogBytes is the most log text one program read returns.
const MaxLogBytes = 64 * 1024

// DefaultLogTailLines is how many of the newest lines a read returns when the
// program names no tail.
const DefaultLogTailLines = 1000

// BoundedLogOptions copies upstream options into a finite read of the newest
// lines. A byte limit sent to Kubernetes would return the OLDEST bytes of the
// window, so the request carries a line tail instead, and the byte bound is
// returned for the reader to apply with NewestLogBytes. The caller's
// LimitBytes, when set, lowers that bound.
func BoundedLogOptions(options corev1.PodLogOptions) (corev1.PodLogOptions, int64, error) {
	o := options.DeepCopy()
	if o.Follow {
		return corev1.PodLogOptions{}, 0, fmt.Errorf("bounded log reads cannot follow")
	}
	if o.SinceSeconds != nil && (*o.SinceSeconds <= 0 || o.SinceTime != nil) {
		return corev1.PodLogOptions{}, 0, fmt.Errorf("logs require positive since seconds, exclusive with since time")
	}
	if o.TailLines != nil && *o.TailLines < 0 {
		return corev1.PodLogOptions{}, 0, fmt.Errorf("log tail lines must be nonnegative")
	}
	limit := int64(MaxLogBytes)
	if o.LimitBytes != nil {
		if *o.LimitBytes <= 0 || *o.LimitBytes > MaxLogBytes {
			return corev1.PodLogOptions{}, 0, fmt.Errorf("log byte limit must be between 1 and %d", MaxLogBytes)
		}
		limit = *o.LimitBytes
	}
	o.LimitBytes = nil
	if o.TailLines == nil {
		tail := int64(DefaultLogTailLines)
		o.TailLines = &tail
	}
	return *o, limit, nil
}

// NewestLogBytes keeps the newest whole lines of data that fit in limit bytes.
func NewestLogBytes(data string, limit int64) string {
	if int64(len(data)) <= limit {
		return data
	}
	data = data[int64(len(data))-limit:]
	if i := strings.IndexByte(data, '\n'); i >= 0 {
		return data[i+1:]
	}
	return ""
}

func (snapshotReadView) LogsWithOptions(sdk.ObjectRef, corev1.PodLogOptions) (string, error) {
	return "", fmt.Errorf("logs unavailable in detector context")
}
func (v *checkedReadView) LogsWithOptions(ref sdk.ObjectRef, options corev1.PodLogOptions) (string, error) {
	r, err := v.ReadView.LogsWithOptions(ref, options)
	v.check(err)
	v.record("logs", podTarget(ref, options.Container), fmt.Sprintf("%d bytes", len(r)), err)
	return r, err
}

// Bounds of a LogMatches read. The scan covers the whole window up to MaxLogScanLines newest lines and
// MaxLogScanBytes, then only the matching lines are returned, so a busy pod's noise never pushes the match out the way
// the newest-64 KiB cut of Logs does.
const (
	MaxLogScanLines   = 100000
	MaxLogScanBytes   = 64 << 20
	MaxLogMatchLines  = 200
	maxLogLineBytes   = 1 << 20
	maxLogMatchLength = 4096
)

// LogMatchResult is what LogMatches found. Lines are the newest matches, oldest first, each cut to 4 KiB. Scanned
// counts the lines examined. ScanTruncated says the window held more than the scan covers (the oldest lines were not
// examined), so an absent match is then not evidence of absence.
type LogMatchResult struct {
	Lines         []string `json:"lines"`
	Scanned       int      `json:"scanned"`
	ScanTruncated bool     `json:"scan_truncated"`
}

// ValidateLogMatchRequest checks the arguments of LogMatches.
func ValidateLogMatchRequest(since time.Duration, pattern string, max int) (*regexp.Regexp, error) {
	if since <= 0 {
		return nil, fmt.Errorf("log matches need a positive window")
	}
	if max < 1 || max > MaxLogMatchLines {
		return nil, fmt.Errorf("log matches return between 1 and %d lines", MaxLogMatchLines)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("log match pattern: %w", err)
	}
	return re, nil
}

// LogMatchOptions is the Kubernetes read a LogMatches scan needs: the window, with a line tail that bounds the scan.
func LogMatchOptions(container string, since time.Duration) corev1.PodLogOptions {
	seconds := int64(since / time.Second)
	if since%time.Second != 0 {
		seconds++
	}
	tail := int64(MaxLogScanLines)
	return corev1.PodLogOptions{Container: container, SinceSeconds: &seconds, TailLines: &tail}
}

// ScanLogMatches reads a log stream and keeps the newest max lines that match re.
func ScanLogMatches(r io.Reader, re *regexp.Regexp, max int) (LogMatchResult, error) {
	result := LogMatchResult{Lines: []string{}}
	limited := &io.LimitedReader{R: r, N: MaxLogScanBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 0, 64*1024), maxLogLineBytes)
	for scanner.Scan() {
		result.Scanned++
		line := scanner.Text()
		if !re.MatchString(line) {
			continue
		}
		if len(line) > maxLogMatchLength {
			line = line[:maxLogMatchLength]
		}
		if len(result.Lines) == max {
			result.Lines = result.Lines[1:]
		}
		result.Lines = append(result.Lines, line)
	}
	if err := scanner.Err(); err != nil {
		if !errors.Is(err, bufio.ErrTooLong) {
			return result, err
		}
		result.ScanTruncated = true
	}
	if limited.N <= 0 || result.Scanned >= MaxLogScanLines {
		result.ScanTruncated = true
	}
	return result, nil
}

func (snapshotReadView) LogMatches(sdk.ObjectRef, string, time.Duration, string, int) (LogMatchResult, error) {
	return LogMatchResult{}, fmt.Errorf("logs unavailable in detector context")
}
func (v *checkedReadView) LogMatches(ref sdk.ObjectRef, container string, since time.Duration, pattern string, max int) (LogMatchResult, error) {
	r, err := v.ReadView.LogMatches(ref, container, since, pattern, max)
	v.check(err)
	outcome := fmt.Sprintf("%d lines matched of %d scanned", len(r.Lines), r.Scanned)
	if r.ScanTruncated {
		outcome += " (scan truncated)"
	}
	v.record("log_matches", podTarget(ref, container), outcome, err)
	return r, err
}
