package runtime

import (
	"bufio"
	"context"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// Container logs are read through one reader. The Kubernetes log API applies
// a byte limit to the START of the window, so a byte cap returns the oldest
// lines and loses the newest, which are the ones an incident is about. The
// reader asks for the newest lines (TailLines) instead, bounds each line's
// length while reading, and hands every line to the caller whole, so callers
// filter and parse before any truncation. The result says which time range was
// read and whether older lines of the window were left unread.
const (
	// newestLogLines is how many of a container's newest lines one read returns.
	newestLogLines = 20000
	// logLineBytes bounds one line while reading; the rest of a longer line is dropped.
	logLineBytes = 8 * 1024
)

// LogCoverage is what one container read covered.
type LogCoverage struct {
	// Lines is the number of non-empty lines read.
	Lines int
	// Oldest and Newest are the timestamps of the first and last line read.
	Oldest, Newest time.Time
	// Cut is true when the window held more lines than were read: the lines
	// before Oldest were not read.
	Cut bool
}

// readNewestLogs reads the newest tail lines of one container within options
// (SinceTime, Previous and so on) and calls visit with each raw line, oldest
// first. options.LimitBytes and options.TailLines are replaced.
func readNewestLogs(ctx context.Context, read PodLogReader, namespace, pod string, options corev1.PodLogOptions, tail int, visit func(raw string)) (LogCoverage, error) {
	var cover LogCoverage
	limit := int64(tail)
	options.TailLines, options.LimitBytes, options.Timestamps = &limit, nil, true
	stream, err := read(ctx, namespace, pod, &options)
	if err != nil {
		return cover, err
	}
	defer stream.Close()
	reader := bufio.NewReaderSize(stream, 64*1024)
	for {
		raw, err := readLogLine(reader)
		if raw != "" {
			cover.Lines++
			if at, _ := splitLogTimestamp(raw); !at.IsZero() {
				if cover.Oldest.IsZero() {
					cover.Oldest = at
				}
				cover.Newest = at
			}
			visit(raw)
		}
		if err != nil {
			cover.Cut = cover.Lines >= tail
			if err == io.EOF {
				return cover, nil
			}
			return cover, err
		}
	}
}

// readLogLine returns the next non-empty line, truncated to logLineBytes.
func readLogLine(r *bufio.Reader) (string, error) {
	for {
		var line []byte
		for {
			part, prefix, err := r.ReadLine()
			if len(line) < logLineBytes {
				line = append(line, part...)
			}
			if err != nil {
				return strings.TrimSpace(string(line)), err
			}
			if !prefix {
				break
			}
		}
		if text := strings.TrimSpace(string(line)); text != "" {
			return text, nil
		}
	}
}
