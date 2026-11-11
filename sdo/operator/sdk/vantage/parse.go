package vantage

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// FactStatus is the HTTP status of a dialing probe's last answered request.
const FactStatus = "status"

// Parse turns a dialing probe command's output into its dials and the
// reading's facts. It fails when the output is not the command's.
func Parse(kind Kind, stdout string, observedAt time.Time) (facts map[string]string, dials []Dial, err error) {
	switch kind {
	case TCPConnect, HTTPGet:
		dials, err = parseDials(kind, stdout, observedAt)
		facts = map[string]string{}
		for _, dial := range slices.Backward(dials) {
			if dial.Status != "" {
				facts[FactStatus] = dial.Status
				break
			}
		}
		return facts, dials, err
	}
	return nil, nil, fmt.Errorf("unknown vantage kind %q", kind)
}

func parseDials(kind Kind, stdout string, observedAt time.Time) ([]Dial, error) {
	var dials []Dial
	for _, line := range strings.Split(stdout, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "psi" && len(dials) > 0 {
			// The node's CPU stall right after the dial before it.
			stall, err := strconv.ParseFloat(fields[1], 64)
			if err != nil || stall < 0 {
				return nil, fmt.Errorf("malformed stall line %q", line)
			}
			dials[len(dials)-1].HostCPUStall = stall
			continue
		}
		if len(fields) < 6 || fields[0] != "dial" {
			continue
		}
		numbers := make([]int64, 5)
		for i := range numbers {
			n, err := strconv.ParseInt(fields[i+1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("malformed dial line %q", line)
			}
			numbers[i] = n
		}
		start := time.Unix(numbers[1], numbers[2]*1000)
		end := time.Unix(numbers[3], numbers[4]*1000)
		message := strings.TrimSpace(strings.Join(fields[6:], " "))
		dial := Dial{At: end.UTC(), Duration: max(0, end.Sub(start)), OK: numbers[0] == 0}
		if kind == HTTPGet {
			dial.Status = httpStatus(message)
			if code, err := strconv.Atoi(dial.Status); err == nil {
				dial.OK = code >= 100 && code <= 499
			}
		}
		if !dial.OK {
			dial.Class, dial.Error = dialFailure(kind, message, dial)
		}
		dials = append(dials, dial)
	}
	if len(dials) == 0 {
		return nil, fmt.Errorf("no dial was reported")
	}
	// The helper's clock dates the dials; the observation time bounds them,
	// so a skewed node clock cannot date a dial after its observation.
	if shift := dials[len(dials)-1].At.Sub(observedAt); shift > 0 {
		for i := range dials {
			dials[i].At = dials[i].At.Add(-shift)
		}
	}
	return dials, nil
}

func httpStatus(message string) string {
	index := strings.Index(message, "HTTP/")
	if index < 0 {
		return ""
	}
	fields := strings.Fields(message[index:])
	if len(fields) < 2 {
		return ""
	}
	if _, err := strconv.Atoi(fields[1]); err != nil {
		return ""
	}
	return fields[1]
}

// dialFailure classifies a failed dial (traffic.LinkFailureClass values).
func dialFailure(kind Kind, message string, dial Dial) (string, string) {
	lower := strings.ToLower(message)
	text := message
	if text == "" {
		text = "no answer"
	}
	switch {
	case dial.Status != "":
		return "not_serving", "HTTP " + dial.Status
	case strings.Contains(lower, "bad address") || strings.Contains(lower, "unknown host") || strings.Contains(lower, "resolve"):
		return "transport_error", text
	case strings.Contains(lower, "refused"):
		return "refused", text
	case strings.Contains(lower, "reset"):
		return "reset", text
	case dial.Duration >= DialTimeout*9/10 || strings.Contains(lower, "timed out"):
		return "timeout", "no connection within " + DialTimeout.String()
	case kind == TCPConnect:
		// BusyBox nc -z reports a refused connection by its exit code alone.
		return "refused", text
	}
	return "transport_error", text
}
