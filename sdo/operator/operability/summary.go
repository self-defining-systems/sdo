package operability

import (
	"encoding/json"
	"fmt"
	"time"
)

// MaxSummaryReportBytes caps the report JSON a summary carries. A larger
// report is carried without check details, then without checks that passed.
const MaxSummaryReportBytes = 200 * 1024

// ItemSummary is the one-line state of an item for people and harnesses that
// read the exposed summary instead of the report.
type ItemSummary struct {
	ID ItemID `json:"id"`
	// Status is the worst check status of the item: fail, pending, pass, note,
	// skipped, or missing when the item has no checks.
	Status           string `json:"status"`
	FirstFailedCheck string `json:"first_failed_check,omitempty"`
	Cause            Cause  `json:"cause,omitempty"`
	Owner            Owner  `json:"owner,omitempty"`
}

// Summary is the exposure envelope of a report. ObservedAt is the only
// timestamp and lives here, not in the digested report, so a re-run of the same
// state has the same Digest.
type Summary struct {
	AcceptedCommit string        `json:"accepted_commit"`
	Stage          Stage         `json:"stage"`
	Digest         string        `json:"digest"`
	ObservedAt     string        `json:"observed_at"`
	Items          []ItemSummary `json:"items"`
	// ReportTruncated says the report JSON carried alongside lost details or
	// passing checks to fit MaxSummaryReportBytes; the digest is of the full
	// report.
	ReportTruncated bool `json:"report_truncated,omitempty"`
}

// Summarize derives the envelope of a valid report. Owners are those of fresh
// onboarding (Route).
func Summarize(r Report, observedAt time.Time) (Summary, error) {
	digest, err := r.Digest()
	if err != nil {
		return Summary{}, err
	}
	out := Summary{AcceptedCommit: r.Subject.AcceptedCommit, Stage: r.Stage, Digest: digest, ObservedAt: observedAt.UTC().Format(time.RFC3339)}
	for _, item := range r.Items {
		out.Items = append(out.Items, summarizeItem(item))
	}
	return out, nil
}

func summarizeItem(item Item) ItemSummary {
	s := ItemSummary{ID: item.ID, Status: "missing"}
	rank := map[Status]int{StatusSkipped: 1, StatusNote: 2, StatusPass: 3, StatusPending: 4, StatusFail: 5}
	worst := 0
	for _, c := range item.Checks {
		if rank[c.Status] > worst {
			worst, s.Status = rank[c.Status], string(c.Status)
		}
		if (c.Status == StatusFail || c.Status == StatusPending) && s.FirstFailedCheck == "" {
			s.FirstFailedCheck, s.Cause, s.Owner = c.ID, c.Cause, Route(c.Cause)
		}
	}
	return s
}

// SummaryData is the key-value form of a summary and its report: "summary.json"
// is the envelope, "report.json" the report (truncated to MaxSummaryReportBytes).
func SummaryData(r Report, observedAt time.Time) (map[string]string, error) {
	summary, err := Summarize(r, observedAt)
	if err != nil {
		return nil, err
	}
	reportJSON, err := r.Encode()
	if err != nil {
		return nil, err
	}
	if len(reportJSON) > MaxSummaryReportBytes {
		summary.ReportTruncated = true
		if reportJSON, err = encodeTruncated(r); err != nil {
			return nil, err
		}
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		return nil, err
	}
	return map[string]string{"summary.json": string(encoded), "report.json": string(reportJSON)}, nil
}

// encodeTruncated drops check details, then passing and skipped checks, until
// the report fits. Failing and pending checks always stay.
func encodeTruncated(r Report) ([]byte, error) {
	cut := r.clone()
	for i := range cut.Items {
		for j := range cut.Items[i].Checks {
			cut.Items[i].Checks[j].Detail, cut.Items[i].Checks[j].Paths = "", nil
		}
	}
	data, err := json.Marshal(cut)
	if err != nil {
		return nil, err
	}
	if len(data) <= MaxSummaryReportBytes {
		return data, nil
	}
	for i := range cut.Items {
		kept := []Check{}
		for _, c := range cut.Items[i].Checks {
			if c.Status == StatusFail || c.Status == StatusPending {
				kept = append(kept, c)
			}
		}
		cut.Items[i].Checks = kept
	}
	if data, err = json.Marshal(cut); err != nil {
		return nil, err
	}
	if len(data) > MaxSummaryReportBytes {
		return nil, fmt.Errorf("operability report summary exceeds %d bytes", MaxSummaryReportBytes)
	}
	return data, nil
}
