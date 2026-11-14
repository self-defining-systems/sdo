package runtime

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"sdo.dev/operator/apihistory"
	"sdo.dev/operator/sdk"
)

// noteObservation is the alert or finding set one verify or detect call saw.
type noteObservation struct {
	at  time.Time
	key string
}

func noteKey(findings []sdk.Finding) string {
	keys := make([]string, 0, len(findings))
	for _, finding := range findings {
		fingerprint := finding.Fingerprint
		if fingerprint == "" {
			fingerprint = FindingFingerprint(finding)
		}
		keys = append(keys, fingerprint)
	}
	sort.Strings(keys)
	return strings.Join(keys, "\x00")
}

func noteTime(at time.Time) string { return at.UTC().Format("15:04:05Z") }

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// writeTimes are the times of the responder's writes as Go knows them: the plan applications it
// recorded and, while the write recording exists, the recorded writes (the same source verify --wait
// uses). known is false when neither signal exists (no recording and no application), so a caller
// omits the write clause instead of claiming that no write happened.
func (l *ResponderLoop) writeTimes() (times []time.Time, known bool) {
	for _, application := range l.applicationsLog {
		known = true
		at := application.CompletedAt
		if at.IsZero() {
			at = application.StartedAt
		}
		if !at.IsZero() {
			times = append(times, at)
		}
	}
	if l.ExternalReceipts != nil {
		if receipts, _, _, err := l.ExternalReceipts(); err == nil {
			known = true
			for _, receipt := range receipts {
				at := receipt.CompletedAt
				if at.IsZero() {
					at = receipt.StartedAt
				}
				times = append(times, at)
			}
		}
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	return times, known
}

// incidentWrites are the incident's own writes as `sdo changes` lists them:
// the responder's recorded writes and its `sdo mitigate` applications (actor
// agent) and SDO's automatic-stage and verification applications (actor sdo).
func (l *ResponderLoop) incidentWrites() []IncidentChange {
	var rows []IncidentChange
	object := func(receipt RepairActionReceipt) string {
		if len(receipt.Resources) > 0 {
			return receipt.Resources[0].Kind + "/" + receipt.Resources[0].Name
		}
		return receipt.Target
	}
	at := func(receipt RepairActionReceipt) time.Time {
		if receipt.CompletedAt.IsZero() {
			return receipt.StartedAt
		}
		return receipt.CompletedAt
	}
	if l.ExternalReceipts != nil {
		if receipts, _, _, err := l.ExternalReceipts(); err == nil {
			for _, receipt := range receipts {
				if apihistory.HelperKind(receipt.Kind) || receipt.Kind == apihistory.GapKind {
					continue
				}
				rows = append(rows, IncidentChange{At: at(receipt), Source: "write", ReaderOrObject: object(receipt), Key: receipt.Kind,
					After: receipt.Summary, Writer: sdk.ResponderFieldManager, Actor: ActorAgent})
			}
		}
	}
	for _, application := range l.applicationsLog {
		// The agent applies through `sdo mitigate` (menu attempts); SDO's
		// automatic stage applies plans and verification records equivalent repairs.
		actor, writer := ActorAgent, "sdo mitigate"
		if strings.HasPrefix(application.ID, "application-") || strings.HasPrefix(application.ID, "equivalent-repair-") {
			actor, writer = ActorSDO, "sdo playbook stage"
		}
		for _, receipt := range ProgramRepairReceipts([]RecordedPlanApplication{application}, nil) {
			rows = append(rows, IncidentChange{At: at(receipt), Source: "write", ReaderOrObject: object(receipt), Key: "mitigation",
				After: "playbook " + strings.Join(application.ProgramIDs, ","), Writer: writer, Actor: actor})
		}
	}
	return rows
}

// startNotes fixes the session's start and seeds the verify history with the alerts the incident opened with.
// The caller holds l.mu.
func (l *ResponderLoop) startNotes() {
	if !l.noteStart.IsZero() {
		return
	}
	l.noteStart = l.clock()
	l.noteLog = map[string][]noteObservation{"verify": {{at: l.noteStart, key: noteKey(l.Request.Findings)}}}
}

// verifyNote states, as facts only, what an unchanged verify result contradicts. The caller holds l.mu.
func (l *ResponderLoop) verifyNote(findings []sdk.Finding) string {
	l.startNotes()
	now := l.clock()
	key := noteKey(findings)
	history := l.noteLog["verify"]
	defer func() { l.noteLog["verify"] = append(l.noteLog["verify"], noteObservation{at: now, key: key}) }()
	// The trailing run of observations that saw this same set.
	run := len(history)
	for run > 0 && history[run-1].key == key {
		run--
	}
	stretch := history[run:]
	writes, known := l.writeTimes()
	switch {
	case !known:
		if len(history) > 0 && history[len(history)-1].key == key && len(history) > 1 {
			return fmt.Sprintf("opening alerts unchanged since your last verify at %s", noteTime(history[len(history)-1].at))
		}
	case len(writes) == 0:
		if calls := len(history) - 1; calls >= 1 {
			return fmt.Sprintf("no write since %s; %s since", noteTime(l.noteStart), plural(calls, "verify call"))
		}
	default:
		last := writes[len(writes)-1]
		if len(stretch) == 0 || stretch[0].at.After(last) {
			return ""
		}
		count := 0
		for _, at := range writes {
			if !at.Before(stretch[0].at) {
				count++
			}
		}
		if count == 0 {
			return ""
		}
		return fmt.Sprintf("opening alerts unchanged since your last write at %s (%s since the alerts last changed)", noteTime(last), plural(count, "write"))
	}
	return ""
}

// detectNote reports that an unfiltered detect saw the findings its previous call saw. The caller holds l.mu.
func (l *ResponderLoop) detectNote(findings []sdk.Finding) string {
	l.startNotes()
	key := noteKey(findings)
	history := l.noteLog["detect"]
	l.noteLog["detect"] = append(history, noteObservation{at: l.clock(), key: key})
	if len(history) > 0 && history[len(history)-1].key == key {
		return fmt.Sprintf("findings unchanged since your last call at %s", noteTime(history[len(history)-1].at))
	}
	return ""
}

// appendReportNotes shows each newly arrived investigator report once, on a successful response.
func (l *ResponderLoop) appendReportNotes(response *ToolResponse) {
	if response.Error != "" {
		return
	}
	for _, note := range l.Investigations.ReportNotes() {
		appendNote(response, note)
	}
}

func appendNote(response *ToolResponse, note string) {
	if note == "" {
		return
	}
	if response.Text != "" {
		response.Text = strings.TrimRight(response.Text, "\n") + "\n" + note + "\n"
	}
	if response.Summary == "" {
		response.Summary = note
	} else {
		response.Summary += "; " + note
	}
}

// liveRequest is the loop's request with the alert set the controller holds
// now: user tests that joined the incident after the request was taken.
func (l *ResponderLoop) liveRequest() IncidentRequest {
	request := l.Request
	if l.CurrentEvidence == nil {
		return request
	}
	current := l.CurrentEvidence()
	if current == nil || current.IncidentID != request.IncidentID {
		return request
	}
	request.JoinedKeys = current.JoinedKeys
	request.Findings = append(slices.Clone(request.Findings), current.Findings...)
	var seen []string
	request.Findings = slices.DeleteFunc(request.Findings, func(finding sdk.Finding) bool {
		key := findingKey(finding)
		if slices.Contains(seen, key) {
			return true
		}
		seen = append(seen, key)
		return false
	})
	return request
}
