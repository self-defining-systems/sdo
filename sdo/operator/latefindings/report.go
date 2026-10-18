// Package latefindings owns the logical, bounded incident telemetry projection.
// Workers receive only a read-only ConfigMap projection, never controller storage.
package latefindings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const ReportKey = "report.json"
const HistoryKey = "history.json"

type Snapshot struct {
	Digest      string   `json:"digest"`
	DetectorIDs []string `json:"detector_ids"`
	PlaybookIDs []string `json:"playbook_ids"`
}

type Finding struct {
	DetectorID        string         `json:"detector_id"`
	DetectorClass     string         `json:"detector_class,omitempty"`
	RuleID            string         `json:"rule_id,omitempty"`
	Fingerprint       string         `json:"fingerprint"`
	Severity          string         `json:"severity,omitempty"`
	ParameterBindings map[string]any `json:"parameter_bindings,omitempty"`
	PlaybookIDs       []string       `json:"playbook_ids"`
	ActivatedAt       string         `json:"activated_at"`
	StillActive       bool           `json:"still_active"`
}

type Report struct {
	SchemaVersion      string    `json:"schema_version"`
	IncidentID         string    `json:"incident_id"`
	TelemetryAvailable bool      `json:"telemetry_available"`
	LateFindings       []Finding `json:"late_findings"`
}

type Event struct {
	IncidentID, Relation, Event, DetectorID, DetectorClass, RuleID, Fingerprint, Severity, RecordedAt string
	ParameterBindings                                                                                 map[string]any
	PlaybookIDs                                                                                       []string
}

type Store struct {
	Client    kubernetes.Interface
	Namespace string
}

func ConfigMapName(incidentID string) string {
	sum := sha256.Sum256([]byte(incidentID))
	return "sdo-late-" + hex.EncodeToString(sum[:10])
}

func EmptyReport(incidentID string) Report {
	return Report{SchemaVersion: "sdo.late-findings/v1", IncidentID: incidentID, TelemetryAvailable: true, LateFindings: []Finding{}}
}

func (s Store) Ensure(ctx context.Context, incidentID string) error {
	if s.Client == nil || s.Namespace == "" || incidentID == "" {
		return errors.New("late findings store requires client, namespace, and incident ID")
	}
	initial := EmptyReport(incidentID)
	report, _ := json.Marshal(initial)
	snapshot, _ := reportSnapshot(initial)
	history, _ := json.Marshal([]Snapshot{snapshot})
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName(incidentID), Namespace: s.Namespace, Labels: map[string]string{"app.kubernetes.io/name": "sdo-late-findings"}}, Data: map[string]string{ReportKey: string(report), HistoryKey: string(history)}}
	_, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		prior, readErr := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
		if readErr != nil {
			return readErr
		}
		var old Report
		if json.Unmarshal([]byte(prior.Data[ReportKey]), &old) != nil || old.IncidentID != incidentID {
			return errors.New("late findings ConfigMap identity mismatch")
		}
		return nil
	}
	return err
}

func (s Store) Update(ctx context.Context, event Event) error {
	if event.IncidentID == "" || event.DetectorID == "" || event.Fingerprint == "" {
		return nil
	}
	for _, id := range event.PlaybookIDs { if id=="" || strings.ContainsAny(id,"/\\") || strings.Contains(id,".sdo") { return errors.New("late findings require logical playbook IDs") } }
	if event.Event != "cleared" && !(event.Event == "activated" && event.Relation == "after_dispatch") {
		return nil
	}
	if err := s.Ensure(ctx, event.IncidentID); err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		maps := s.Client.CoreV1().ConfigMaps(s.Namespace)
		cm, err := maps.Get(ctx, ConfigMapName(event.IncidentID), metav1.GetOptions{})
		if err != nil {
			return err
		}
		var report Report
		if err := json.Unmarshal([]byte(cm.Data[ReportKey]), &report); err != nil {
			return err
		}
		if report.IncidentID != event.IncidentID {
			return errors.New("late findings report identity mismatch")
		}
		index := -1
		for i, finding := range report.LateFindings {
			if finding.DetectorID == event.DetectorID && finding.Fingerprint == event.Fingerprint {
				index = i
				break
			}
		}
		if event.Event == "activated" {
			if index < 0 {
				if len(report.LateFindings) >= 64 {
					return errors.New("late findings report exceeds 64 findings")
				}
				report.LateFindings = append(report.LateFindings, Finding{DetectorID: event.DetectorID, DetectorClass: event.DetectorClass, RuleID: event.RuleID, Fingerprint: event.Fingerprint, Severity: event.Severity, ParameterBindings: event.ParameterBindings, PlaybookIDs: append([]string(nil), event.PlaybookIDs...), ActivatedAt: event.RecordedAt, StillActive: true})
			} else {
				report.LateFindings[index].StillActive = true
			}
		} else if index >= 0 {
			report.LateFindings[index].StillActive = false
		} else {
			return nil
		}
		slices.SortFunc(report.LateFindings, func(a, b Finding) int {
			return strings.Compare(a.ActivatedAt+"\x00"+a.DetectorID+"\x00"+a.Fingerprint, b.ActivatedAt+"\x00"+b.DetectorID+"\x00"+b.Fingerprint)
		})
		data, err := json.Marshal(report)
		if err != nil {
			return err
		}
		if len(data) > 256<<10 {
			return fmt.Errorf("late findings report exceeds 256 KiB")
		}
		var history []Snapshot
		if err := json.Unmarshal([]byte(cm.Data[HistoryKey]), &history); err != nil {
			return fmt.Errorf("decode late findings digest history: %w", err)
		}
		snapshot, err := reportSnapshot(report)
		if err != nil {
			return err
		}
		if len(history) == 0 || history[len(history)-1].Digest != snapshot.Digest {
			history = append(history, snapshot)
		}
		if len(history) > 64 {
			history = history[len(history)-64:]
		}
		historyData, err := json.Marshal(history)
		if err != nil {
			return err
		}
		cm.Data[ReportKey] = string(data)
		cm.Data[HistoryKey] = string(historyData)
		_, err = maps.Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

func Digest(report Report) (string, error) {
	data, err := json.Marshal(report)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func reportSnapshot(report Report) (Snapshot, error) {
	digest, err := Digest(report)
	if err != nil {
		return Snapshot{}, err
	}
	detectors := map[string]struct{}{}
	playbooks := map[string]struct{}{}
	for _, finding := range report.LateFindings {
		detectors[finding.DetectorID] = struct{}{}
		for _, id := range finding.PlaybookIDs {
			playbooks[id] = struct{}{}
		}
	}
	snapshot := Snapshot{Digest: digest, DetectorIDs: make([]string, 0, len(detectors)), PlaybookIDs: make([]string, 0, len(playbooks))}
	for id := range detectors {
		snapshot.DetectorIDs = append(snapshot.DetectorIDs, id)
	}
	for id := range playbooks {
		snapshot.PlaybookIDs = append(snapshot.PlaybookIDs, id)
	}
	slices.Sort(snapshot.DetectorIDs)
	slices.Sort(snapshot.PlaybookIDs)
	return snapshot, nil
}

// VerifyPull checks a worker claim against reports produced by this controller.
// A worker can still choose not to invoke the command; pull_count is a claim.
func (s Store) VerifyPull(ctx context.Context, incidentID string, count uint32, digest string, detectorIDs, playbookIDs []string) error {
	if incidentID == "" || count == 0 || digest == "" {
		return errors.New("late findings pull receipt is incomplete")
	}
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, ConfigMapName(incidentID), metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read controller late findings report: %w", err)
	}
	var report Report
	if err := json.Unmarshal([]byte(cm.Data[ReportKey]), &report); err != nil {
		return err
	}
	if report.IncidentID != incidentID {
		return errors.New("late findings report incident mismatch")
	}
	var history []Snapshot
	if err := json.Unmarshal([]byte(cm.Data[HistoryKey]), &history); err != nil {
		return err
	}
	for _, snapshot := range history {
		if snapshot.Digest == digest && slices.Equal(snapshot.DetectorIDs, detectorIDs) && slices.Equal(snapshot.PlaybookIDs, playbookIDs) {
			return nil
		}
	}
	return errors.New("late findings pull does not match controller-authored report history")
}
