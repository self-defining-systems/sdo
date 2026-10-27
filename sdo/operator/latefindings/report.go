// Package latefindings owns the logical, bounded incident telemetry projection.
// Workers receive only a read-only ConfigMap projection, never controller storage.
// The report, its history, and the worker's pull receipt are the generated
// LateFindings* messages; this package holds no parallel Go shapes.
package latefindings

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

const ReportKey = "report.json"
const HistoryKey = "history.json"

// DefaultReportPath is where the controller mounts the report in a worker.
const DefaultReportPath = "/sdo/late-findings/report.json"

// DefaultReceiptsPath is where `sdo-operator late-findings` appends one receipt
// per pull. The dispatcher passes it to the worker as
// SDO_LATE_FINDINGS_RECEIPTS_PATH, so the worker reads the path from there
// rather than carrying its own copy.
const DefaultReceiptsPath = "/tmp/sdo-late-findings-pulls.jsonl"

// ReceiptsPathEnv and ReportPathEnv override the defaults.
const (
	ReceiptsPathEnv = "SDO_LATE_FINDINGS_RECEIPTS_PATH"
	ReportPathEnv   = "SDO_LATE_FINDINGS_REPORT_PATH"
)

// maxReportBytes bounds the encoded report.
const maxReportBytes = 256 << 10

// SchemaVersion is the const rule of LateFindingsReport.schema_version.
var SchemaVersion = wire.StringConst(&contracts.LateFindingsReport{}, "schema_version")

type Store struct {
	Client    kubernetes.Interface
	Namespace string
}

func ConfigMapName(incidentID string) string {
	sum := sha256.Sum256([]byte(incidentID))
	return "sdo-late-" + hex.EncodeToString(sum[:10])
}

func EmptyReport(incidentID string) *contracts.LateFindingsReport {
	return &contracts.LateFindingsReport{SchemaVersion: SchemaVersion, IncidentId: incidentID, TelemetryAvailable: true}
}

// ParseReport decodes and validates a report document.
func ParseReport(data []byte) (*contracts.LateFindingsReport, error) {
	if len(data) > maxReportBytes {
		return nil, errors.New("late findings projection exceeds limit")
	}
	report := &contracts.LateFindingsReport{}
	if err := wire.Unmarshal(data, report); err != nil {
		return nil, err
	}
	return report, nil
}

func (s Store) Ensure(ctx context.Context, incidentID string) error {
	if s.Client == nil || s.Namespace == "" || incidentID == "" {
		return errors.New("late findings store requires client, namespace, and incident ID")
	}
	initial := EmptyReport(incidentID)
	report, err := wire.MarshalFull(initial)
	if err != nil {
		return err
	}
	snapshot, err := ReportSnapshot(initial)
	if err != nil {
		return err
	}
	history, err := wire.MarshalFull(&contracts.LateFindingsHistory{Snapshots: []*contracts.LateFindingsSnapshot{snapshot}})
	if err != nil {
		return err
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: ConfigMapName(incidentID), Namespace: s.Namespace, Labels: map[string]string{"app.kubernetes.io/name": "sdo-late-findings"}}, Data: map[string]string{ReportKey: string(report), HistoryKey: string(history)}}
	_, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		prior, readErr := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, cm.Name, metav1.GetOptions{})
		if readErr != nil {
			return readErr
		}
		old, parseErr := ParseReport([]byte(prior.Data[ReportKey]))
		if parseErr != nil || old.GetIncidentId() != incidentID {
			return errors.New("late findings ConfigMap identity mismatch")
		}
		return nil
	}
	return err
}

// Update folds one firing record into the incident's report. Only a clear, or
// an activation after the responder was dispatched, belongs to the report.
func (s Store) Update(ctx context.Context, record *contracts.FiringRecord) error {
	event := record.GetEvent()
	if record.GetIncidentId() == "" || record.GetDetectorId() == "" || record.GetFingerprint() == "" {
		return nil
	}
	if event != "cleared" && !(event == "activated" && record.GetDispatchRelation() == "after_dispatch") {
		return nil
	}
	if err := s.Ensure(ctx, record.GetIncidentId()); err != nil {
		return err
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		maps := s.Client.CoreV1().ConfigMaps(s.Namespace)
		cm, err := maps.Get(ctx, ConfigMapName(record.GetIncidentId()), metav1.GetOptions{})
		if err != nil {
			return err
		}
		report, err := ParseReport([]byte(cm.Data[ReportKey]))
		if err != nil {
			return err
		}
		if report.GetIncidentId() != record.GetIncidentId() {
			return errors.New("late findings report identity mismatch")
		}
		index := slices.IndexFunc(report.LateFindings, func(finding *contracts.LateFinding) bool {
			return finding.GetDetectorId() == record.GetDetectorId() && finding.GetFingerprint() == record.GetFingerprint()
		})
		if event == "activated" {
			if index < 0 {
				report.LateFindings = append(report.LateFindings, &contracts.LateFinding{
					DetectorId: record.GetDetectorId(), DetectorClass: record.GetDetectorClass(), RuleId: record.GetRuleId(),
					Fingerprint: record.GetFingerprint(), Severity: record.GetSeverity(), ParameterBindings: record.GetParameterBindings(),
					PlaybookIds: slices.Clone(record.GetSurfacedPlaybooks()), ActivatedAt: record.GetRecordedAt(), StillActive: true,
				})
			} else {
				report.LateFindings[index].StillActive = true
			}
		} else if index >= 0 {
			report.LateFindings[index].StillActive = false
		} else {
			return nil
		}
		slices.SortFunc(report.LateFindings, func(a, b *contracts.LateFinding) int {
			return strings.Compare(findingOrder(a), findingOrder(b))
		})
		data, err := wire.MarshalFull(report)
		if err != nil {
			return err
		}
		if len(data) > maxReportBytes {
			return fmt.Errorf("late findings report exceeds 256 KiB")
		}
		history := &contracts.LateFindingsHistory{}
		if err := wire.Unmarshal([]byte(cm.Data[HistoryKey]), history); err != nil {
			return fmt.Errorf("decode late findings digest history: %w", err)
		}
		snapshot, err := ReportSnapshot(report)
		if err != nil {
			return err
		}
		snapshots := history.Snapshots
		if len(snapshots) == 0 || snapshots[len(snapshots)-1].GetDigest() != snapshot.GetDigest() {
			snapshots = append(snapshots, snapshot)
		}
		if len(snapshots) > 64 {
			snapshots = snapshots[len(snapshots)-64:]
		}
		historyData, err := wire.MarshalFull(&contracts.LateFindingsHistory{Snapshots: snapshots})
		if err != nil {
			return err
		}
		cm.Data[ReportKey] = string(data)
		cm.Data[HistoryKey] = string(historyData)
		_, err = maps.Update(ctx, cm, metav1.UpdateOptions{})
		return err
	})
}

func findingOrder(finding *contracts.LateFinding) string {
	at := finding.GetActivatedAt()
	return fmt.Sprintf("%020d.%09d\x00%s\x00%s", at.GetSeconds(), at.GetNanos(), finding.GetDetectorId(), finding.GetFingerprint())
}

// Digest identifies one report by the SHA-256 of its deterministic encoding,
// so the controller and the worker-side command agree regardless of how the
// ProtoJSON document was formatted.
func Digest(report *contracts.LateFindingsReport) (string, error) {
	encoded, err := wire.Digest(report)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// ReportSnapshot names a report version: its digest and the sorted detector and
// playbook IDs it covers.
func ReportSnapshot(report *contracts.LateFindingsReport) (*contracts.LateFindingsSnapshot, error) {
	digest, err := Digest(report)
	if err != nil {
		return nil, err
	}
	detectors := map[string]struct{}{}
	playbooks := map[string]struct{}{}
	for _, finding := range report.GetLateFindings() {
		detectors[finding.GetDetectorId()] = struct{}{}
		for _, id := range finding.GetPlaybookIds() {
			playbooks[id] = struct{}{}
		}
	}
	snapshot := &contracts.LateFindingsSnapshot{Digest: digest, DetectorIds: sortedKeys(detectors), PlaybookIds: sortedKeys(playbooks)}
	return snapshot, nil
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
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
	report, err := ParseReport([]byte(cm.Data[ReportKey]))
	if err != nil {
		return err
	}
	if report.GetIncidentId() != incidentID {
		return errors.New("late findings report incident mismatch")
	}
	history := &contracts.LateFindingsHistory{}
	if err := wire.Unmarshal([]byte(cm.Data[HistoryKey]), history); err != nil {
		return err
	}
	for _, snapshot := range history.GetSnapshots() {
		if snapshot.GetDigest() == digest && slices.Equal(snapshot.GetDetectorIds(), detectorIDs) && slices.Equal(snapshot.GetPlaybookIds(), playbookIDs) {
			return nil
		}
	}
	return errors.New("late findings pull does not match controller-authored report history")
}
