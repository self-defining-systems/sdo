package operatorcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"sdo.dev/operator/latefindings"
)

const defaultLateReportPath = "/sdo/late-findings/report.json"
const defaultLateReceiptsPath = "/tmp/sdo-late-findings-pulls.jsonl"

type LateFindingsConfig struct{ IncidentID, ReportPath, ReceiptsPath string }

func ParseLateFindingsLaunch(args []string) (LateFindingsConfig, error) {
	if len(args) == 0 || args[0] != "late-findings" {
		return LateFindingsConfig{}, errors.New("expected late-findings subcommand")
	}
	flags := flag.NewFlagSet("sdo-operator late-findings", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	id := flags.String("incident-id", "", "logical incident identity")
	if err := flags.Parse(args[1:]); err != nil {
		return LateFindingsConfig{}, err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*id) == "" {
		return LateFindingsConfig{}, errors.New("late-findings requires --incident-id and no extra arguments")
	}
	path := os.Getenv("SDO_LATE_FINDINGS_REPORT_PATH")
	if path == "" {
		path = defaultLateReportPath
	}
	receipts := os.Getenv("SDO_LATE_FINDINGS_RECEIPTS_PATH")
	if receipts == "" {
		receipts = defaultLateReceiptsPath
	}
	if !filepath.IsAbs(path) || !filepath.IsAbs(receipts) {
		return LateFindingsConfig{}, errors.New("late findings report and receipt paths must be absolute")
	}
	return LateFindingsConfig{IncidentID: *id, ReportPath: path, ReceiptsPath: receipts}, nil
}

func RunLateFindings(_ context.Context, cfg LateFindingsConfig, output io.Writer) error {
	if cfg.IncidentID == "" || output == nil {
		return errors.New("late-findings requires incident ID and output")
	}
	data, err := os.ReadFile(cfg.ReportPath)
	if err != nil {
		return fmt.Errorf("read controller late findings projection: %w", err)
	}
	if len(data) > 256<<10 {
		return errors.New("late findings projection exceeds limit")
	}
	var report latefindings.Report
	if err := json.Unmarshal(data, &report); err != nil {
		return fmt.Errorf("decode controller late findings projection: %w", err)
	}
	if report.SchemaVersion != "sdo.late-findings/v1" || report.IncidentID != cfg.IncidentID {
		return errors.New("late findings projection belongs to another incident or schema")
	}
	digest, err := latefindings.Digest(report)
	if err != nil {
		return err
	}
	detectors := map[string]struct{}{}
	playbooks := map[string]struct{}{}
	for _, finding := range report.LateFindings {
		if finding.DetectorID == "" || finding.Fingerprint == "" {
			return errors.New("late findings projection contains incomplete finding")
		}
		detectors[finding.DetectorID] = struct{}{}
		for _, id := range finding.PlaybookIDs {
			if id == "" || strings.Contains(id, "/") {
				return errors.New("late findings projection contains nonlogical playbook ID")
			}
			playbooks[id] = struct{}{}
		}
	}
	detectorIDs := make([]string, 0, len(detectors))
	for id := range detectors {
		detectorIDs = append(detectorIDs, id)
	}
	slices.Sort(detectorIDs)
	playbookIDs := make([]string, 0, len(playbooks))
	for id := range playbooks {
		playbookIDs = append(playbookIDs, id)
	}
	slices.Sort(playbookIDs)
	receipt := struct {
		IncidentID   string   `json:"incident_id"`
		DetectorIDs  []string `json:"detector_ids"`
		PlaybookIDs  []string `json:"playbook_ids"`
		ReportDigest string   `json:"report_digest"`
	}{cfg.IncidentID, detectorIDs, playbookIDs, digest}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(cfg.ReceiptsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("record late findings pull: %w", err)
	}
	if _, err = file.Write(append(encoded, '\n')); err != nil {
		file.Close()
		return fmt.Errorf("record late findings pull: %w", err)
	}
	if err = file.Close(); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, string(data))
	return err
}
