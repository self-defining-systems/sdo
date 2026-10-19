package operatorcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/latefindings"
)

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
	path := os.Getenv(latefindings.ReportPathEnv)
	if path == "" {
		path = latefindings.DefaultReportPath
	}
	receipts := os.Getenv(latefindings.ReceiptsPathEnv)
	if receipts == "" {
		receipts = latefindings.DefaultReceiptsPath
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
	report, err := latefindings.ParseReport(data)
	if err != nil {
		return fmt.Errorf("decode controller late findings projection: %w", err)
	}
	if report.GetIncidentId() != cfg.IncidentID {
		return errors.New("late findings projection belongs to another incident")
	}
	snapshot, err := latefindings.ReportSnapshot(report)
	if err != nil {
		return err
	}
	encoded, err := wire.Marshal(&contracts.LateFindingsReceipt{
		IncidentId: cfg.IncidentID, DetectorIds: snapshot.GetDetectorIds(), PlaybookIds: snapshot.GetPlaybookIds(), ReportDigest: snapshot.GetDigest(),
	})
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
