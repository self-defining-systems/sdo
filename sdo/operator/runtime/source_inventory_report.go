package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sdo.dev/operator/rendered"
	"sdo.dev/operator/sdk"
)

// WriteSourceInventoryReport is the output edge of --evaluate-once
// --source-drift-report. The check is read-only; only the requested file is
// written. It does not start probers, dispatch workers or adopt source fields.
func WriteSourceInventoryReport(stdout io.Writer, path string, index *SourceInventory, objects []*unstructured.Unstructured) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("source-drift-report requires an absolute output path")
	}
	findings := index.Findings(objects, time.Now().UTC())
	report := struct {
		Namespace    string                          `json:"namespace"`
		Revision     string                          `json:"accepted_commit"`
		Healthy      bool                            `json:"healthy"`
		Findings     []sdk.Finding                   `json:"findings"`
		EvidenceGaps []rendered.InventoryEvidenceGap `json:"evidence_gaps"`
	}{index.Namespace, index.Revision, len(findings) == 0, findings, index.EvidenceGaps}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "source drift: %d unexplained objects, %d evidence gaps at %s; full JSON: %s\n", len(findings), len(index.EvidenceGaps), index.Revision, path)
	return err
}
