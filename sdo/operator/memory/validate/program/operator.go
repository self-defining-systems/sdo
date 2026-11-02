package program

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"sdo.dev/operator/sdk/sdktest"
)

// OperatorProvenance explicitly distinguishes hand-written inputs from captures.
// Accepted Git trailers attribute all imported artifacts to the human operator.
type OperatorProvenance struct {
	SchemaVersion int    `json:"schema_version"`
	Origin        string `json:"origin"`
	Description   string `json:"description"`
}

// OperatorCorpus recognizes operator.json without relaxing capture provenance.
// It requires a separate explicit source inventory for production executor replay.
func OperatorCorpus(dir string) (bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "operator.json"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var p OperatorProvenance
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		return true, fmt.Errorf("operator corpus %s: invalid operator provenance", filepath.Base(dir))
	}
	if err := decoder.Decode(new(any)); err != io.EOF || p.SchemaVersion != 1 || p.Origin != "operator" || strings.TrimSpace(p.Description) == "" {
		return true, fmt.Errorf("operator corpus %s: requires schema_version 1, origin operator, and description", filepath.Base(dir))
	}
	c, err := sdktest.LoadCorpusMetadata(dir)
	if err != nil {
		return true, err
	}
	if c.Provenance != (sdktest.CorpusProvenance{}) || c.SyntheticNoMatchReason != "" {
		return true, fmt.Errorf("operator corpus %s must not claim captured or synthetic-negative provenance", filepath.Base(dir))
	}
	if len(c.RootCauseProgramIDs) == 0 {
		return true, fmt.Errorf("operator corpus %s requires positive program labels", filepath.Base(dir))
	}
	if info, err := os.Lstat(filepath.Join(dir, "accepted.json")); err != nil || !info.Mode().IsRegular() {
		return true, fmt.Errorf("operator corpus %s requires regular accepted.json source inventory", filepath.Base(dir))
	}
	return true, nil
}
