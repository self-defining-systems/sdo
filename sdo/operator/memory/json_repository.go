package memory

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sdo.dev/operator/contracts/wire"
	"sort"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/layout"
)

// JSONStore is the repository adapter. No caller needs to know how .sdo
// outcomes are named or represented on disk.
type JSONStore struct{ root string }

func NewJSONStore(repositoryRoot string) *JSONStore { return &JSONStore{root: repositoryRoot} }

// OpenReader resolves the current repository representation behind the
// logical read interface. Runtime callers never depend on its file layout.
func OpenReader(repositoryRoot string) Reader { return NewJSONStore(repositoryRoot) }

func (s *JSONStore) Put(ctx context.Context, record *contracts.OutcomeRecord) (Outcome, error) {
	if record == nil {
		return Outcome{}, fmt.Errorf("outcome record is required")
	}
	document, err := wire.MarshalFull(record)
	if err != nil {
		return Outcome{}, err
	}
	return s.putDocument(ctx, document)
}

func (s *JSONStore) putDocument(ctx context.Context, document []byte) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	outcome, err := parse(document)
	if err != nil {
		return Outcome{}, err
	}
	directory := filepath.Join(s.root, layout.OutcomesDir)
	if err := safeDirectory(directory); err != nil {
		return Outcome{}, err
	}
	path := filepath.Join(directory, layout.OutcomeFileName(outcome.IncidentID))
	file, err := os.CreateTemp(directory, ".outcome-*")
	if err != nil {
		return Outcome{}, fmt.Errorf("create outcome: %w", err)
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(outcome.document); err != nil {
		file.Close()
		return Outcome{}, fmt.Errorf("write outcome: %w", err)
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return Outcome{}, fmt.Errorf("sync outcome: %w", err)
	}
	if err = file.Close(); err != nil {
		return Outcome{}, fmt.Errorf("close outcome: %w", err)
	}
	if err := os.Chmod(file.Name(), 0o644); err != nil {
		return Outcome{}, fmt.Errorf("chmod outcome: %w", err)
	}
	if err := os.Link(file.Name(), path); os.IsExist(err) {
		previous, readErr := s.Get(ctx, outcome.IncidentID)
		if readErr != nil {
			return Outcome{}, readErr
		}
		if !bytes.Equal(previous.document, outcome.document) && !originalOutcomeRetry(previous.Record, outcome.Record) {
			return Outcome{}, fmt.Errorf("%w: %s", ErrConflict, outcome.IncidentID)
		}
		return previous, nil
	} else if err != nil {
		return Outcome{}, fmt.Errorf("publish outcome: %w", err)
	}
	return outcome, nil
}

func (s *JSONStore) Get(ctx context.Context, incidentID string) (Outcome, error) {
	if err := ctx.Err(); err != nil {
		return Outcome{}, err
	}
	if strings.TrimSpace(incidentID) == "" {
		return Outcome{}, fmt.Errorf("incident ID is required")
	}
	path := filepath.Join(s.root, layout.OutcomePath(incidentID))
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		legacy, legacyErr := s.legacyOutcomes(ctx)
		if legacyErr != nil {
			return Outcome{}, legacyErr
		}
		for _, outcome := range legacy {
			if outcome.IncidentID == incidentID {
				return outcome, nil
			}
		}
		return Outcome{}, ErrNotFound
	}
	if err != nil {
		return Outcome{}, fmt.Errorf("stat outcome: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Outcome{}, fmt.Errorf("outcome is not a regular file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Outcome{}, fmt.Errorf("read outcome: %w", err)
	}
	outcome, err := parse(data)
	if err != nil {
		return Outcome{}, fmt.Errorf("invalid stored outcome: %w", err)
	}
	if outcome.IncidentID != incidentID {
		return Outcome{}, fmt.Errorf("outcome filename and incident ID disagree")
	}
	return outcome, nil
}

// List returns every readable outcome. An outcome that cannot be read, decoded
// or validated is skipped and reported by ListWithNotes: one bad document must
// never stop the controller from using the rest of accepted memory.
func (s *JSONStore) List(ctx context.Context) ([]Outcome, error) {
	outcomes, _, err := s.ListWithNotes(ctx)
	return outcomes, err
}

// ListWithNotes is List plus a note per skipped outcome document, in file-name
// order, from the same scan.
func (s *JSONStore) ListWithNotes(ctx context.Context) ([]Outcome, []UnreadableOutcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	directory := filepath.Join(s.root, layout.OutcomesDir)
	entries, err := os.ReadDir(directory)
	if err != nil && !os.IsNotExist(err) {
		return nil, nil, fmt.Errorf("list outcomes: %w", err)
	}
	outcomes := make([]Outcome, 0, len(entries))
	var notes []UnreadableOutcome
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if filepath.Ext(entry.Name()) != ".json" {
			// Not an outcome document (for example a leftover write temp file).
			continue
		}
		outcome, err := readOutcomeEntry(directory, entry)
		if err != nil {
			notes = append(notes, UnreadableOutcome{Name: entry.Name(), Reason: err.Error()})
			continue
		}
		outcomes = append(outcomes, outcome)
	}
	legacy, err := s.legacyOutcomes(ctx)
	if err != nil {
		return nil, nil, err
	}
	known := make(map[string]struct{}, len(outcomes))
	for _, outcome := range outcomes {
		known[outcome.IncidentID] = struct{}{}
	}
	for _, outcome := range legacy {
		if _, stored := known[outcome.IncidentID]; !stored {
			outcomes = append(outcomes, outcome)
		}
	}
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].IncidentID < outcomes[j].IncidentID })
	return outcomes, notes, nil
}

func readOutcomeEntry(directory string, entry os.DirEntry) (Outcome, error) {
	if !entry.Type().IsRegular() {
		return Outcome{}, fmt.Errorf("invalid outcome entry")
	}
	data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
	if err != nil {
		return Outcome{}, fmt.Errorf("read outcome: %w", err)
	}
	outcome, err := parse(data)
	if err != nil {
		return Outcome{}, fmt.Errorf("invalid outcome: %w", err)
	}
	if layout.OutcomeFileName(outcome.IncidentID) != entry.Name() {
		return Outcome{}, fmt.Errorf("outcome filename and incident ID disagree")
	}
	return outcome, nil
}

// legacyOutcomes reads the retired JSONL stream without modifying the
// repository, so readers of an accepted checkout see pre-migration history.
// Playbook references are normalized to logical IDs. An incident repeated with
// identical content is one outcome; conflicting repeats are an error.
func (s *JSONStore) legacyOutcomes(ctx context.Context) ([]Outcome, error) {
	path := filepath.Join(s.root, layout.LegacyOutcomesFile)
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open legacy outcomes: %w", err)
	}
	defer file.Close()
	var outcomes []Outcome
	reader := bufio.NewReader(file)
	for line := 1; ; line++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(data)) != 0 {
			outcome, err := normalizeLegacy(data)
			if err != nil {
				return nil, fmt.Errorf("legacy outcome line %d: %w", line, err)
			}
			duplicate := false
			for _, earlier := range outcomes {
				if earlier.IncidentID != outcome.IncidentID {
					continue
				}
				if !bytes.Equal(earlier.document, outcome.document) {
					return nil, fmt.Errorf("legacy outcome line %d: %w: %s", line, ErrConflict, outcome.IncidentID)
				}
				duplicate = true
			}
			if !duplicate {
				outcomes = append(outcomes, outcome)
			}
		}
		if readErr == io.EOF {
			return outcomes, nil
		}
		if readErr != nil {
			return nil, fmt.Errorf("read legacy outcomes: %w", readErr)
		}
	}
}

func normalizeLegacy(data []byte) (Outcome, error) {
	record := new(contracts.OutcomeRecord)
	if err := wire.DecodeLenientUnvalidated(data, record); err != nil {
		return Outcome{}, err
	}
	for _, list := range []*[]string{
		&record.SurfacedPlaybooks, &record.InspectedPlaybooks, &record.ConfirmedPlaybooks,
		&record.RejectedPlaybooks, &record.AppliedPlaybooks,
	} {
		ids, err := layout.PlaybookIDs(*list)
		if err != nil {
			return Outcome{}, err
		}
		*list = ids
	}
	document, err := wire.MarshalFull(record)
	if err != nil {
		return Outcome{}, err
	}
	return parse(document)
}

// ImportLegacy performs a one-time, restartable migration from the previous
// stream. Playbook references are stored as logical IDs. The stream is deleted
// only after every record has been accepted.
func (s *JSONStore) ImportLegacy(ctx context.Context) error {
	path := filepath.Join(s.root, layout.LegacyOutcomesFile)
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil
	}
	outcomes, err := s.legacyOutcomes(ctx)
	if err != nil {
		return err
	}
	for _, outcome := range outcomes {
		if _, err := s.putDocument(ctx, outcome.document); err != nil {
			return fmt.Errorf("legacy outcome %q: %w", outcome.IncidentID, err)
		}
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove legacy outcomes: %w", err)
	}
	return nil
}

// LogicalOutcome returns record as every stored outcome reads: each text field
// with its physical memory paths rewritten to logical references.
func LogicalOutcome(record *contracts.OutcomeRecord) (*contracts.OutcomeRecord, error) {
	document, err := wire.MarshalFull(record)
	if err != nil {
		return nil, err
	}
	outcome, err := parse(document)
	if err != nil {
		return nil, err
	}
	return outcome.Record, nil
}

// parse is the one path by which outcome documents are written and read, so
// stored outcomes, including memory accepted before agent prose was rewritten,
// never show a worker a physical memory path.
func parse(document []byte) (Outcome, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil || fields == nil {
		return Outcome{}, fmt.Errorf("outcome must be a JSON object")
	}
	document, err := logicalDocument(document)
	if err != nil {
		return Outcome{}, err
	}
	record := new(contracts.OutcomeRecord)
	if err := wire.UnmarshalLenientMigrating(document, record); err != nil {
		return Outcome{}, err
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, bytes.TrimSpace(document), "", "  "); err != nil {
		return Outcome{}, err
	}
	buf.WriteByte('\n')
	return Outcome{Record: record, SchemaVersion: int(record.GetSchemaVersion()), IncidentID: record.GetIncidentId(), Classification: record.GetClassification(), document: buf.Bytes()}, nil
}

func safeDirectory(directory string) error {
	for _, dir := range []string{filepath.Dir(directory), directory} {
		info, err := os.Lstat(dir)
		if os.IsNotExist(err) {
			if err := os.Mkdir(dir, 0o755); err != nil {
				return fmt.Errorf("create outcome directory: %w", err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("outcome directory is not a directory: %s", dir)
		}
	}
	return nil
}

// logicalDocument rewrites every string in a JSON document, known or
// historical field, through layout.LogicalizeText. A document without a
// physical memory path is returned unchanged.
func logicalDocument(document []byte) ([]byte, error) {
	if !layout.ContainsPhysicalPath(string(document)) {
		return document, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var tree any
	if err := decoder.Decode(&tree); err != nil {
		return nil, err
	}
	var logical func(any) any
	logical = func(value any) any {
		switch value := value.(type) {
		case string:
			return layout.LogicalizeText(value)
		case []any:
			for i := range value {
				value[i] = logical(value[i])
			}
		case map[string]any:
			rewritten := make(map[string]any, len(value))
			for key, item := range value {
				rewritten[layout.LogicalizeText(key)] = logical(item)
			}
			return rewritten
		}
		return value
	}
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(logical(tree)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
