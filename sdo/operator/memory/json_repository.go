package memory

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
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
	document, err := (protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}).Marshal(record)
	if err != nil {
		return Outcome{}, fmt.Errorf("encode outcome contract: %w", err)
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
	directory := filepath.Join(s.root, ".sdo", "outcomes")
	if err := safeDirectory(directory); err != nil {
		return Outcome{}, err
	}
	path := filepath.Join(directory, filename(outcome.IncidentID))
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
		if !bytes.Equal(previous.document, outcome.document) {
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
	path := filepath.Join(s.root, ".sdo", "outcomes", filename(incidentID))
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
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

func (s *JSONStore) List(ctx context.Context) ([]Outcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directory := filepath.Join(s.root, ".sdo", "outcomes")
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list outcomes: %w", err)
	}
	outcomes := make([]Outcome, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".json" {
			return nil, fmt.Errorf("invalid outcome entry %q", entry.Name())
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read outcome %q: %w", entry.Name(), err)
		}
		outcome, err := parse(data)
		if err != nil {
			return nil, fmt.Errorf("invalid outcome %q: %w", entry.Name(), err)
		}
		if filename(outcome.IncidentID) != entry.Name() {
			return nil, fmt.Errorf("outcome filename and incident ID disagree: %q", entry.Name())
		}
		outcomes = append(outcomes, outcome)
	}
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].IncidentID < outcomes[j].IncidentID })
	return outcomes, nil
}

// ImportLegacy performs a one-time, restartable migration from the previous
// stream. The stream is deleted only after every record has been accepted.
func (s *JSONStore) ImportLegacy(ctx context.Context) error {
	path := filepath.Join(s.root, ".sdo", "outcomes.jsonl")
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("open legacy outcomes: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for line := 1; ; line++ {
		data, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(data)) != 0 {
			if _, err := s.putDocument(ctx, data); err != nil {
				return fmt.Errorf("legacy outcome line %d: %w", line, err)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read legacy outcomes: %w", readErr)
		}
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close legacy outcomes: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("remove legacy outcomes: %w", err)
	}
	return nil
}

func parse(document []byte) (Outcome, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil || fields == nil {
		return Outcome{}, fmt.Errorf("outcome must be a JSON object")
	}
	record := new(contracts.OutcomeRecord)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(document, record); err != nil {
		return Outcome{}, fmt.Errorf("invalid outcome contract: %w", err)
	}
	if record.GetSchemaVersion() != 1 {
		return Outcome{}, fmt.Errorf("unsupported outcome schema version %d", record.GetSchemaVersion())
	}
	if strings.TrimSpace(record.GetIncidentId()) == "" {
		return Outcome{}, fmt.Errorf("incident ID is required")
	}
	switch record.GetClassification() {
	case "success", "partial", "external_recovery", "cleared_without_action", "false_positive", "false_negative", "failed", "cancelled":
	default:
		return Outcome{}, fmt.Errorf("unknown outcome classification %q", record.GetClassification())
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, bytes.TrimSpace(document), "", "  "); err != nil {
		return Outcome{}, err
	}
	buf.WriteByte('\n')
	return Outcome{Record: record, SchemaVersion: int(record.GetSchemaVersion()), IncidentID: record.GetIncidentId(), Classification: record.GetClassification(), document: buf.Bytes()}, nil
}

func filename(incidentID string) string {
	return fmt.Sprintf("%x.json", sha256.Sum256([]byte(incidentID)))
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
