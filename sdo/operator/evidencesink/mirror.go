package evidencesink

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
)

// Place maps one runtime evidence path (relative to .sdo-runtime) to its sink path, or reports false for a path
// that is not allowed evidence:
//
//	sessions/<digest>/...   -> incidents/<digest>/...   (one incident's agent session)
//	tasks/<key>/...         -> lifecycle/<key>/...      (one lifecycle task: designer, deployer, judge)
//	usage/*, telemetry/*    -> usage/*, telemetry/*
//	setup/..., codex/..., claude/... -> controller/...  (agent sessions run by the controller process itself)
func Place(runtimeRelative string) (string, bool) {
	if checkRelative(runtimeRelative) != nil || !Allowed(runtimeRelative) {
		return "", false
	}
	head, rest, _ := strings.Cut(runtimeRelative, "/")
	switch head {
	case "sessions":
		return "incidents/" + rest, true
	case "tasks":
		return "lifecycle/" + rest, true
	case "usage", "telemetry":
		return runtimeRelative, true
	case "setup", "codex", "claude":
		return "controller/" + runtimeRelative, true
	}
	return "", false
}

// IncidentPath is the sink directory of one incident: the same digest names its runtime session directory.
func IncidentPath(incidentID string) string {
	return "incidents/" + fmt.Sprintf("%x", sha256.Sum256([]byte(incidentID)))[:32]
}

type sourceState struct {
	size    int64
	modTime time.Time
}

type storedFile struct {
	sha256 string
	bytes  int64
}

// Mirror copies allowed runtime evidence and broker ledgers into a sink and seals finished units. It only reads
// the runtime root and never deletes from the sink. Pass is deterministic given the two trees and the clock.
type Mirror struct {
	// RuntimeRoot is the absolute runtime evidence root (<repository mount>/.sdo-runtime).
	RuntimeRoot string
	// LedgerRoot is the absolute native broker ledger directory; empty mirrors no ledgers.
	LedgerRoot string
	Sink       Sink
	Now        func() time.Time

	loaded  bool
	sources map[string]sourceState // sink path -> last copied source state
	stored  map[string]storedFile  // sink path -> stored content
	sealed  map[string]string      // unit path -> last sealed digest
	dirty   map[string]bool        // unit path -> content changed since its last seal
}

type ledgerUnit struct {
	path   string
	ledger *contracts.BrokerLedger
	raw    []byte
}

// Pass runs one mirror pass: read ledgers, copy changed evidence, then seal. Ledgers are read first so every file
// written before the ledger state that triggers a seal is already copied when the seal is computed.
func (m *Mirror) Pass() error {
	if m.Sink == nil || m.Now == nil || !filepath.IsAbs(m.RuntimeRoot) {
		return errors.New("evidence mirror requires a sink, a clock and an absolute runtime root")
	}
	if !m.loaded {
		if err := m.load(); err != nil {
			return err
		}
	}
	ledgers, ledgerErr := m.readLedgers()
	copyErr := m.copyRuntime()
	var errs []error
	for _, unit := range ledgers {
		if err := m.put(unit.path+"/"+LedgerFile, unit.raw); err != nil {
			errs = append(errs, err)
		}
	}
	for _, unit := range ledgers {
		if unit.ledger.GetAcknowledged() {
			if err := m.seal(unit.path, incidentRecord(unit.ledger)); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := m.sealTasks(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(append([]error{ledgerErr, copyErr}, errs...)...)
}

func (m *Mirror) load() error {
	m.sources, m.stored, m.sealed, m.dirty = map[string]sourceState{}, map[string]storedFile{}, map[string]string{}, map[string]bool{}
	records, err := m.Sink.Records()
	if err != nil {
		return fmt.Errorf("read evidence index: %w", err)
	}
	for _, record := range records {
		m.sealed[record.Path] = record.SHA256
	}
	m.loaded = true
	return nil
}

func (m *Mirror) readLedgers() ([]ledgerUnit, error) {
	if m.LedgerRoot == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(m.LedgerRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var units []ledgerUnit
	var errs []error
	for _, entry := range entries {
		name := entry.Name()
		if !entry.Type().IsRegular() || !strings.HasSuffix(name, ".json") || len(name) != 64+len(".json") {
			continue
		}
		data, err := readRegular(filepath.Join(m.LedgerRoot, name), 2<<20)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		ledger := &contracts.BrokerLedger{}
		// Lenient and unvalidated: the mirror reads only identity, times and outcome, and must not drop evidence
		// over a field another build added.
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(data, ledger); err != nil {
			continue
		}
		if fmt.Sprintf("%x.json", sha256.Sum256([]byte(ledger.GetIncidentId()))) != name {
			continue
		}
		units = append(units, ledgerUnit{path: IncidentPath(ledger.GetIncidentId()), ledger: ledger, raw: data})
	}
	return units, errors.Join(errs...)
}

func (m *Mirror) copyRuntime() error {
	if _, err := os.Lstat(m.RuntimeRoot); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	var errs []error
	walkErr := filepath.WalkDir(m.RuntimeRoot, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			if name == m.RuntimeRoot {
				return err
			}
			// A directory removed or unreadable during the walk is skipped.
			return nil
		}
		if name == m.RuntimeRoot {
			return nil
		}
		relative, err := filepath.Rel(m.RuntimeRoot, name)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if entry.IsDir() {
			if !descend(relative) {
				return filepath.SkipDir
			}
			return nil
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		target, ok := Place(relative)
		if !ok {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return nil
		}
		state := sourceState{size: info.Size(), modTime: info.ModTime()}
		if previous, seen := m.sources[target]; seen && previous == state {
			return nil
		}
		if err := m.copyFile(name, target, state); err != nil {
			errs = append(errs, err)
		}
		return nil
	})
	return errors.Join(append(errs, walkErr)...)
}

// copyFile copies the file's first state.size bytes: a session still appending to its rollout is mirrored as a
// consistent prefix, and the rest arrives on a later pass.
func (m *Mirror) copyFile(source, target string, state sourceState) error {
	file, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	size := min(state.size, MaxFileBytes)
	stored, err := m.Sink.Put(target, io.LimitReader(file, size))
	if err != nil {
		return fmt.Errorf("mirror %s: %w", target, err)
	}
	m.sources[target] = state
	m.record(target, stored)
	return nil
}

func (m *Mirror) put(target string, data []byte) error {
	stored, err := m.Sink.Put(target, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("mirror %s: %w", target, err)
	}
	m.record(target, stored)
	return nil
}

func (m *Mirror) record(target string, stored Stored) {
	m.stored[target] = storedFile{sha256: stored.SHA256, bytes: stored.Bytes}
	if stored.Changed {
		if unit := unitOf(target); unit != "" {
			m.dirty[unit] = true
		}
	}
}

// unitOf returns the sealable unit (incidents/<digest> or lifecycle/<key>) holding a sink path.
func unitOf(target string) string {
	parts := strings.SplitN(target, "/", 3)
	if len(parts) == 3 && (parts[0] == "incidents" || parts[0] == "lifecycle") {
		return parts[0] + "/" + parts[1]
	}
	return ""
}

type sealEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type sealDocument struct {
	Schema   string      `json:"schema"`
	Path     string      `json:"path"`
	SealedAt time.Time   `json:"sealed_at"`
	Files    []sealEntry `json:"files"`
}

// seal writes the unit's seal.json (every file with its digest) and appends one index record, once per unit and
// again only when the unit's content changed after its last seal.
func (m *Mirror) seal(unit string, record Record) error {
	if _, sealed := m.sealed[unit]; sealed && !m.dirty[unit] {
		return nil
	}
	document := sealDocument{Schema: SealSchema, Path: unit, SealedAt: m.Now().UTC()}
	var total int64
	for target, file := range m.stored {
		if unitOf(target) != unit || target == unit+"/"+SealFile {
			continue
		}
		document.Files = append(document.Files, sealEntry{Path: strings.TrimPrefix(target, unit+"/"), SHA256: file.sha256, Bytes: file.bytes})
		total += file.bytes
	}
	if len(document.Files) == 0 {
		return nil
	}
	sort.Slice(document.Files, func(i, j int) bool { return document.Files[i].Path < document.Files[j].Path })
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := m.Sink.Put(unit+"/"+SealFile, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("seal %s: %w", unit, err)
	}
	digest := sha256.Sum256(data)
	record.Path, record.SealedAt, record.SHA256 = unit, document.SealedAt, hex.EncodeToString(digest[:])
	record.Files, record.Bytes = len(document.Files), total
	if err := m.Sink.Append(record); err != nil {
		return fmt.Errorf("index %s: %w", unit, err)
	}
	m.sealed[unit] = record.SHA256
	delete(m.dirty, unit)
	return nil
}

func incidentRecord(ledger *contracts.BrokerLedger) Record {
	record := Record{Kind: KindIncident, ID: ledger.GetIncidentId()}
	closure := ledger.GetClosure()
	if at := closure.GetDetectedAt(); at != nil {
		value := at.AsTime().UTC()
		record.DetectedAt = &value
	}
	if at := closure.GetVerifiedAt(); at != nil {
		value := at.AsTime().UTC()
		record.VerifiedAt = &value
	}
	if at := ledger.GetAcknowledgedAt(); at != nil {
		value := at.AsTime().UTC()
		record.AcknowledgedAt = &value
	}
	outcome := ledger.GetOutcomeDocument()
	record.Outcome = outcome.GetClassification()
	seen := map[string]bool{}
	for _, cause := range outcome.GetConfirmedRootCauses() {
		if class := cause.GetCauseClass(); class != "" && !seen[class] {
			seen[class] = true
			record.CauseClasses = append(record.CauseClasses, class)
		}
	}
	return record
}

type taskIdentity struct {
	TaskID   string `json:"task_id"`
	AgentID  string `json:"agent_id"`
	TaskType string `json:"task_type"`
}

// sealTasks seals each lifecycle task whose result or error is recorded.
func (m *Mirror) sealTasks() error {
	units := map[string]bool{}
	for target := range m.stored {
		if unit := unitOf(target); strings.HasPrefix(unit, "lifecycle/") {
			units[unit] = true
		}
	}
	names := make([]string, 0, len(units))
	for unit := range units {
		names = append(names, unit)
	}
	sort.Strings(names)
	var errs []error
	for _, unit := range names {
		status := ""
		if _, ok := m.stored[unit+"/result.json"]; ok {
			status = "succeeded"
		} else if _, ok := m.stored[unit+"/error.json"]; ok {
			status = "failed"
		}
		if status == "" {
			continue
		}
		identity := taskIdentity{}
		key := strings.TrimPrefix(unit, "lifecycle/")
		if data, err := readRegular(filepath.Join(m.RuntimeRoot, "tasks", key, "task.json"), 64<<10); err == nil {
			_ = json.Unmarshal(data, &identity)
		}
		if identity.TaskID == "" {
			identity.TaskID = key
		}
		if err := m.seal(unit, Record{Kind: KindTask, ID: identity.TaskID, AgentID: identity.AgentID, TaskType: identity.TaskType, Status: status}); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func readRegular(name string, limit int64) ([]byte, error) {
	file, err := os.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return nil, fmt.Errorf("%s is not a regular file within %d bytes", path.Base(filepath.ToSlash(name)), limit)
	}
	return io.ReadAll(io.LimitReader(file, limit))
}
