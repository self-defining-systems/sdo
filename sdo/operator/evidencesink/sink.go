// Package evidencesink externalizes SDO's run evidence (agent session rollouts, task requests and results, usage,
// telemetry, command reports, broker ledgers) to a sink the runner owns, so it survives the control namespace.
//
// The first sink is a directory of structured files with an append-only index (Dir). The Sink interface is the
// seam for a database sink later. Mirror fills a sink continuously from the runtime evidence root.
package evidencesink

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Layout constants. The layout is versioned: a reader checks layout.json's schema.
const (
	LayoutSchema = "sdo.evidence/v1"
	IndexSchema  = "sdo.evidence-index/v1"
	SealSchema   = "sdo.evidence-seal/v1"

	LayoutFile = "layout.json"
	IndexFile  = "index.jsonl"
	SealFile   = "seal.json"
	LedgerFile = "ledger.json"

	// IncidentIdentityFile is written into each incident session directory with the exact incident ID, because
	// the directory is named by a digest of the ID.
	IncidentIdentityFile = "incident.json"

	KindIncident = "incident"
	KindTask     = "task"

	// MaxFileBytes caps one mirrored file; a larger file is mirrored as its prefix.
	MaxFileBytes = 128 << 20
)

var runIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ValidRunID reports whether id can name a run directory in a sink.
func ValidRunID(id string) bool { return runIDPattern.MatchString(id) }

// Stored describes one file after Put.
type Stored struct {
	SHA256 string
	Bytes  int64
	// Changed is false when the sink already held identical content.
	Changed bool
}

// Record is one line of index.jsonl: one sealed incident or lifecycle task. A unit resealed after later evidence
// arrived (for example a reflection after closure) gets another record; the last record for a path is current.
type Record struct {
	Schema   string    `json:"schema"`
	Kind     string    `json:"kind"`
	ID       string    `json:"id"`
	Path     string    `json:"path"`
	SealedAt time.Time `json:"sealed_at"`
	// SHA256 is the digest of the unit's seal.json, which lists every file with its own digest.
	SHA256 string `json:"sha256"`
	Files  int    `json:"files"`
	Bytes  int64  `json:"bytes"`

	DetectedAt     *time.Time `json:"detected_at,omitempty"`
	VerifiedAt     *time.Time `json:"verified_at,omitempty"`
	AcknowledgedAt *time.Time `json:"acknowledged_at,omitempty"`
	Outcome        string     `json:"outcome,omitempty"`
	CauseClasses   []string   `json:"cause_classes,omitempty"`

	AgentID  string `json:"agent_id,omitempty"`
	TaskType string `json:"task_type,omitempty"`
	Status   string `json:"status,omitempty"`
}

// Dir is the evidence sink of one run: <root>/<run-id>/ holding layout.json, index.jsonl and the evidence tree.
// Paths are slash-separated and relative to the run.
// Directories are world-writable and files world-readable so the runner (a different uid than the controller)
// can read and clean them up.
type Dir struct {
	root string
}

type layoutDocument struct {
	Schema    string    `json:"schema"`
	RunID     string    `json:"run_id"`
	CreatedAt time.Time `json:"created_at"`
}

// OpenDir opens or creates the run directory under sinkRoot and records the layout version once.
func OpenDir(sinkRoot, runID string, now time.Time) (*Dir, error) {
	if !filepath.IsAbs(sinkRoot) {
		return nil, errors.New("evidence sink root must be absolute")
	}
	if !ValidRunID(runID) {
		return nil, fmt.Errorf("invalid evidence run ID %q", runID)
	}
	info, err := os.Stat(sinkRoot)
	if err != nil {
		return nil, fmt.Errorf("evidence sink root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("evidence sink root %s is not a directory", sinkRoot)
	}
	dir := &Dir{root: filepath.Join(sinkRoot, runID)}
	if err := mkdirShared(dir.root); err != nil {
		return nil, err
	}
	layoutPath := filepath.Join(dir.root, LayoutFile)
	data, err := os.ReadFile(layoutPath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		document, err := json.Marshal(layoutDocument{Schema: LayoutSchema, RunID: runID, CreatedAt: now.UTC()})
		if err != nil {
			return nil, err
		}
		if _, err := dir.Put(LayoutFile, bytes.NewReader(append(document, '\n'))); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		var existing layoutDocument
		if err := json.Unmarshal(data, &existing); err != nil || existing.Schema != LayoutSchema {
			return nil, fmt.Errorf("evidence run directory %s has an unsupported layout", dir.root)
		}
	}
	return dir, nil
}

// Root is the run directory.
func (d *Dir) Root() string { return d.root }

func checkRelative(relative string) error {
	if relative == "" || path.IsAbs(relative) || path.Clean(relative) != relative || relative == "." || strings.HasPrefix(relative, "../") || relative == ".." || strings.Contains(relative, "\\") {
		return fmt.Errorf("unsafe evidence path %q", relative)
	}
	return nil
}

func mkdirShared(directory string) error {
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	return os.Chmod(directory, 0o777)
}

func (d *Dir) mkdirs(relativeDir string) error {
	if relativeDir == "." {
		return nil
	}
	current := d.root
	for _, part := range strings.Split(relativeDir, "/") {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
				return err
			}
			if err := os.Chmod(current, 0o777); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("evidence directory %s is not a real directory", current)
		}
	}
	return nil
}

// Put writes to a temporary sibling and renames it into place, so a crash loses at most the file in flight.
func (d *Dir) Put(relative string, content io.Reader) (Stored, error) {
	if err := checkRelative(relative); err != nil {
		return Stored{}, err
	}
	if err := d.mkdirs(path.Dir(relative)); err != nil {
		return Stored{}, err
	}
	target := filepath.Join(d.root, filepath.FromSlash(relative))
	temp, err := os.CreateTemp(filepath.Dir(target), "."+filepath.Base(target)+".*.partial")
	if err != nil {
		return Stored{}, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.Remove(temp.Name())
		}
	}()
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(temp, hash), content)
	closeErr := temp.Close()
	if copyErr != nil {
		return Stored{}, copyErr
	}
	if closeErr != nil {
		return Stored{}, closeErr
	}
	stored := Stored{SHA256: hex.EncodeToString(hash.Sum(nil)), Bytes: size, Changed: true}
	if existing, err := fileDigest(target, size); err == nil && existing == stored.SHA256 {
		stored.Changed = false
		return stored, nil
	}
	if err := os.Chmod(temp.Name(), 0o644); err != nil {
		return Stored{}, err
	}
	if err := os.Rename(temp.Name(), target); err != nil {
		return Stored{}, err
	}
	published = true
	return stored, nil
}

// fileDigest hashes an existing regular file of exactly size bytes; any other file yields an error.
func fileDigest(name string, size int64) (string, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != size {
		return "", errors.New("different file")
	}
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Append writes one complete line with a single write to an O_APPEND file.
func (d *Dir) Append(record Record) error {
	record.Schema = IndexSchema
	line, err := json.Marshal(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(d.root, IndexFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(line, '\n'))
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

// Records reads the index. A torn last line (a crash during Append) is ignored.
func (d *Dir) Records() ([]Record, error) {
	file, err := os.Open(filepath.Join(d.root, IndexFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var records []Record
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil || record.Schema != IndexSchema {
			continue
		}
		records = append(records, record)
	}
	return records, scanner.Err()
}
