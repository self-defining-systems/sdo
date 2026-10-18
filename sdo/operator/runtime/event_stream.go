package runtime

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// durableEventStream is a bounded, append-only JSONL event log with
// content-addressed deduplication. It is the shared durability mechanism behind
// both the detector firing stream and the incident lifecycle stream: every line
// carries an "event_id" field, a record whose id is already present (in the live
// file or its one rotated predecessor) is dropped, and ids derive from the
// controller's durable state, so a controller that crashes between emitting an
// event and persisting its state replays the identical id and the stream drops
// the duplicate.
//
// The live file is bounded at maxBytes and rotated once to "<path>.1", so total
// size never exceeds twice maxBytes. A trailing line torn by an interrupted
// write is repaired on the next append. A stream left over from an earlier
// controller lifecycle is archived to "<path>.prev" by StartFresh, because ids
// derived from fresh state would otherwise be suppressed as duplicates of the
// stale stream.
type durableEventStream struct {
	path     string
	maxBytes int64

	mu   sync.Mutex
	seen map[string]struct{}
	size int64
	// dirty is set when the file ends in a partial line from an interrupted write.
	dirty bool
}

func openDurableEventStream(path string, maxBytes int64) (*durableEventStream, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("event stream path is required")
	}
	if maxBytes <= 0 {
		return nil, fmt.Errorf("event stream size bound must be positive")
	}
	stream := &durableEventStream{path: path, maxBytes: maxBytes, seen: make(map[string]struct{})}
	if _, err := stream.scan(path+".1", false); err != nil {
		return nil, err
	}
	if _, err := stream.scan(path, true); err != nil {
		return nil, err
	}
	return stream, nil
}

// scan loads event ids from a stream file. current marks the live file, whose
// size and trailing-newline state the stream tracks.
func (s *durableEventStream) scan(path string, current bool) (int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("open event stream: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 64*1024)
	var total int64
	endedWithNewline := true
	for {
		line, readErr := reader.ReadBytes('\n')
		total += int64(len(line))
		if len(line) > 0 {
			endedWithNewline = line[len(line)-1] == '\n'
			var record struct {
				EventID string `json:"event_id"`
			}
			if json.Unmarshal(line, &record) == nil && record.EventID != "" {
				s.seen[record.EventID] = struct{}{}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read event stream: %w", readErr)
		}
	}
	if current {
		s.size = total
		s.dirty = total > 0 && !endedWithNewline
	}
	return total, nil
}

// append writes one JSON line (without a trailing newline) keyed by eventID. A
// record whose id is already present is dropped so replays are idempotent.
func (s *durableEventStream) append(eventID string, line []byte) error {
	if eventID == "" {
		return fmt.Errorf("event has no event id")
	}
	payload := make([]byte, 0, len(line)+1)
	payload = append(payload, line...)
	payload = append(payload, '\n')
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, duplicate := s.seen[eventID]; duplicate {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create event stream directory: %w", err)
	}
	if s.size > 0 && s.size+int64(len(payload)) > s.maxBytes {
		if err := s.rotate(); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open event stream: %w", err)
	}
	defer file.Close()
	if s.dirty {
		// Terminate a line torn by a crash so the new record parses on its own.
		payload = append([]byte{'\n'}, payload...)
	}
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync event stream: %w", err)
	}
	s.dirty = false
	s.size += int64(len(payload))
	s.seen[eventID] = struct{}{}
	return nil
}

// rotate moves the live file to "<path>.1", discarding the previous rotation,
// and keeps only the ids of the rotated file for deduplication.
func (s *durableEventStream) rotate() error {
	if err := os.Rename(s.path, s.path+".1"); err != nil {
		return fmt.Errorf("rotate event stream: %w", err)
	}
	s.seen = make(map[string]struct{})
	if _, err := s.scan(s.path+".1", false); err != nil {
		return err
	}
	s.size = 0
	s.dirty = false
	return nil
}

// StartFresh archives a stream left by an earlier controller lifecycle. Event
// ids derive from controller state, so a stream that outlived its state would
// otherwise suppress the new lifecycle's records as duplicates.
func (s *durableEventStream) StartFresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.size == 0 {
		return nil
	}
	if err := os.Rename(s.path, s.path+".prev"); err != nil {
		return fmt.Errorf("archive event stream: %w", err)
	}
	s.seen = make(map[string]struct{})
	s.size = 0
	s.dirty = false
	_, err := s.scan(s.path+".1", false)
	return err
}

// contentAddressedID hashes its parts into a short, stable, content-addressed
// event id. The same parts always yield the same id, which is what lets a
// crashed controller re-emit a transition and have the stream drop the
// duplicate. It is shared by the firing and lifecycle streams.
func contentAddressedID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:10])
}
