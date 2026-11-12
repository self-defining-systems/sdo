package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/layout"
)

// SetSourceRepair records what became of the source a responder committed. It
// is an annotation like AppendSDKGaps: it replaces only the source_repair field
// of an existing outcome and keeps every other byte of its evidence. Setting
// the value already stored is a no-op; a different value replaces it (the
// publication moves from one status to the next).
func (s *JSONStore) SetSourceRepair(ctx context.Context, id string, repair *contracts.SourceRepair) (Outcome, error) {
	if repair == nil {
		return Outcome{}, fmt.Errorf("source repair is required")
	}
	previous, err := s.Get(ctx, id)
	if err != nil {
		return Outcome{}, err
	}
	if proto.Equal(previous.Record.GetSourceRepair(), repair) {
		return previous, nil
	}
	next := proto.Clone(previous.Record).(*contracts.OutcomeRecord)
	next.SourceRepair = proto.Clone(repair).(*contracts.SourceRepair)
	encoded, err := wire.MarshalFull(next)
	if err != nil {
		return Outcome{}, err
	}
	var document, updated map[string]json.RawMessage
	if err = json.Unmarshal(previous.document, &document); err != nil {
		return Outcome{}, err
	}
	if err = json.Unmarshal(encoded, &updated); err != nil {
		return Outcome{}, err
	}
	document["source_repair"] = updated["source_repair"]
	raw, err := json.Marshal(document)
	if err != nil {
		return Outcome{}, err
	}
	outcome, err := parse(raw)
	if err != nil {
		return Outcome{}, err
	}
	if err = ctx.Err(); err != nil {
		return Outcome{}, err
	}
	directory := filepath.Join(s.root, layout.OutcomesDir)
	if err = safeDirectory(directory); err != nil {
		return Outcome{}, err
	}
	file, err := os.CreateTemp(directory, ".source-repair-*")
	if err != nil {
		return Outcome{}, err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(outcome.document); err != nil {
		file.Close()
		return Outcome{}, err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return Outcome{}, err
	}
	if err = file.Close(); err != nil {
		return Outcome{}, err
	}
	if err = os.Chmod(file.Name(), 0644); err != nil {
		return Outcome{}, err
	}
	if err = os.Rename(file.Name(), filepath.Join(s.root, layout.OutcomePath(id))); err != nil {
		return Outcome{}, err
	}
	return outcome, nil
}
