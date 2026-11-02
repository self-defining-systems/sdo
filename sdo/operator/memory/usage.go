package memory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// ResponderUsage reads worker-normalized counters, never provider transcripts.
// Atomic replacement gives one latest snapshot per process turn, including
// turns of the same task resumed after worker restart. nil means unobserved.
func ResponderUsage(repositoryMount, pvcSubPath, incidentID string) (*contracts.UsageMetrics, error) {
	if repositoryMount == "" {
		return nil, nil
	}
	sub, err := IncidentSessionSubPath(pvcSubPath, incidentID)
	if err != nil {
		return nil, err
	}
	base := pvcSubPath
	if base == "" {
		base = "."
	}
	relative, err := filepath.Rel(base, sub)
	if err != nil {
		return nil, err
	}
	directory := filepath.Join(repositoryMount, relative, "usage")
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var total *contracts.UsageMetrics
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, err
		}
		var checkpoint struct {
			Version    int             `json:"version"`
			IncidentID string          `json:"incident_id"`
			TaskID     string          `json:"task_id"`
			TurnID     string          `json:"turn_id"`
			Phase      string          `json:"phase"`
			Complete   *bool           `json:"complete"`
			Usage      json.RawMessage `json:"usage"`
		}
		if err := json.Unmarshal(data, &checkpoint); err != nil {
			return nil, fmt.Errorf("decode usage checkpoint %s: %w", entry.Name(), err)
		}
		suffix, continuation := strings.CutPrefix(checkpoint.TaskID, incidentID+"-continue-")
		attempt, parseErr := strconv.Atoi(suffix)
		validTask := checkpoint.TaskID == incidentID || (continuation && parseErr == nil && attempt > 0 && strconv.Itoa(attempt) == suffix)
		if checkpoint.Version != 1 || checkpoint.IncidentID != incidentID || !validTask || checkpoint.Phase != "responder" || checkpoint.TurnID+".json" != entry.Name() || checkpoint.Complete == nil || len(checkpoint.Usage) == 0 || string(checkpoint.Usage) == "null" {
			return nil, fmt.Errorf("usage checkpoint %s has invalid incident/task/phase/turn identity", entry.Name())
		}
		usage := new(contracts.UsageMetrics)
		if err := protojson.Unmarshal(checkpoint.Usage, usage); err != nil {
			return nil, fmt.Errorf("decode usage counters %s: %w", entry.Name(), err)
		}
		if err := wire.Validate(usage); err != nil {
			return nil, fmt.Errorf("validate usage counters %s: %w", entry.Name(), err)
		}
		usage.Partial = !*checkpoint.Complete
		total = AddUsage(total, usage)
	}
	return total, nil
}

// AddUsage preserves unknown optional counters and partial accounting.
func AddUsage(prior, current *contracts.UsageMetrics) *contracts.UsageMetrics {
	if prior == nil {
		return proto.Clone(current).(*contracts.UsageMetrics)
	}
	result := proto.Clone(current).(*contracts.UsageMetrics)
	target, source := result.ProtoReflect(), prior.ProtoReflect()
	fields := source.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if field.HasPresence() && (!source.Has(field) || !target.Has(field)) {
			target.Clear(field)
			continue
		}
		switch field.Kind() {
		case protoreflect.Int64Kind:
			target.Set(field, protoreflect.ValueOfInt64(target.Get(field).Int()+source.Get(field).Int()))
		case protoreflect.DoubleKind:
			target.Set(field, protoreflect.ValueOfFloat64(target.Get(field).Float()+source.Get(field).Float()))
		case protoreflect.BoolKind:
			target.Set(field, protoreflect.ValueOfBool(target.Get(field).Bool() || source.Get(field).Bool()))
		}
	}
	return result
}
