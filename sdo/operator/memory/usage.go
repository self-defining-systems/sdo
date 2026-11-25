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
//
// A checkpoint whose counters fail decoding or validation is a usage shape problem, not an incident failure:
// it is left out of the total, the total is marked partial, and the problem is returned beside it. Identity
// problems (wrong incident, task or turn) still fail.
func ResponderUsage(repositoryMount, pvcSubPath, incidentID string) (*contracts.UsageMetrics, []error, error) {
	return PhaseUsage(repositoryMount, pvcSubPath, incidentID, "respond")
}

// PhaseUsage reads the latest checkpoint for each turn in the requested phase.
func PhaseUsage(repositoryMount, pvcSubPath, incidentID, phase string) (*contracts.UsageMetrics, []error, error) {
	if repositoryMount == "" {
		return nil, nil, nil
	}
	sub, err := IncidentSessionSubPath(pvcSubPath, incidentID)
	if err != nil {
		return nil, nil, err
	}
	base := pvcSubPath
	if base == "" {
		base = "."
	}
	relative, err := filepath.Rel(base, sub)
	if err != nil {
		return nil, nil, err
	}
	directory := filepath.Join(repositoryMount, relative, "usage")
	var validTask func(string) bool
	switch phase {
	case "respond", "investigate":
		validTask = func(task string) bool {
			suffix, continuation := strings.CutPrefix(task, incidentID+"-continue-")
			attempt, parseErr := strconv.Atoi(suffix)
			return task == incidentID || (continuation && parseErr == nil && attempt > 0 && strconv.Itoa(attempt) == suffix)
		}
	case "reflect", "source_repair":
		directory = filepath.Join(directory, phase)
		prefix := map[string]string{"reflect": "reflection-", "source_repair": "closing-cleanup-"}[phase]
		validTask = func(task string) bool { return strings.HasPrefix(task, prefix) }
	default:
		return nil, nil, fmt.Errorf("unsupported incident usage phase %q", phase)
	}
	return readUsage(directory, incidentID, phase, validTask)
}

// SetupUsage reads one setup task's checkpoints from its evidence directory. A setup phase (deploy, design)
// belongs to no incident: its task is its whole identity and its checkpoints carry no incident ID.
func SetupUsage(taskDirectory, taskID, phase string) (*contracts.UsageMetrics, []error, error) {
	switch {
	case phase != "deploy" && phase != "design":
		return nil, nil, fmt.Errorf("unsupported setup usage phase %q", phase)
	case taskID == "":
		return nil, nil, errors.New("setup usage requires a task")
	}
	return readUsage(filepath.Join(taskDirectory, "usage", phase), "", phase, func(task string) bool { return task == taskID })
}

func readUsage(directory, incidentID, phase string, validTask func(string) bool) (*contracts.UsageMetrics, []error, error) {
	entries, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var total *contracts.UsageMetrics
	var problems []error
	partial := false
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			return nil, nil, err
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
			return nil, nil, fmt.Errorf("decode usage checkpoint %s: %w", entry.Name(), err)
		}
		validPhase := checkpoint.Phase == phase ||
			(phase == "respond" || phase == "investigate") && checkpoint.Phase == "responder"
		if checkpoint.Version != 1 || checkpoint.IncidentID != incidentID || !validTask(checkpoint.TaskID) || !validPhase || checkpoint.TurnID+".json" != entry.Name() || checkpoint.Complete == nil || len(checkpoint.Usage) == 0 || string(checkpoint.Usage) == "null" {
			return nil, nil, fmt.Errorf("usage checkpoint %s has invalid incident/task/phase/turn identity", entry.Name())
		}
		if string(checkpoint.Usage) == "{}" && !*checkpoint.Complete {
			partial = true
			continue
		}
		usage := new(contracts.UsageMetrics)
		if err := protojson.Unmarshal(checkpoint.Usage, usage); err != nil {
			problems = append(problems, fmt.Errorf("decode usage counters %s: %w", entry.Name(), err))
			partial = true
			continue
		}
		if err := wire.Validate(usage); err != nil {
			problems = append(problems, fmt.Errorf("validate usage counters %s: %w", entry.Name(), err))
			partial = true
			continue
		}
		usage.Partial = !*checkpoint.Complete
		total = AddUsage(total, usage)
	}
	if partial {
		if total == nil {
			total = new(contracts.UsageMetrics)
		}
		total.Partial = true
	}
	return total, problems, nil
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
