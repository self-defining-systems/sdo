package memory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func TestResponderUsageRejectsUnboundOrInvalidCheckpoints(t *testing.T) {
	for _, field := range []string{"version", "incident_id", "task_id", "phase", "turn_id", "complete", "usage"} {
		t.Run(field, func(t *testing.T) {
			root := t.TempDir()
			sub, err := IncidentSessionSubPath("tenant", "incident")
			if err != nil {
				t.Fatal(err)
			}
			relative, err := filepath.Rel("tenant", sub)
			if err != nil {
				t.Fatal(err)
			}
			directory := filepath.Join(root, relative, "usage")
			if err := os.MkdirAll(directory, 0700); err != nil {
				t.Fatal(err)
			}
			document := map[string]any{"version": 1, "incident_id": "incident", "task_id": "incident", "phase": "responder", "turn_id": "turn", "complete": false, "usage": map[string]int{"input_tokens": 100, "output_tokens": 10}}
			switch field {
			case "version":
				document[field] = 2
			case "complete":
				delete(document, field)
			case "usage":
				document[field] = map[string]int{"input_tokens": -1}
			default:
				document[field] = "other"
			}
			data, err := json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "turn.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if usage, err := ResponderUsage(root, "tenant", "incident"); err == nil || usage != nil {
				t.Fatalf("invalid checkpoint accepted as accounting: %v %v", usage, err)
			}
		})
	}
}

func TestResponderUsageAbsenceIsUnknownAndTempsAreIgnored(t *testing.T) {
	root := t.TempDir()
	usage, err := ResponderUsage(root, "", "incident")
	if err != nil || usage != nil {
		t.Fatalf("absence became zero: %v %v", usage, err)
	}
	sub, _ := IncidentSessionSubPath("", "incident")
	directory := filepath.Join(root, sub, "usage")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, ".usage-tmp"), []byte("incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	usage, err = ResponderUsage(root, "", "incident")
	if err != nil || usage != nil {
		t.Fatalf("staging file became observed usage: %v %v", usage, err)
	}
}

func TestAddUsagePreservesPartialAndUnknownCounters(t *testing.T) {
	first := &contracts.UsageMetrics{InputTokens: 100, OutputTokens: 10, Partial: true, ModelRequests: proto.Int64(2), TotalCostUsd: proto.Float64(.1)}
	second := &contracts.UsageMetrics{InputTokens: 200, OutputTokens: 20, ModelRequests: proto.Int64(3)}
	total := AddUsage(first, second)
	if total.InputTokens != 300 || total.OutputTokens != 30 || !total.Partial || total.GetModelRequests() != 5 || total.TotalCostUsd != nil {
		t.Fatalf("invented or lost accounting: %v", total)
	}
	if first.InputTokens != 100 || second.InputTokens != 200 || second.Partial {
		t.Fatal("sum mutated source snapshots")
	}
}
