package runtime

import (
	"bytes"
	"encoding/json"
	"errors"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// The outer tool envelope retains the established JSON spelling; generated
// inner contracts use strict ProtoJSON (including string uint64 epochs).
func (r *ToolRequest) UnmarshalJSON(data []byte) error {
	type plain ToolRequest
	envelope := struct {
		*plain
		Selection   json.RawMessage `json:"selection"`
		ReportQuery json.RawMessage `json:"report_query"`
		Checkpoint  json.RawMessage `json:"checkpoint"`
	}{plain: (*plain)(r)}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return errors.New("invalid tool request envelope")
	}
	for _, field := range []struct {
		raw     json.RawMessage
		message proto.Message
	}{
		{envelope.Selection, new(contracts.MitigationSelection)},
		{envelope.ReportQuery, new(contracts.MitigationReportQuery)},
		{envelope.Checkpoint, new(contracts.MitigationSessionCheckpoint)},
	} {
		if len(field.raw) == 0 {
			continue
		}
		if string(field.raw) == "null" || protojson.Unmarshal(field.raw, field.message) != nil {
			return errors.New("invalid generated tool contract")
		}
		switch value := field.message.(type) {
		case *contracts.MitigationSelection:
			r.Selection = value
		case *contracts.MitigationReportQuery:
			r.ReportQuery = value
		case *contracts.MitigationSessionCheckpoint:
			r.Checkpoint = value
		}
	}
	return nil
}
func toolContractJSON(message proto.Message) (json.RawMessage, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, nil
	}
	return (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
}
func (r ToolRequest) MarshalJSON() ([]byte, error) {
	type plain ToolRequest
	selection, err := toolContractJSON(r.Selection)
	if err != nil {
		return nil, err
	}
	query, err := toolContractJSON(r.ReportQuery)
	if err != nil {
		return nil, err
	}
	checkpoint, err := toolContractJSON(r.Checkpoint)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		plain
		Selection   json.RawMessage `json:"selection,omitempty"`
		ReportQuery json.RawMessage `json:"report_query,omitempty"`
		Checkpoint  json.RawMessage `json:"checkpoint,omitempty"`
	}{plain(r), selection, query, checkpoint})
}
func (r ToolResponse) MarshalJSON() ([]byte, error) {
	type plain ToolResponse
	menu, err := toolContractJSON(r.Menu)
	if err != nil {
		return nil, err
	}
	report, err := toolContractJSON(r.Report)
	if err != nil {
		return nil, err
	}
	session, err := toolContractJSON(r.Session)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		plain
		Menu    json.RawMessage `json:"menu,omitempty"`
		Report  json.RawMessage `json:"report,omitempty"`
		Session json.RawMessage `json:"session,omitempty"`
	}{plain(r), menu, report, session})
}

// Defining an outer marshaler prevents the embedded ToolResponse marshaler
// from swallowing truncation counters used by the revision-bound trust reader.
func (r truncatedToolResponse) MarshalJSON() ([]byte, error) {
	encoded, err := json.Marshal(r.ToolResponse)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	for key, value := range map[string]any{"truncated": r.Truncated, "findings_count": r.FindingsCount, "program_runs_count": r.ProgramRunsCount, "detector_states_count": r.DetectorStatesCount, "patches_count": r.PatchesCount} {
		fields[key], err = json.Marshal(value)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(fields)
}
