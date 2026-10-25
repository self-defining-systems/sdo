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
		SelectionSet json.RawMessage `json:"selection_set,omitempty"`
		Selection    json.RawMessage `json:"selection"`
		ReportQuery  json.RawMessage `json:"report_query"`
		Checkpoint   json.RawMessage `json:"checkpoint"`
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
		{envelope.SelectionSet, new(contracts.MitigationSelectionSet)},
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
		case *contracts.MitigationSelectionSet:
			r.SelectionSet = value
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
	selectionSet, err := toolContractJSON(r.SelectionSet)
	if err != nil {
		return nil, err
	}
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
		SelectionSet json.RawMessage `json:"selection_set,omitempty"`
		Selection    json.RawMessage `json:"selection,omitempty"`
		ReportQuery  json.RawMessage `json:"report_query,omitempty"`
		Checkpoint   json.RawMessage `json:"checkpoint,omitempty"`
	}{plain(r), selectionSet, selection, query, checkpoint})
}
func (r ToolResponse) MarshalJSON() ([]byte, error) {
	type plain ToolResponse
	menu, err := toolContractJSON(r.Menu)
	if err != nil {
		return nil, err
	}
	menu, err = menuParameterJSON(menu, r.MenuParameters)
	if err != nil {
		return nil, err
	}
	report, err := toolContractJSON(r.Report)
	if err != nil {
		return nil, err
	}
	report, err = menuExecutionJSON(report, r.MenuExecution)
	if err != nil {
		return nil, err
	}
	optionReports := make([]json.RawMessage, 0, len(r.OptionReports))
	for _, item := range r.OptionReports {
		encoded, err := toolContractJSON(item)
		if err != nil {
			return nil, err
		}
		optionReports = append(optionReports, encoded)
	}
	session, err := toolContractJSON(r.Session)
	if err != nil {
		return nil, err
	}
	receipt, err := toolContractJSON(r.Receipt)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		plain
		OptionReports []json.RawMessage `json:"option_reports,omitempty"`
		Receipt       json.RawMessage   `json:"receipt,omitempty"`
		Menu          json.RawMessage   `json:"menu,omitempty"`
		Report        json.RawMessage   `json:"report,omitempty"`
		Session       json.RawMessage   `json:"session,omitempty"`
	}{plain(r), optionReports, receipt, menu, report, session})
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
	if r.VerificationSummary != nil {
		fields["verification_summary"], err = json.Marshal(r.VerificationSummary)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(fields)
}

func menuParameterJSON(menu json.RawMessage, parameters map[string][]MenuParameter) (json.RawMessage, error) {
	if len(menu) == 0 || parameters == nil {
		return menu, nil
	}
	var envelope struct {
		Options []map[string]json.RawMessage `json:"options"`
	}
	if err := json.Unmarshal(menu, &envelope); err != nil {
		return nil, err
	}
	for _, option := range envelope.Options {
		var id string
		if err := json.Unmarshal(option["option_id"], &id); err != nil {
			return nil, err
		}
		schema, err := json.Marshal(parameters[id])
		if err != nil {
			return nil, err
		}
		option["parameters"] = schema
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(menu, &fields); err != nil {
		return nil, err
	}
	options, err := json.Marshal(envelope.Options)
	if err != nil {
		return nil, err
	}
	fields["options"] = options
	return json.Marshal(fields)
}

func (r *ToolResponse) UnmarshalJSON(data []byte) error {
	type plain ToolResponse
	envelope := struct {
		*plain
		Receipt json.RawMessage `json:"receipt"`
	}{plain: (*plain)(r)}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if len(envelope.Receipt) > 0 && string(envelope.Receipt) != "null" {
		r.Receipt = new(contracts.RepairActionReceipt)
		return protojson.Unmarshal(envelope.Receipt, r.Receipt)
	}
	return nil
}
