package runtime

import (
	"encoding/json"
	"google.golang.org/protobuf/encoding/protojson"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

// MenuRoundExecution describes execution without retaining resolved values or
// rollback preimages in the tool report. Application IDs use ordinary rollback.
type MenuRoundExecution struct {
	ActionResults []PlanActionReport `json:"action_results,omitempty"`
	RoundIndex    uint32             `json:"round_index"`
	ApplicationID string             `json:"application_id,omitempty"`
	Plan          *contracts.Plan    `json:"-"`
	ChangesRemain bool               `json:"changes_remain"`
	RolledBack    bool               `json:"rolled_back"`
	Pending       bool               `json:"pending"`
	Error         string             `json:"error,omitempty"`
}

func (r MenuRoundExecution) MarshalJSON() ([]byte, error) {
	type plain MenuRoundExecution
	plan, err := toolContractJSON(r.Plan)
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		plain
		Plan json.RawMessage `json:"plan,omitempty"`
	}{plain(r), plan})
}

func menuExecutionJSON(report json.RawMessage, execution []MenuRoundExecution) (json.RawMessage, error) {
	if len(report) == 0 || len(execution) == 0 {
		return report, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(report, &fields); err != nil {
		return nil, err
	}
	data, err := json.Marshal(execution)
	if err != nil {
		return nil, err
	}
	fields["execution"] = data
	return json.Marshal(fields)
}

func (r *MenuRoundExecution) UnmarshalJSON(data []byte) error {
	type plain MenuRoundExecution
	envelope := struct {
		*plain
		Plan json.RawMessage `json:"plan"`
	}{plain: (*plain)(r)}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	if len(envelope.Plan) > 0 && string(envelope.Plan) != "null" {
		r.Plan = &contracts.Plan{}
		if err := protojson.Unmarshal(envelope.Plan, r.Plan); err != nil {
			return err
		}
	}
	return nil
}
