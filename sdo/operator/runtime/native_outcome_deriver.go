package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory"
	"sdo.dev/operator/outcome_derivation"
)

// ProtoOutcomeDeriver bridges the runtime closure to the generated contract.
// The derivation package remains independent of runtime and repository layout.
type ProtoOutcomeDeriver struct{}

func (ProtoOutcomeDeriver) Derive(ctx context.Context, closure IncidentClosure, repairCommit, backend, model string) (DerivedOutcome, error) {
	message, err := toProtoClosure(closure)
	if err != nil {
		return DerivedOutcome{}, err
	}
	result, err := outcome_derivation.Derive(ctx, message, repairCommit, backend, model)
	if err != nil {
		return DerivedOutcome{}, err
	}
	return DerivedOutcome{Record: result.Record, RequiresReflection: result.RequiresReflection}, nil
}

func toProtoClosure(closure IncidentClosure) (*contracts.IncidentClosure, error) {
	data, err := json.Marshal(closure)
	if err != nil {
		return nil, err
	}
	data, err = memory.NormalizeWorkerWire(data)
	if err != nil {
		return nil, fmt.Errorf("logical closure playbooks: %w", err)
	}
	message := &contracts.IncidentClosure{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, message); err != nil {
		return nil, fmt.Errorf("closure violates protobuf contract: %w", err)
	}
	return message, nil
}
