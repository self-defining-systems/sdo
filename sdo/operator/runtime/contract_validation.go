package runtime

import (
	"fmt"

	"sdo.dev/operator/contracts/wire"
)

// validateIncidentRequest runs the contract's protovalidate rules over the
// request as the responder would receive it.
func validateIncidentRequest(request IncidentRequest) error {
	typed, err := incidentRequestContract(request)
	if err != nil {
		return err
	}
	return wire.Validate(typed)
}

// validateIncidentResultFor runs the contract's rules over result and checks
// the bindings no single message can state: that it answers this request.
func validateIncidentResultFor(result IncidentResult, request IncidentRequest) error {
	typed, err := incidentResultContract(result)
	if err != nil {
		return err
	}
	if err := wire.Validate(typed); err != nil {
		return err
	}
	if result.IncidentID != request.IncidentID {
		return fmt.Errorf("incident id %q does not match request %q", result.IncidentID, request.IncidentID)
	}
	if pull := result.LateFindingsPull; pull != nil && pull.IncidentID != request.IncidentID {
		return fmt.Errorf("late findings pull names incident %q, not %q", pull.IncidentID, request.IncidentID)
	}
	return nil
}
