package operatorcli

import (
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"
	c "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/outcome_derivation"
)

// RunControllerObservation is a transport-only read projection. It opens no
// Kubernetes client and returns a typed failure with exit status zero.
func RunControllerObservation(input io.Reader, output io.Writer) error {
	const limit = 2 << 20
	data, err := io.ReadAll(io.LimitReader(input, limit+1))
	if err != nil {
		return err
	}
	var result *c.ControllerObservation
	if len(data) > limit {
		result = &c.ControllerObservation{Failure: &c.ControllerObservationFailure{Code: c.ControllerObservationFailure_CODE_INVALID_SNAPSHOT, Message: "controller observation exceeds 2 MiB"}}
	} else {
		result = outcome_derivation.ObserveController(data)
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(result)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, string(encoded))
	return err
}
