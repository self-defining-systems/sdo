// This package must fail to build: action values cannot be raw Go literals.
package literal

import "sdo.dev/operator/sdk/program"

var _ = program.SetField{Value: 8080}
