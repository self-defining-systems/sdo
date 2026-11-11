package program

import (
	"fmt"
	"strings"

	"sdo.dev/operator/sdk"
)

// RestoreSecretFromSource restores declared Secret data. It writes the literal
// data the accepted source declares (data/stringData in a rendered manifest),
// creating the Secret when it is absent; undeclared keys are removed. Go resolves and
// writes the values: no value reaches a program, agent or record, and the
// receipt names keys and short value digests only. Refused when source does
// not declare the Secret with literal data (generated or external secret
// material). DiffAgainstSource on a Secret names keys that differ.
type RestoreSecretFromSource struct {
	Ref    sdk.ObjectRef
	Reason string
}

func (RestoreSecretFromSource) action() {}

// Validate requires a named, namespaced v1 Secret and a reason.
func (a RestoreSecretFromSource) Validate() error {
	if a.Ref.APIVersion != "v1" || a.Ref.Kind != "Secret" || a.Ref.Namespace == "" || a.Ref.Name == "" {
		return fmt.Errorf("RestoreSecretFromSource requires a named, namespaced v1 Secret")
	}
	if strings.TrimSpace(a.Reason) == "" {
		return fmt.Errorf("RestoreSecretFromSource requires a reason")
	}
	return nil
}
