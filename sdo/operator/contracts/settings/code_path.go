package settings

import (
	"fmt"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
)

// CodePathSelection is the typed code path the agent section selects: nil for the plain go code path, which every
// seam treats as its default, otherwise the named path with its overrides.
func (a Agent) CodePathSelection() (*contracts.CodePathConfig, error) {
	if (a.CodePath == "" || a.CodePath == "go") && a.CodePathOverrides == "" {
		return nil, nil
	}
	name := a.CodePath
	if name == "" {
		name = "go"
	}
	if name != "go" && name != "shell" {
		return nil, fmt.Errorf("unknown code path %q", name)
	}
	selection := &contracts.CodePathConfig{Name: name}
	if a.CodePathOverrides != "" {
		selection.Overrides = &contracts.CodePathOverrides{}
		if err := wire.Unmarshal([]byte(a.CodePathOverrides), selection.Overrides); err != nil {
			return nil, err
		}
	}
	return selection, nil
}
