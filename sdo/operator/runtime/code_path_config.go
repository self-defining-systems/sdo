package runtime

import (
	"encoding/json"
	"fmt"
	"strings"

	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func codePathConfigFromContract(config *contracts.CodePathConfig) CodePathConfig {
	result := CodePathConfig{Name: config.GetName()}
	if o := config.GetOverrides(); o != nil {
		result.Overrides = CodePathOverrides{DetectorSource: o.GetDetectorSource(), Coverage: o.GetCoverage(), MemoryValidation: o.GetMemoryValidation(), Activation: o.GetActivation(), AgentGuidance: o.GetAgentGuidance()}
	}
	return result
}
func codePathFromContract(config *contracts.CodePathConfig) (*CodePath, error) {
	return NewCodePathRegistry().Resolve(codePathConfigFromContract(config))
}
func codePathFromFlags(name, overrides string) (CodePathConfig, error) {
	config := CodePathConfig{Name: name}
	if overrides != "" {
		decoder := json.NewDecoder(strings.NewReader(overrides))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&config.Overrides); err != nil {
			return config, &CodePathConfigError{Seam: "overrides", Reason: err.Error()}
		}
	}
	_, err := NewCodePathRegistry().Resolve(config)
	return config, err
}
func codePathOverrideArgs(config *contracts.CodePathConfig) []string {
	if config == nil {
		return nil
	}
	overrides := codePathConfigFromContract(config).Overrides
	data, err := json.Marshal(overrides)
	if err != nil {
		panic(fmt.Sprintf("encode typed code-path overrides: %v", err))
	}
	if string(data) == "{}" {
		return nil
	}
	return []string{"--code-path-overrides", string(data)}
}

func codePathCustomized(config CodePathConfig) bool {
	o := config.Overrides
	return config.Name != "" && config.Name != "go" || o.DetectorSource != "" || o.Coverage != "" || o.MemoryValidation != "" || o.Activation != "" || o.AgentGuidance != ""
}
func codePathContract(config CodePathConfig) *contracts.CodePathConfig {
	if !codePathCustomized(config) {
		return nil
	}
	o := config.Overrides
	return &contracts.CodePathConfig{Name: config.Name, Overrides: &contracts.CodePathOverrides{DetectorSource: o.DetectorSource, Coverage: o.Coverage, MemoryValidation: o.MemoryValidation, Activation: o.Activation, AgentGuidance: o.AgentGuidance}}
}
