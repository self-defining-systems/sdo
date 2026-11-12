package runtime

import (
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
)

func codePathConfigFromContract(config *contracts.CodePathConfig) CodePathConfig {
	result := CodePathConfig{Name: config.GetName()}
	if o := config.GetOverrides(); o != nil {
		result.Overrides = CodePathOverrides{DetectorSource: o.GetDetectorSource(), Coverage: o.GetCoverage(), MemoryValidation: o.GetMemoryValidation(), Activation: o.GetActivation(), AgentGuidance: o.GetAgentGuidance()}
		if len(o.GetResponseStages()) > 0 {
			result.Overrides.ResponseStages = append([]string(nil), o.GetResponseStages()...)
		}
	}
	return result
}
func codePathFromContract(config *contracts.CodePathConfig) (*CodePath, error) {
	return NewCodePathRegistry().Resolve(codePathConfigFromContract(config))
}
func codePathCustomized(config CodePathConfig) bool {
	o := config.Overrides
	return config.Name != "" && config.Name != "go" || o.DetectorSource != "" || o.Coverage != "" || o.MemoryValidation != "" || o.Activation != "" || o.AgentGuidance != "" || o.ResponseStages != nil
}
func codePathContract(config CodePathConfig) *contracts.CodePathConfig {
	if !codePathCustomized(config) {
		return nil
	}
	o := config.Overrides
	return &contracts.CodePathConfig{Name: config.Name, Overrides: &contracts.CodePathOverrides{DetectorSource: o.DetectorSource, Coverage: o.Coverage, ResponseStages: o.ResponseStages, MemoryValidation: o.MemoryValidation, Activation: o.Activation, AgentGuidance: o.AgentGuidance}}
}
