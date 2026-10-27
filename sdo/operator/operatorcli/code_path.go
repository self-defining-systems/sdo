package operatorcli

import (
	"fmt"
	"strings"

	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/contracts/wire"
	"sdo.dev/operator/memory/validate"
)

func parseCodePathSelection(name, overrides string) (*contracts.CodePathConfig, error) {
	if name == "" && overrides == "" {
		return nil, nil
	}
	if name == "" {
		name = "go"
	}
	if name != "go" && name != "shell" {
		return nil, fmt.Errorf("unknown code path %q", name)
	}
	result := &contracts.CodePathConfig{Name: name}
	if overrides != "" {
		result.Overrides = &contracts.CodePathOverrides{}
		if err := wire.Unmarshal([]byte(overrides), result.Overrides); err != nil {
			return nil, err
		}
	}
	return result, nil
}
func addResponseStageOverride(config **contracts.CodePathConfig, value []string) {
	if value == nil {
		return
	}
	if *config == nil {
		*config = &contracts.CodePathConfig{Name: "go"}
	}
	if (*config).Overrides == nil {
		(*config).Overrides = &contracts.CodePathOverrides{}
	}
	(*config).Overrides.ResponseStages = append([]string{}, value...)
}
func memoryCodePath(config *contracts.CodePathConfig) string {
	mode := config.GetOverrides().GetMemoryValidation()
	if mode == "go" {
		return "go"
	}
	if mode != "" || config.GetName() == "shell" {
		return "shell"
	}
	return "go"
}
func codePathWorkerEnvironment(config *contracts.CodePathConfig, environment map[string]string) map[string]string {
	if config == nil {
		return environment
	}
	if environment == nil {
		environment = map[string]string{}
	}
	name := config.GetName()
	if name == "" {
		name = "go"
	}
	guidance := config.GetOverrides().GetAgentGuidance()
	if guidance == "" {
		guidance = name
	}
	environment["SDO_CODE_PATH"] = name
	environment["SDO_AGENT_GUIDANCE"] = guidance
	environment["SDO_MEMORY_VALIDATION"] = memoryCodePath(config)
	return environment
}
func stageOverride(value string) []string { return strings.Split(value, ",") }

func selectMemoryValidator(config *contracts.CodePathConfig, goValidator broker.Validator) broker.Validator {
	if memoryCodePath(config) == "shell" {
		return validate.ShellHygieneValidator
	}
	return goValidator
}
