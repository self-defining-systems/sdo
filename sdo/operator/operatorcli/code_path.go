package operatorcli

import (
	"sdo.dev/operator/broker"
	contracts "sdo.dev/operator/contracts/gen/sdodev/contracts/v1alpha1"
	"sdo.dev/operator/memory/validate"
)

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

func selectMemoryValidator(config *contracts.CodePathConfig, goValidator broker.Validator) broker.Validator {
	if memoryCodePath(config) == "shell" {
		return validate.ShellHygieneValidator
	}
	return goValidator
}
