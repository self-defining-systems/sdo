package operatorcli

// Seams for the cross-module integration tests in sdo/operator/integration.
// That test-only module cannot see this package's unexported names, and it is
// where the tests that need both this module and sdo/operator/runtime live
// (the root module must not require runtime: runtime requires the root).
// Production code does not use these names; each is the production value.

// AdvanceMaintenance is the advance's maintenance pause.
type AdvanceMaintenance = advanceMaintenance

// LifecycleRegistry builds the operate Job's worker registry.
var LifecycleRegistry = lifecycleRegistry

// LifecycleWorkers builds the operate Job's agent transport.
var LifecycleWorkers = lifecycleWorkers

// OnboardingBroker builds the operate Job's broker.
var OnboardingBroker = onboardingBroker

// TelemetryRegistry is the operator's telemetry adapter registry.
var TelemetryRegistry = telemetryRegistry

const (
	// MemoryTelemetryPolicyEnv names the telemetry policy a judge Job carries.
	MemoryTelemetryPolicyEnv = memoryTelemetryPolicyEnv
	// MemoryOperatorEnv names the operator sources root `memory check` reads.
	MemoryOperatorEnv = memoryOperatorEnv
	// ExitClean and ExitDiagnostics are `memory check`'s exit codes.
	ExitClean       = exitClean
	ExitDiagnostics = exitDiagnostics
)
