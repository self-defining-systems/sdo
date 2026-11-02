#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Keep the entire driver within one shared-host worker budget, including the
# concurrent Python shard. Default eight preserves the normal CI allocation.
hermetic_jobs="${SDO_HERMETIC_JOBS:-8}"
if [[ "$hermetic_jobs" != 4 && "$hermetic_jobs" != 8 ]]; then
    echo "SDO_HERMETIC_JOBS must be 4 or 8" >&2
    exit 2
fi
export GOMAXPROCS="$hermetic_jobs" GOFLAGS="${GOFLAGS:-} -p=$hermetic_jobs"

# Deployed-source tests always exercise the portable repository fixture.
# Set SDO_DRIFT_SEED explicitly to also check a full deployed hotel seed.

(
    cd "$root/sdo/operator/runtime"
    go test -tags=integration -run '^(TestPendingDriftJoinsIncidentBeforeVerifiedClosure|TestPendingDriftPreventsVerificationWhenPeerIsUnresolved|TestHermeticFastloopRealGoGate|TestHermeticNewDetectorInputs|TestHermeticNewSnapshotKinds|TestHermeticFakeConfigMapEnforcesProductionLimit|TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit|TestHermeticResponderProgramReuseThroughRealToolTransport|TestHermeticAcceptedSourceAfterMemoryCommit|TestGitPlanSourceUsesOnlyDeployedManifestSet|TestGitPlanSourceSelectsDeployedHotelSeed|TestControllerAdoptsSeededServiceThenDispatchesLaterDrift|TestUnexplainedTargetPortOpensLatentIncidentWithDriftCorpus|TestUnexplainedDriftDuringResponseAttachesToOpenIncident|TestHermeticForeignNetworkPolicyOpensLatentIncident|TestForeignNetworkPolicyInvariantNearMisses|TestForeignDriftNormalChurn|TestHermeticDriftlive4NewFaultAfterHealthyBaseline|TestRepairedDriftRecursWithoutProgramAfterRestart|TestHermeticRoutingCaptureCompilesReplayableMitigator|TestIncidentCaptureRecordsRoutingTCPBeforeAnyProgramExists|TestRoutingCaptureHasCountAndTimeBudgets|TestRoutingCaptureRecordsUnavailableCapabilityOnce|TestHermeticAdoptedHostPortRepairLearnsAndReplays|TestHermeticAgentRepairsTwoCoveredFaultsAndOneUnexplained|TestHermeticBringupMaintenanceGenerationDuringOnboarding|TestHermeticBringupPersistsPendingAndRestarts|TestHermeticControllerClosureReachesNativeOutcomeBroker|TestHermeticLearnAcceptRepeatKeepsControllerIterating|TestHermeticReflectionLearnsThenAgentSelectsIdenticalRecurrence|TestHermeticResponderThreeFaultContinuation|TestHermeticResponderWorkingPlaybookCLI|TestHermeticResponsePipelineClearsTwoFaultsThroughControllerClosure|TestHermeticSavedRepeatWaitsForSharedMinuteWithoutModels|TestHermeticSharedMemoryControllers|TestHermeticSourceReconcileLearningExposesVerifiedCounters|TestHermeticStaleSavedStepRepromptsWithOnlyTheStillBrokenFinding)$' -count=1 .
    go test -tags=integration -run '^TestHermeticProtocolDialer$' -count=1 ./prober
)

(
    cd "$root/sdo/operator"
    go test -tags=integration ./broker -run '^TestAgentScriptJudgeMemoryCheckAndBroker$' -count=1
    go test -tags=integration ./install -run '^TestObservabilityPlanAndApply$' -count=1
)

cd "$root"
uv run --extra test pytest -q -p no:tach \
    tests/unit/benchmarks/sregym/test_agent_script.py \
    tests/unit/benchmarks/sregym/fastloop/test_fake_kube.py \
    tests/unit/benchmarks/sregym/fastloop/test_toy.py \
    tests/unit/benchmarks/sregym/fastloop/test_fastloop_loop.py \
    tests/unit/benchmarks/sregym/fastloop/test_fastloop_fault_driver.py \
    tests/unit/benchmarks/sregym/fastloop/test_fastloop_hermetic_run.py \
    tests/unit/benchmarks/sregym/fastloop/test_fastloop_composite.py::test_failed_composite_reset_restarts_controller_before_next_incident \
    tests/unit/benchmarks/sregym/adapter/test_native_persistent_ops.py::test_reset_stage_syncs_accepted_pvc_before_recreating_controller \
    tests/unit/benchmarks/sregym/fastloop/test_fastloop_reset.py::test_reset_cancels_and_awaits_workers_before_deleting_captures
