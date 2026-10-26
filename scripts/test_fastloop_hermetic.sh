#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Deployed-source tests always exercise the portable repository fixture.
# Set SDO_DRIFT_SEED explicitly to also check a full deployed hotel seed.

(
    cd "$root/sdo/operator"
    go test -run '^(TestKubernetesExecutorNeverRecreatesWorkerWithUnsettledResult|TestKubernetesExecutorConcurrentPayloads|TestKubernetesExecutorHealthJudge|TestKubernetesExecutorRecoversRequestBoundStagedResult|TestRuntimeArtifactsExportsSetupUsage)' -count=1 ./worker_registry ./operatorcli
)

(
    cd "$root/sdo/operator"
    go test -run '^TestResetControlState|^TestParseResetControlState' -count=1 ./onboarding ./operatorcli

    go test -run '^TestInstallExisting.*Onboarding|^TestInstallExistingClearAdoption|^TestInstallExistingAdoptedVerifiedSeedOnboarding$' -count=1 -timeout=5m ./operatorcli
)

# Fixture checks compile their own isolated CLI/workspaces. Run that process
# alongside the remaining runtime checks, with four Go workers on each side.
# Separate processes also isolate tests that set session environment variables.
(
    export GOMAXPROCS=4 GOFLAGS="${GOFLAGS:-} -p=4"
    cd "$root/sdo/operator/runtime"
    go test -run '^(TestHermeticSharedMemoryControllers|TestReflectionHotelRunScriptUpgradesReadmeThroughCLIAndBroker|TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker|TestReflectionLearnsDeploymentRepairWithUnwrittenServiceSymptoms)$' -count=1 -timeout=5m .
) &
reflection_pid=$!
runtime_status=0

(
    # The Python shard runs while the first Go group needs three workers.
    # Reflection has four: 4 + 3 + 1 stays within eight concurrent workers.
    (
        export GOMAXPROCS=1 GOFLAGS="${GOFLAGS:-} -p=1"
        cd "$root"
        uv run --extra test pytest -q -p no:tach \
            tests/unit/scripts/test_hermetic_shards.py \
            tests/unit/libs/agent_cli/test_structured.py \
            tests/unit/sdo/agents/test_health_judge_worker.py \
            tests/unit/sdo/agents/responder/test_result_publisher.py \
            tests/unit/benchmarks/sregym/fastloop/test_fastloop_hermetic_run.py \
            tests/unit/benchmarks/sregym/fastloop/test_fastloop_loop.py \
            tests/unit/benchmarks/sregym/fastloop/test_fastloop_cli.py::test_fresh_memory_resets_control_state_before_install_but_warm_repeat_keeps_it \
            tests/unit/benchmarks/sregym/fastloop/test_sdo3_descriptions.py \
            tests/unit/benchmarks/sregym/fastloop/test_sdo3_s3a_contracts.py \
            tests/unit/benchmarks/sregym/fastloop/test_fastloop_composite.py::test_failed_composite_resets_only_after_verified_harness_recovery \
            tests/unit/benchmarks/sregym/adapter/test_native_persistent_ops.py::test_reset_stage_syncs_accepted_pvc_before_recreating_controller \
            tests/unit/benchmarks/sregym/fastloop/test_fastloop_reset.py::test_reset_cancels_and_awaits_workers_before_deleting_captures \
            tests/unit/benchmarks/sregym/adapter/test_scripted_drift.py \
            tests/unit/benchmarks/sregym/assurance/test_mixed10_replay.py \
            tests/unit/benchmarks/sregym/fastloop/test_fastloop_fault_driver.py \
            tests/unit/benchmarks/sregym/fastloop/test_scripted_image_build.py
    ) &
    python_pid=$!
    shard_status=0
    export GOMAXPROCS=3 GOFLAGS="${GOFLAGS:-} -p=3"
    cd "$root/sdo/operator/runtime"
    go test -run '^(TestHermeticBringupPersistsPendingAndRestarts|TestHermeticBringupMaintenanceGenerationDuringOnboarding|TestRunOnboardingMaintenanceGenerationPreservesSamples|TestPendingDetector|TestSourceInventory|TestLatent|TestTrafficPartialIteration)' -count=1 -timeout=5m . || shard_status=$?
    wait "$python_pid" || shard_status=$?
    export GOMAXPROCS=4 GOFLAGS="${GOFLAGS:-} -p=4"
    go test -run '^(TestRunScriptActVerifyRequiresDiagnosisAndTargetedClearance|TestRunScriptJobBoundsRecoveryAndEnvironment|TestRunScriptFailureNeverAutomaticallyUndoesAndOutputStaysPrivate|TestRunScriptRawMenuEnvironmentValues|TestLearnedOriginCreditIncludesObservedUnwrittenSymptoms|TestPartialReflectionRetainsRecoveredUnwrittenSymptoms|TestEvidenceGapsRecognizeCapturedSymptomObjects|TestHermeticLearnAcceptRepeatKeepsControllerIterating|TestRunRepeatWithLearnedDetectorErrorAndQueuedPodChanges|TestProgramDiagnosisRequiresFindingSevenRoles|TestObjectChangeAndTimerNeverDiagnosePlaybooks|TestDriftAndControllerEntrypointsCannotInvokePlaybookDiagnosis|TestHermeticResponderThreeFaultContinuation|TestMitigationReturnsWithUnrelatedFaultBeforeGlobalHealthBudget|TestHermeticSourceReconcileLearningExposesVerifiedCounters|TestHermeticAgentRepairsTwoCoveredFaultsAndOneUnexplained|TestPlaybookCoverageIncludesEveryMatchAndUnexplainedFinding|TestPendingDriftJoinsIncidentBeforeVerifiedClosure|TestPendingDriftPreventsVerificationWhenPeerIsUnresolved|TestNewProgramGetsOneVerifiedOriginCreditWithoutPriorProgramRun|TestAcceptedCorpusDoesNotInventTrustForAnUnmatchedOrigin|TestRunPreservesStateChangesAcrossObservationBarrier|TestRunPreservesLatentFaultAcrossObservationBarrier|TestHermeticFastloopRealGoGate|TestHermeticReflectionLearnsThenAgentSelectsIdenticalRecurrence|TestReadmeOnlyVerifiedReflectionIsRejectedAndRetried|TestFailedGlobalClosureCanLearnControllerAllClearRepair|TestFailedClosureReflectionReceivesExactlyVerifiedCauses|TestReflectionPublicationRetainsFrozenHealthWithCapture|TestHermeticFakeConfigMapEnforcesProductionLimit|TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit|TestHermeticResponderProgramReuseThroughRealToolTransport|TestHermeticResponderWorkingPlaybookCLI|TestHermeticAcceptedSourceAfterMemoryCommit|TestAcceptedSourceRenderFailureIsObjectScoped|TestAcceptedSourceDoesNotReuseImagesAcrossSourceChange|TestControllerAdoptsAvailableObjectsWithExplicitRenderGap|TestGitPlanSourceUsesOnlyDeployedManifestSet|TestGitPlanSourceSelectsDeployedHotelSeed|TestControllerAdoptsSeededServiceThenDispatchesLaterDrift|TestUnexplainedTargetPortOpensLatentIncidentWithDriftCorpus|TestUnexplainedDriftDuringResponseAttachesToOpenIncident|TestHermeticForeignNetworkPolicyOpensLatentIncident|TestForeignNetworkPolicyInvariantNearMisses|TestForeignDriftNormalChurn|TestHermeticBenignDriftAcrossFastloopResetAndNextIncident|TestHermeticDriftlive4NewFaultAfterHealthyBaseline|TestRepairedDriftRecursWithoutProgramAfterRestart|TestVerifyRetainsDriftWithHealthyDetectors|TestClosureAdmittedDriftContracts|TestClosureDriftResolvedByRestoringSource|TestClosureL3I04Reduction|TestClosureDriftSourceErrorFailsClosed|TestClosureDriftRestoredFieldsIgnoreUnadmittedChanges|TestClosureDriftUsesSourceFieldsWhenReconcileIsNoop|TestSimSchedulePartialRepairCannotVerifyDrift|TestUnexplainedDriftSourceErrorNeverClears|TestTerminalDriftResponderFailureDoesNotWaitForTTL|TestCheckedBenignNoopClosesWithoutMitigationCredit|TestHermeticRoutingCaptureCompilesReplayableMitigator|TestIncidentCaptureRecordsRoutingTCPBeforeAnyProgramExists|TestRoutingCaptureHasCountAndTimeBudgets|TestRunCapturesRoutingWithoutTrafficWorkloads|TestRunStartsRoutingProberWithoutTrafficWorkloads|TestRoutingCaptureRecordsUnavailableCapabilityOnce|TestGS1DeployedIdentityAndNamespaceScope|TestGS1TrustSurvivesProgramRevision|TestRV4RestartMustNotBlessChangedPinnedImage|TestRV4PublicationFailureMustKeepAcceptedCommit|TestRV4OldReadyReplicaMustNotCountAsCompletedRollout|TestRenderedLockRestartWithoutNamedEvidenceHasObjectGap|TestRenderedLockJobNeedsRequestedCompletions|TestRenderedLockMutableImageReferencesDoNotCreateDrift|TestRenderedLockAfterRolloutAndRestart|TestRenderedLockMissingIsEvidenceGap|TestRenderedLockPublicationWaitsForClosureAcknowledgment|TestRenderedLockPerObjectGaps|TestRenderedLockConsulGapAfterMemoryCommitKeepsReservationDrift|TestSimRenderedLockSchedule|TestClosureRetainsDriftWithHealthyDetectors|TestTerminalDriftResponderFailureDoesNotWaitForTTLOrCloseHealthy|TestOrdinaryDriftAdmissionBudgetsBatchingAndNewKeyPriority|TestHermeticControllerClosureReachesNativeOutcomeBroker|TestControllerIntegrationInformerPersistenceAndRestart|TestClosureNamesRolloutBlockersWithoutProgramApplication|TestClosureRolloutIgnoresOtherNamespacesAndHelpers|TestClosureRolloutBlockerClasses|TestClosureRolloutRecordedWriteReachesCachedVerification|TestClosureRolloutWaitsBeforePublishingSuccess)$' -count=1 -timeout=5m . || shard_status=$?
    exit "$shard_status"
) || runtime_status=$?

wait "$reflection_pid" || runtime_status=$?
if ((runtime_status != 0)); then
    exit "$runtime_status"
fi
