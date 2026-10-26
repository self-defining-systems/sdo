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
reflection_jobs=$((hermetic_jobs / 2))
first_runtime_jobs=$((reflection_jobs - 1))
export GOMAXPROCS="$hermetic_jobs" GOFLAGS="${GOFLAGS:-} -p=$hermetic_jobs"

# Deployed-source tests always exercise the portable repository fixture.
# Set SDO_DRIFT_SEED explicitly to also check a full deployed hotel seed.

(
    cd "$root/sdo/operator"
    go test -run '^(TestKubernetesExecutorVanishedJobIsTerminalAcrossRestart|TestKubernetesExecutorNeverRecreatesWorkerWithUnsettledResult|TestKubernetesExecutorConcurrentPayloads|TestKubernetesExecutorHealthJudge|TestKubernetesExecutorRecoversRequestBoundStagedResult|TestRuntimeArtifactsExportsSetupUsage)' -count=1 ./worker_registry ./operatorcli
)

(
    cd "$root/sdo/operator"
    go test -run '^TestResetControlState|^TestParseResetControlState' -count=1 ./onboarding ./operatorcli
    go test -run '^(TestInstallPlanCarriesOwnershipOnEveryObject|TestInstallReconcileAddsOwnershipToLegacyCaptureGrants|TestDiscoveredPermissiveReadGrantsCarryInstallOwnership)$' -count=1 -timeout=1m ./install

    go test -run '^TestInstallExisting.*Onboarding|^TestInstallExistingClearAdoption|^TestInstallExistingAdoptedVerifiedSeedOnboarding$' -count=1 -timeout=5m ./operatorcli
)

# Fixture checks compile their own isolated CLI/workspaces. Run that process
# alongside the remaining runtime checks, with four Go workers on each side.
# Separate processes also isolate tests that set session environment variables.
(
    export GOMAXPROCS="$reflection_jobs" GOFLAGS="${GOFLAGS:-} -p=$reflection_jobs"
    cd "$root/sdo/operator/runtime"
    go test -run '^(TestHermeticSharedMemoryControllers|TestReflectionHotelRunScriptUpgradesReadmeThroughCLIAndBroker|TestReflectionMongoAuthUpgradesReadmeThroughCLIAndBroker|TestReflectionLearnsDeploymentRepairWithUnwrittenServiceSymptoms)$' -count=1 -timeout=5m .
) &
reflection_pid=$!
runtime_status=0

(
    # The Python shard runs while the first Go group needs three workers.
    # Reflection, runtime and Python share the selected four/eight-worker cap.
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
    export GOMAXPROCS="$first_runtime_jobs" GOFLAGS="${GOFLAGS:-} -p=$first_runtime_jobs"
    cd "$root/sdo/operator/runtime"
    go test -run '^(TestHermeticBringupPersistsPendingAndRestarts|TestHermeticBringupMaintenanceGenerationDuringOnboarding|TestRunOnboardingMaintenanceGenerationPreservesSamples|TestPendingDetector|TestSourceInventory|TestInstallEnvironment|TestSimInstallerOwnership|TestLatent|TestTrafficPartialIteration)' -count=1 -timeout=5m . || shard_status=$?
    wait "$python_pid" || shard_status=$?
    export GOMAXPROCS="$reflection_jobs" GOFLAGS="${GOFLAGS:-} -p=$reflection_jobs"
    go test -run '^(TestHermeticResponsePipelineClearsTwoFaultsThroughControllerClosure|TestDeterministicResponseStageUsesPostApplyEvidenceForOpeningIncidentFinding|TestDeterministicResponseStageHandoffRetainsObservedDetectorIdentity|TestDeterministicResponseStageReflectionReceivesVerifiedRepairCauses|TestResponseStageRejoinRetainsDeterministicApplicationEvidence|TestResponseStageVerifiedRepairsReachRegisteredReflection|TestDeterministicResponseStageWaitsForPostRolloutHealthObservation|TestDeterministicResponseStageCannotClearWithoutIndependentProof|TestDeterministicResponseStageObservationBudgetPreservesUnresolvedHealth|TestDeterministicResponseStageRejoinReobservesRetainedIncidentFinding|TestSimScheduleDeterministicPipelineEvidence|TestRunScriptActVerifyRequiresDiagnosisAndTargetedClearance|TestRunScriptJobBoundsRecoveryAndEnvironment|TestRunScriptFailureNeverAutomaticallyUndoesAndOutputStaysPrivate|TestRunScriptRawMenuEnvironmentValues|TestLearnedOriginCreditIncludesObservedUnwrittenSymptoms|TestPartialReflectionRetainsRecoveredUnwrittenSymptoms|TestEvidenceGapsRecognizeCapturedSymptomObjects|TestHermeticLearnAcceptRepeatKeepsControllerIterating|TestRunRepeatWithLearnedDetectorErrorAndQueuedPodChanges|TestProgramDiagnosisRequiresFindingSevenRoles|TestObjectChangeAndTimerNeverDiagnosePlaybooks|TestDriftAndControllerEntrypointsCannotInvokePlaybookDiagnosis|TestHermeticResponderThreeFaultContinuation|TestMitigationReturnsWithUnrelatedFaultBeforeGlobalHealthBudget|TestHermeticSourceReconcileLearningExposesVerifiedCounters|TestHermeticAgentRepairsTwoCoveredFaultsAndOneUnexplained|TestPlaybookCoverageIncludesEveryMatchAndUnexplainedFinding|TestPendingDriftJoinsIncidentBeforeVerifiedClosure|TestPendingDriftPreventsVerificationWhenPeerIsUnresolved|TestNewProgramGetsOneVerifiedOriginCreditWithoutPriorProgramRun|TestAcceptedCorpusDoesNotInventTrustForAnUnmatchedOrigin|TestRunPreservesStateChangesAcrossObservationBarrier|TestRunPreservesLatentFaultAcrossObservationBarrier|TestHermeticFastloopRealGoGate|TestHermeticReflectionLearnsThenAgentSelectsIdenticalRecurrence|TestReadmeOnlyVerifiedReflectionIsRejectedAndRetried|TestFailedGlobalClosureCanLearnControllerAllClearRepair|TestHermeticFakeConfigMapEnforcesProductionLimit|TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit|TestHermeticResponderProgramReuseThroughRealToolTransport|TestHermeticResponderWorkingPlaybookCLI|TestHermeticAcceptedSourceAfterMemoryCommit|TestAcceptedSourceRenderFailureIsObjectScoped|TestAcceptedSourceDoesNotReuseImagesAcrossSourceChange|TestControllerAdoptsAvailableObjectsWithExplicitRenderGap|TestGitPlanSourceUsesOnlyDeployedManifestSet|TestGitPlanSourceSelectsDeployedHotelSeed|TestControllerAdoptsSeededServiceThenDispatchesLaterDrift|TestUnexplainedTargetPortOpensLatentIncidentWithDriftCorpus|TestUnexplainedDriftDuringResponseAttachesToOpenIncident|TestHermeticForeignNetworkPolicyOpensLatentIncident|TestForeignNetworkPolicyInvariantNearMisses|TestForeignDriftNormalChurn|TestHermeticBenignDriftAcrossFastloopResetAndNextIncident|TestHermeticDriftlive4NewFaultAfterHealthyBaseline|TestRepairedDriftRecursWithoutProgramAfterRestart|TestVerifyRetainsDriftWithHealthyDetectors|TestClosureAdmittedDriftContracts|TestClosureDriftResolvedByRestoringSource|TestClosureL3I04Reduction|TestClosureDriftSourceErrorFailsClosed|TestClosureDriftRestoredFieldsIgnoreUnadmittedChanges|TestClosureDriftUsesSourceFieldsWhenReconcileIsNoop|TestSimSchedulePartialRepairCannotVerifyDrift|TestUnexplainedDriftSourceErrorNeverClears|TestTerminalDriftResponderFailureDoesNotWaitForTTL|TestCheckedBenignNoopClosesWithoutMitigationCredit|TestHermeticRoutingCaptureCompilesReplayableMitigator|TestIncidentCaptureRecordsRoutingTCPBeforeAnyProgramExists|TestRoutingCaptureHasCountAndTimeBudgets|TestRunCapturesRoutingWithoutTrafficWorkloads|TestRunStartsRoutingProberWithoutTrafficWorkloads|TestRoutingCaptureRecordsUnavailableCapabilityOnce|TestGS1DeployedIdentityAndNamespaceScope|TestGS1TrustSurvivesProgramRevision|TestRV4RestartMustNotBlessChangedPinnedImage|TestRV4PublicationFailureMustKeepAcceptedCommit|TestRV4OldReadyReplicaMustNotCountAsCompletedRollout|TestRenderedLockRestartWithoutNamedEvidenceHasObjectGap|TestRenderedLockJobNeedsRequestedCompletions|TestRenderedLockMutableImageReferencesDoNotCreateDrift|TestRenderedLockAfterRolloutAndRestart|TestRenderedLockMissingIsEvidenceGap|TestRenderedLockPublicationWaitsForClosureAcknowledgment|TestRenderedLockPerObjectGaps|TestRenderedLockConsulGapAfterMemoryCommitKeepsReservationDrift|TestSimRenderedLockSchedule|TestClosureRetainsDriftWithHealthyDetectors|TestTerminalDriftResponderFailureDoesNotWaitForTTLOrCloseHealthy|TestOrdinaryDriftAdmissionBudgetsBatchingAndNewKeyPriority|TestDeterministicResponseStageWaitFollowsDetectorWindow|TestDeterministicResponseStageWaitStopsAtCeiling|TestReflectionVanishedJobStopsClosureRetriesWithEvidence|TestReflectionFailedJobStopsClosureRetriesWithEvidence|TestFailedClosureReflectionReceivesExactlyVerifiedCauses|TestReflectionPublicationRetainsFrozenHealthWithCapture|TestClosureDriftRemovedExtraFieldsAgreeWithStatus|TestHermeticControllerClosureReachesNativeOutcomeBroker|TestControllerIntegrationInformerPersistenceAndRestart|TestClosureNamesRolloutBlockersWithoutProgramApplication|TestClosureRolloutIgnoresOtherNamespacesAndHelpers|TestClosureRolloutBlockerClasses|TestClosureRolloutRecordedWriteReachesCachedVerification|TestClosureRolloutWaitsBeforePublishingSuccess)$' -count=1 -timeout=5m . || shard_status=$?
    exit "$shard_status"
) || runtime_status=$?

wait "$reflection_pid" || runtime_status=$?
if ((runtime_status != 0)); then
    exit "$runtime_status"
fi
