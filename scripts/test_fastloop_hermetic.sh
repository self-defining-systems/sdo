#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
seed=/mnt/data/shli/sdo-exp/luna-mixed-pilot/seed/fair-lifecycle-fmt
if [[ -d "$seed/kubernetes" ]]; then
    export SDO_DRIFT_SEED="$seed"
fi

(
    cd "$root/sdo/operator/runtime"
    go test -run '^(TestHermeticFastloopRealGoGate|TestHermeticFakeConfigMapEnforcesProductionLimit|TestRegisteredReflectionWorkerRejectsOversizedResultBeforeCommit|TestHermeticResponderProgramReuseThroughRealToolTransport|TestGitPlanSourceUsesOnlyDeployedManifestSet|TestGitPlanSourceSelectsDeployedHotelSeed|TestControllerAdoptsSeededServiceThenDispatchesLaterDrift|TestInvestigatedTargetPortOpensLatentIncidentWithDriftCorpus|TestHermeticFourIncidentTrustedFastPathCompletesWithoutResponderJob|TestHermeticTrustedLatentDriftFastPathSkipsResponderAndReflection|TestFailedFastPathRollsBackAndDispatchesResponderWithSummary|TestTrustedProgramFastPathRequiresEveryFindingExplained|TestTrustedProgramFastPathRejectsNeedsJudgement|TestNativeBrokerSkipsReflectionAfterTrustedFastPath)$' -count=1 .
)

cd "$root"
uv run --extra test pytest -q -p no:tach tests/unit/benchmarks/sregym/fastloop/test_fastloop_hermetic_run.py
