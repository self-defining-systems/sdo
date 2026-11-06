#!/bin/bash
# Run python tests using pytest via uv

set -e

# Get the directory of the script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "Running Python tests..."
cd "$PROJECT_ROOT"

# xdist workers swallow a missing-prerequisite error as an INTERNALERROR with no
# reason; fail here, once, with the remedy printed.
python3 scripts/init_test_prereqs.py --check

source "$PROJECT_ROOT/scripts/isolate_git_config.sh"

# A pull request run names the test files its changes can affect (scripts/ci_select.py);
# without a plan, or when the plan is the full suite, the whole shard runs.
selected=()
if [[ -n "${SDO_CI_PLAN:-}" && "$(python3 scripts/ci_select.py mode --plan "$SDO_CI_PLAN")" == selected ]]; then
    mapfile -t selected < <(python3 scripts/ci_select.py python-paths --plan "$SDO_CI_PLAN" --shard "${SDO_PYTEST_SHARD:-core}")
    # pytest runs an explicitly named file even when --ignore excludes it, so a selected file the
    # caller excludes (the nightly-tier suites) must be dropped here or it runs in the pull request.
    kept=()
    for path in "${selected[@]}"; do
        [[ -n "$path" ]] || continue
        excluded=false
        for arg in "$@"; do
            [[ "$arg" == "--ignore=$path" ]] && excluded=true
        done
        $excluded || kept+=("$path")
    done
    selected=("${kept[@]}")
    if [[ ${#selected[@]} -eq 0 ]]; then
        echo "no Python tests selected for the ${SDO_PYTEST_SHARD:-core} shard"
        exit 0
    fi
    echo "==> selected ${#selected[@]} Python test files"
fi

# Build the benchmark protocol tools once for this invocation, from this checkout.
# xdist workers otherwise independently compile/link the same executables on their
# first receipt/submission test. Only compiler entries persist; never trust a
# restored executable from another source revision.
TOOLS="$(mktemp -d)"
trap 'rm -rf "$TOOLS"' EXIT
builds=()
for spec in 'native_receipt_go sdo-native-receipt' 'submission_go sdo-submission-guard'; do
    read -r module binary <<< "$spec"
    (cd "benchmarks/sregym/protocol/$module" && go build -o "$TOOLS/$binary" "./cmd/$binary") &
    builds+=($!)
done
for build in "${builds[@]}"; do wait "$build"; done
echo "==> protocol tools built after ${SECONDS}s"
export SDO_NATIVE_RECEIPT_BIN="$TOOLS/sdo-native-receipt"
export SDO_SUBMISSION_GUARD_BIN="$TOOLS/sdo-submission-guard"

# tests/conftest.py builds the operator CLI once per checkout under a file lock,
# and every xdist worker waits on it before its first test. Start that build now
# so it overlaps collection (30-50 s on a hosted runner) instead of following it.
# The same content-addressed binary and lock serve the workers; a failure here
# just leaves the build to them.
if [[ -z "${SDO_SKIP_OPERATOR_PREBUILD:-}" ]]; then
    python3 scripts/operator_test_binary.py > /dev/null 2>&1 &
fi

# Tests that run SREGym's own classes call `uv run --directory third_party/sregym`; the first call
# creates or syncs that environment (26.8 s on a cold runner, charged to whichever test ran first).
# Do it now so it overlaps collection. Failure just leaves the sync to those tests.
if [[ -d third_party/sregym && -z "${SDO_SKIP_SREGYM_ENV_PREWARM:-}" ]]; then
    uv run --directory third_party/sregym python -c pass > /dev/null 2>&1 &
fi

# Bound shared-host parallelism; pytest-cov combines the per-worker
# coverage so the fail_under gate still sees the whole suite. Override with
# SDO_PYTEST_WORKERS (e.g. 0 to run in-process for a targeted local run).
if [[ -v SDO_PYTEST_WORKERS ]]; then
    WORKERS="$SDO_PYTEST_WORKERS"
else
    WORKERS="$(nproc)"
    if (( WORKERS > 4 )); then
        WORKERS=4
    fi
fi
# The hosted Python runners share the suite. The adapter tests are about 60% of the non-fastloop
# test time, so they have a runner of their own; every other path belongs to core, including new
# test directories.
# Shards publish raw data; Required PR CI combines it before enforcing coverage.
paths=(tests)
coverage=(--cov --cov-report=term-missing)
case "${SDO_PYTEST_SHARD:-}" in
    "") ;;
    core)
        paths=(--ignore=tests/unit/benchmarks/sregym/fastloop --ignore=tests/unit/benchmarks/sregym/adapter tests)
        coverage=(--cov --cov-report= --cov-fail-under=0)
        ;;
    adapter)
        paths=(tests/unit/benchmarks/sregym/adapter)
        coverage=(--cov --cov-report= --cov-fail-under=0)
        ;;
    fastloop)
        paths=(tests/unit/benchmarks/sregym/fastloop)
        coverage=(--cov --cov-report= --cov-fail-under=0)
        ;;
    *) echo "unknown Python test shard: $SDO_PYTEST_SHARD" >&2; exit 2 ;;
esac
if [[ ${#selected[@]} -gt 0 ]]; then
    paths=("${selected[@]}")
    coverage=(--no-cov) # a partial run says nothing about the floor, which full runs enforce
fi
# Quarantined flaky tests (.github/ci_quarantine.json) run in the nightly tier only.
mapfile -t quarantined < <(python3 scripts/ci_quarantine.py pytest-deselect)
if [[ ${#quarantined[@]} -gt 0 && -n "${quarantined[0]}" ]]; then
    set -- "$@" "${quarantined[@]}"
fi
# Tach's plugin only selects impacted tests when asked (--tach); otherwise it
# adds about ten seconds of git analysis to every xdist worker's collection.
# Module boundaries are enforced by `tach check` in check_errors.sh.
# SDO_PYTEST_LOG keeps the output (with --durations) for scripts/check_slow_tests.py.
if [[ -n "${SDO_PYTEST_LOG:-}" ]]; then
    set -o pipefail
    uv run --extra test pytest -p no:tach -n "$WORKERS" "${coverage[@]}" "$@" "${paths[@]}" | tee "$SDO_PYTEST_LOG"
else
    uv run --extra test pytest -p no:tach -n "$WORKERS" "${coverage[@]}" "$@" "${paths[@]}"
fi
