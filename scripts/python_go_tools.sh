#!/usr/bin/env bash
# Prepare real adapter executables once; consumers require this run's artifact.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:?select prepare or env}"
directory="${2:-$root/.fastloop/python-go-tools}"
case "$mode" in
  prepare)
    mkdir -p "$directory"
    python3 "$root/scripts/operator_test_binary.py" --describe > "$directory/source-inputs.json"
    operator="$(python3 "$root/scripts/operator_test_binary.py")"
    echo "Prepared Python operator: $operator" >&2
    cp "$operator" "$directory/sdo-operator"
    for module in native_receipt_go submission_go; do
      case "$module" in
        native_receipt_go) name=sdo-native-receipt ;;
        submission_go) name=sdo-submission-guard ;;
      esac
      (cd "$root/benchmarks/sregym/protocol/$module" && go build -o "$directory/$name" "./cmd/$name")
    done
    python3 "$root/scripts/operator_test_binary.py" --check "$directory/sdo-operator"
    # Tar preserves executable permissions through upload/download-artifact.
    tar -cf "$directory.tar" -C "$directory" .
    ;;
  env)
    for name in sdo-operator sdo-native-receipt sdo-submission-guard; do
      if [[ ! -x "$directory/$name" ]]; then
        echo "missing prepared Python Go tool: $directory/$name; download this run's artifact" >&2
        exit 1
      fi
    done
    if ! python3 "$root/scripts/operator_test_binary.py" --check "$directory/sdo-operator"; then
      python3 "$root/scripts/operator_test_binary.py" --describe >&2
      cat "$directory/source-inputs.json" >&2
      exit 1
    fi
    printf 'SDO_OPERATOR_BIN=%s\nSDO_NATIVE_RECEIPT_BIN=%s\nSDO_SUBMISSION_GUARD_BIN=%s\n' \
      "$directory/sdo-operator" "$directory/sdo-native-receipt" "$directory/sdo-submission-guard"
    ;;
  *) echo "unknown Python Go tool mode: $mode" >&2; exit 1 ;;
esac
