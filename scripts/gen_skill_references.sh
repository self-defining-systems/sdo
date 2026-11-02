#!/bin/bash
# SDK indexes and topic files share one generator and drift check.
set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
exec python3 "$SCRIPT_DIR/gen_skill_references.py" "$@"
