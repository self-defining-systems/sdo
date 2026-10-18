#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "${repo_root}/scripts/build_sdo_images.sh"
exec bash "${repo_root}/scripts/smoke_sdo_runtime_kind.sh"
