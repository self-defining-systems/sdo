# Shared helpers of the vcluster lane provider (sourced by vcluster-lane.sh and shared-registry.sh).
#
# vc_host_kubeconfig  makes $HK, a private readable copy of the shared cluster's admin kubeconfig
#                     ($VCLUSTER_HOST_KUBECONFIG; a file only a group can read is read through `sg <group>`).
# hk ARGS...          kubectl on the shared (host) cluster.
set -euo pipefail

vc_host_kubeconfig() {
  : "${VCLUSTER_HOST_KUBECONFIG:?VCLUSTER_HOST_KUBECONFIG (the shared cluster admin kubeconfig) is not set}"
  local dir=${VCLUSTER_STATE_DIR:-${XDG_RUNTIME_DIR:-/tmp}/fuzzops-vcluster-$(id -u)}
  mkdir -p "$dir" && chmod 700 "$dir"
  HK="$dir/host.kubeconfig"
  local g=${VCLUSTER_HOST_GROUP:-docker}
  if [ -r "$VCLUSTER_HOST_KUBECONFIG" ]; then
    cat "$VCLUSTER_HOST_KUBECONFIG" >"$HK.tmp.$$"
  else
    sg "$g" -c "cat '$VCLUSTER_HOST_KUBECONFIG'" >"$HK.tmp.$$" ||
      { rm -f "$HK.tmp.$$"; echo "cannot read $VCLUSTER_HOST_KUBECONFIG (directly or as group $g)" >&2; return 1; }
  fi
  chmod 600 "$HK.tmp.$$" && mv "$HK.tmp.$$" "$HK"
  export HK
}

hk() { kubectl --kubeconfig "$HK" "$@"; }
