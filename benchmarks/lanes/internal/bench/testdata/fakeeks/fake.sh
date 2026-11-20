#!/bin/sh
# Stand-in aws, eksctl, kubectl and docker for the EKS lane tests (deploy/eks-lane.sh). State in $FAKE_STATE:
# cluster-<name> (exists), cfg-<name> (the eksctl config it was made from), eks.log (every call).
tool=$1; shift
S=${FAKE_STATE:?}
echo "$tool $*" >> "$S/eks.log"
name_of() { sed -n 's/^  name: //p' "$1" | head -1; }
nodes_of() { c=$(ls "$S"/cfg-* 2>/dev/null | head -1); n=$(sed -n 's/.*desiredCapacity: //p' "$c"); echo "${n:-2}"; }
case $tool in
aws)
  case "$1 $2" in
  "sts get-caller-identity") echo "${FAKE_AWS_ACCOUNT:-123456789012}";;
  "ecr get-login-password") echo tok;;
  "ecr describe-repositories") [ -e "$S/repo-$(echo "$*" | sed 's/.*--repository-names //' | tr '/' '_')" ] || { echo "RepositoryNotFoundException" >&2; exit 254; };;
  "ecr create-repository") touch "$S/repo-$(echo "$*" | sed 's/.*--repository-name \([^ ]*\).*/\1/' | tr '/' '_')";;
  "ecr describe-images") echo "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";;
  "eks update-kubeconfig") echo "apiVersion: v1" > "$(echo "$*" | sed 's/.*--kubeconfig //')";;
  "ec2 describe-subnets") echo us-west-2a;;
  "eks describe-nodegroup") cat "$S/desired" 2>/dev/null || nodes_of;;
  "eks describe-cluster")
    case "$*" in
    *clusterSecurityGroupId*) echo sg-0123;;
    *) n=$(echo "$*" | sed 's/.*--name \([^ ]*\).*/\1/'); [ -n "${FAKE_AWS_DOWN:-}" ] && { echo "Could not connect to the endpoint URL" >&2; exit 255; }
       [ -e "$S/cluster-$n" ] || { echo "An error occurred (ResourceNotFoundException) when calling the DescribeCluster operation: No cluster found for name: $n." >&2; exit 254; }
       echo ACTIVE;;
    esac;;
  "ec2 authorize-security-group-ingress") :;;
  esac;;
eksctl)
  case "$1 $2" in
  "get cluster") [ -e "$S/cluster-$4" ] || exit 1;;
  "create cluster")
    cfg=$4; n=$(name_of "$cfg"); kc=$6
    echo "create-start $n $(date +%s.%N)" >> "$S/eks-times.log"
    [ -z "${FAKE_EKS_CREATE_SECONDS:-}" ] || sleep "$FAKE_EKS_CREATE_SECONDS"
    echo "create-end $n $(date +%s.%N)" >> "$S/eks-times.log"
    cp "$cfg" "$S/cfg-$n"; touch "$S/cluster-$n"
    echo "apiVersion: v1" > "$kc"
    [ -z "${FAKE_EKS_FAIL_CREATE:-}" ] || { echo "eksctl: stack failed" >&2; exit 1; };;
  "delete cluster") rm -f "$S/cluster-$4";;
  "scale nodegroup") echo "$(echo "$*" | sed 's/.*--nodes \([0-9]*\).*/\1/')" > "$S/desired";;
  esac;;
kubectl)
  case " $* " in
  *" get nodes --no-headers"*) n=$(nodes_of); i=1; while [ "$i" -le "$n" ]; do echo "ip-10-0-0-$i Ready <none> 1m v1.34"; i=$((i+1)); done;;
  *" get nodes -o jsonpath={range .items[*]}{.metadata.name}{\"=\"}"*) echo "ip-10-0-0-1=True";;
  *ExternalIP*) echo 54.0.0.1;;
  *" get nodes -o jsonpath"*) n=$(nodes_of); i=1; while [ "$i" -le "$n" ]; do echo "ip-10-0-0-$i"; i=$((i+1)); done;;
  *" get secret "*) printf dG9rZW4=;;
  *"config view"*"server}"*) echo https://fake.eks.example;;
  *"config view"*) printf Y2E=;;
  *" get daemonset aws-node"*) [ -n "${FAKE_NO_NETPOL:-}" ] || echo "[--enable-network-policy=true]";;
  *" get namespace kube-system"*) echo 0b1c-uid;;
  *" get pods -l app.kubernetes.io/name=sdo-lane-node-agent"*) echo "agent-1 agent-2";;
  *"create configmap sdo-lane-prepull"*) printf 'kind: ConfigMap\nname: sdo-lane-prepull\n%s\n' "$*" ;;
  *" apply -f -"*) cat >> "$S/applied.yaml";;
  esac;;
docker) :;;
esac
exit 0
