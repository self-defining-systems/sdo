#!/bin/sh
# Stand-in aws, eksctl, kubectl and docker for the EKS lane tests (deploy/eks-lane.sh). State in $FAKE_STATE:
# cluster-<name> (exists), cfg-<name> (the eksctl config it was made from), eks.log (every call).
tool=$1; shift
S=${FAKE_STATE:?}
echo "$tool $*" >> "$S/eks.log"
name_of() { sed -n 's/^  name: //p' "$1" | head -1; }
val() { k=$1; shift; while [ $# -gt 0 ]; do [ "$1" = "$k" ] && { echo "$2"; return; }; shift; done; }
nodes_of() {
  g=$(ls "$S"/ng-* 2>/dev/null | head -1)
  if [ -n "$g" ]; then n=$(sed -n 's/.*desiredSize=\([0-9]*\).*/\1/p' "$g"); else c=$(ls "$S"/cfg-* 2>/dev/null | head -1); n=$(sed -n 's/.*desiredCapacity: //p' "$c"); fi
  echo "${n:-2}"
}
case $tool in
aws)
  case "$1 $2" in
  "sts get-caller-identity") echo "${FAKE_AWS_ACCOUNT:-123456789012}";;
  "ecr get-login-password") echo tok;;
  "ecr describe-repositories") [ -e "$S/repo-$(echo "$*" | sed 's/.*--repository-names //' | tr '/' '_')" ] || { echo "RepositoryNotFoundException" >&2; exit 254; };;
  "ecr create-repository") touch "$S/repo-$(echo "$*" | sed 's/.*--repository-name \([^ ]*\).*/\1/' | tr '/' '_')";;
  "ecr describe-images") echo "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";;
  "eks update-kubeconfig") echo "apiVersion: v1" > "$(echo "$*" | sed 's/.*--kubeconfig //')";;
  "ec2 describe-subnets") case "$*" in *--filters*) echo subnet-0a;; *) echo us-west-2a;; esac;;
  "eks describe-nodegroup")
    case "$*" in
    *nodeRole*) echo arn:aws:iam::123456789012:role/eksctl-node-role;;
    *) cat "$S/desired" 2>/dev/null || nodes_of;;
    esac;;
  "ec2 create-tags") :;;
  "eks describe-cluster")
    case "$*" in
    *resourcesVpcConfig.vpcId*) echo vpc-0123;;
    *clusterSecurityGroupId*) echo sg-0123;;
    *) n=$(echo "$*" | sed 's/.*--name \([^ ]*\).*/\1/'); [ -n "${FAKE_AWS_DOWN:-}" ] && { echo "Could not connect to the endpoint URL" >&2; exit 255; }
       [ -e "$S/cluster-$n" ] || { echo "An error occurred (ResourceNotFoundException) when calling the DescribeCluster operation: No cluster found for name: $n." >&2; exit 254; }
       echo ACTIVE;;
    esac;;
  "ec2 authorize-security-group-ingress") :;;
  "cloudformation describe-stacks")
    st=$(val --stack-name "$@"); [ -e "$S/stack-$st" ] || { echo "An error occurred (ValidationError) when calling the DescribeStacks operation: Stack with id $st does not exist" >&2; exit 254; }
    case "$*" in
    *StackStatus*) cat "$S/stack-$st";;
    *Outputs*) printf 'VpcId\tvpc-0aaa\nSubnetA\tsubnet-0a\nSubnetB\tsubnet-0b\nClusterRoleArn\tarn:aws:iam::123456789012:role/shared-cluster\nNodeRoleArn\tarn:aws:iam::123456789012:role/shared-node\nNodeLaunchTemplateId\tlt-0abc\n';;
    esac;;
  "cloudformation create-stack")
    st=$(val --stack-name "$@")
    echo CREATE_IN_PROGRESS > "$S/tmp-$$"
    ln "$S/tmp-$$" "$S/stack-$st" 2>/dev/null || { rm -f "$S/tmp-$$"; echo "An error occurred (AlreadyExistsException) when calling the CreateStack operation: Stack [$st] already exists" >&2; exit 254; }
    rm -f "$S/tmp-$$"; echo "$*" >> "$S/stack-args";;
  "cloudformation wait")
    st=$(val --stack-name "$@"); case "$*" in
    *delete-complete*) :;;
    *) [ -z "${FAKE_STACK_SECONDS:-}" ] || sleep "$FAKE_STACK_SECONDS"; echo CREATE_COMPLETE > "$S/stack-$st";; esac;;
  "cloudformation delete-stack") st=$(val --stack-name "$@"); rm -rf "$S/stack-$st";;
  "eks list-clusters") for c in "$S"/cluster-*; do [ -e "$c" ] && echo "${c##*/cluster-}"; done;;
  "eks create-cluster")
    n=$(val --name "$@"); echo "$*" > "$S/clargs-$n"
    echo "create-start $n $(date +%s.%N)" >> "$S/eks-times.log"
    [ -z "${FAKE_EKS_CREATE_SECONDS:-}" ] || sleep "$FAKE_EKS_CREATE_SECONDS"
    echo "create-end $n $(date +%s.%N)" >> "$S/eks-times.log"
    touch "$S/cluster-$n"
    [ -z "${FAKE_EKS_FAIL_CREATE:-}" ] || { echo "create-cluster failed" >&2; exit 1; };;
  "eks wait") :;;
  "eks create-nodegroup") n=$(val --cluster-name "$@"); echo "$*" > "$S/ng-$n";;
  "eks create-addon") echo "$*" >> "$S/addons";;
  "eks describe-addon") echo ACTIVE;;
  "eks list-nodegroups") n=$(val --cluster-name "$@"); [ -e "$S/ng-$n" ] && echo workers;;
  "eks delete-nodegroup") rm -f "$S/ng-$(val --cluster-name "$@")";;
  "eks delete-cluster") rm -f "$S/cluster-$(val --name "$@")";;
  "eks update-nodegroup-config") echo "$(val --scaling-config "$@" | sed 's/.*desiredSize=\([0-9]*\).*/\1/')" > "$S/desired";;
  esac;;
eksctl)
  case "$1 $2" in
  "get cluster") [ -e "$S/cluster-$4" ] || exit 1;;
  "create cluster")
    cfg=$4; n=$(name_of "$cfg"); kc=$6
    echo "create-start $n $(date +%s.%N)" >> "$S/eks-times.log"
    [ -z "${FAKE_EKS_CREATE_SECONDS:-}" ] || sleep "$FAKE_EKS_CREATE_SECONDS"
    echo "create-end $n $(date +%s.%N)" >> "$S/eks-times.log"
    cp "$cfg" "$S/cfg-$n"; touch "$S/cluster-$n" "$S/stack-eksctl-$n-cluster"; echo CREATE_COMPLETE > "$S/stack-eksctl-$n-cluster"
    echo "apiVersion: v1" > "$kc"
    [ -z "${FAKE_EKS_FAIL_CREATE:-}" ] || { echo "eksctl: stack failed" >&2; exit 1; };;
  "delete cluster") rm -f "$S/cluster-$4" "$S/stack-eksctl-$4-cluster";;
  "scale nodegroup") echo "$(echo "$*" | sed 's/.*--nodes \([0-9]*\).*/\1/')" > "$S/desired";;
  esac;;
helm) :;;
vcluster)
  # vcluster create NAME -n NS --values FILE ...; connect NAME ... --server S --kube-config-context-name CTX; delete NAME
  case "$1" in
  create) [ -z "${FAKE_VCLUSTER_FAIL:-}" ] || { echo "vcluster: create failed" >&2; exit 1; }
    touch "$S/ns-$2"; v=$(echo "$*" | sed 's/.*--values \([^ ]*\).*/\1/'); cp "$v" "$S/values-$2";;
  connect)
    srv=$(echo "$*" | sed 's/.*--server \([^ ]*\).*/\1/'); ctx=$(echo "$*" | sed 's/.*--kube-config-context-name \([^ ]*\).*/\1/')
    printf 'apiVersion: v1\nkind: Config\nclusters:\n- name: %s\n  cluster: {server: %s}\ncontexts:\n- name: %s\n  context: {cluster: %s}\ncurrent-context: %s\n' "$2" "$srv" "$ctx" "$2" "$ctx";;
  delete) rm -f "$S/ns-$2";;
  esac;;
kubectl)
  # the host cluster of lanes that are vclusters: namespaces, Karpenter NodePools, the nodes they make
  case " $* " in
  *" get namespace "*" -o name"*) ns=$(echo "$*" | sed 's/.* get namespace \([^ ]*\).*/\1/')
    [ -e "$S/ns-$ns" ] || { [ -n "${FAKE_KUBE_DOWN:-}" ] && { echo "Unable to connect to the server" >&2; exit 1; }; echo "Error from server (NotFound): namespaces \"$ns\" not found" >&2; exit 1; }
    echo "namespace/$ns"; exit 0;;
  *" get statefulset "*) ns=$(echo "$*" | sed 's/.* get statefulset \([^ ]*\).*/\1/'); [ -e "$S/ns-$ns" ] || exit 1; exit 0;;
  *" get svc "*nodePort*) echo 31234; exit 0;;
  *" get nodes -l sdo-role=system --no-headers"*) [ "$(cat "$S/desired" 2>/dev/null || echo 1)" = 0 ] || echo "ip-system Ready <none> 1m v1.34"; exit 0;;
  *" get nodes -l sdo-lane="*--no-headers*) lane=$(echo "$*" | head -1 | sed 's/.*sdo-lane=\([^ ]*\).*/\1/')
    n=$(cat "$S/lane-nodes-$lane" 2>/dev/null || echo 0); i=1; while [ "$i" -le "$n" ]; do echo "ip-$lane-$i Ready <none> 1m v1.34"; i=$((i+1)); done; exit 0;;
  *" get nodes -l sdo-lane="*jsonpath*{.metadata.name}*) lane=$(echo "$*" | head -1 | sed 's/.*sdo-lane=\([^ ]*\).*/\1/')
    n=$(cat "$S/lane-nodes-$lane" 2>/dev/null || echo 0); i=1; while [ "$i" -le "$n" ]; do echo "ip-$lane-$i"; i=$((i+1)); done; exit 0;;
  *" get nodepool "*) ls "$S" | grep '^nodepool-' | sed 's/^nodepool-/lane-/'; exit 0;;
  *" delete nodepool "*) np=$(echo "$*" | sed 's/.* delete nodepool \([^ ]*\).*/\1/'); rm -f "$S/nodepool-${np#lane-}" "$S/lane-nodes-${np#lane-}"; exit 0;;
  *" get configmap sdo-lane-prepull "*) cat "$S/prepull-images" 2>/dev/null; exit 0;;
  *"create configmap sdo-lane-prepull --from-file="*) f=$(echo "$*" | sed 's/.*--from-file=images=\([^ ]*\).*/\1/'); cp "$f" "$S/prepull-images"
    printf 'kind: ConfigMap\nname: sdo-lane-prepull\n'; cat "$f"; exit 0;;
  *" apply -f - "*)
    y=$(cat); printf '%s\n' "$y" >> "$S/applied.yaml"
    # Karpenter: a NodePool named lane-X plus the capacity deployment of replicas N brings N nodes
    for np in $(printf '%s\n' "$y" | sed -n 's/^metadata: {name: lane-\([^,}]*\),.*/\1/p'); do
      touch "$S/nodepool-$np"; n=$(printf '%s\n' "$y" | sed -n 's/^  replicas: //p' | head -1); echo "${n:-0}" > "$S/lane-nodes-$np"
    done
    exit 0;;
  esac
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
