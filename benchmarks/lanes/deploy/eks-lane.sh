#!/usr/bin/env bash
# One lane as an Amazon EKS cluster; the provider `eks` of lanes. Runs on the manager's machine (eksctl, aws, kubectl,
# docker on PATH, AWS credentials in the environment); there is no ssh host.
#
#   eks-lane.sh up NAME KUBECONFIG [EVIDENCE_DIR]   # create (idempotent: an existing cluster is attached) and prepare
#   eks-lane.sh down NAME [KUBECONFIG]              # delete the cluster, its node group and the kubeconfig
#   eks-lane.sh push IMAGE...                       # make nodes resolve each local image under its declared name (ECR mirror)
#   eks-lane.sh pin IMAGE...                        # push a pinned image to ECR; prints <registry>/<repo>@sha256:...
#   eks-lane.sh nodes KUBECONFIG SCRIPT             # run sh -c SCRIPT as root on every node (certs.d at /certs, /var/lib/sdo-lane at /lane)
#   eks-lane.sh exists NAME                         # exit 0 the cluster exists, 1 it does not, 2 cannot tell
#   eks-lane.sh fence KUBECONFIG                    # print "<api server> <kube-system uid>" the kubeconfig reaches
#
# What `up` makes (the cross-cutting items of an EKS lane; each was a failure or a finding of the first lane):
#  - cluster NAME_PREFIX+NAME (default sdo-lane-NAME), STANDARD support, add-ons = what a kind lane has: vpc-cni with the
#    NetworkPolicy agent enabled (CC3), coredns, kube-proxy, metrics-server, pod identity. Nothing else: SDO admission
#    rejects foreign namespaces.
#  - a managed node group of EKS_NODES x EKS_INSTANCE_TYPE, scalable 0..EKS_NODE_MAX, labels: the first node is the
#    infra node (node-role.kubernetes.io/control-plane), every other node, or the only one, is a worker
#    (node-role.kubernetes.io/worker, CC5).
#  - kubeconfig: a service-account token (no aws binary or expiring exec credential needed by what uses it), the
#    context named kind-<lane> because workloads derive the context from the lane name. The admin kubeconfig stays at
#    KUBECONFIG.admin (0600).
#  - the node agent (deploy/eks/node-agent.yaml): a containerd registry mirror to ECR for EKS_MIRROR_UPSTREAMS (default
#    "docker.io ghcr.io quay.io registry.k8s.io") so app images keep their declared names (CC1, CC9), and the node-write seam (CC4).
#  - on any failure after the cluster was created the half-built cluster is deleted, so nothing bills.
#  - one availability zone (EKS_AZ, default <region>a); gp3 (EBS CSI) as the default StorageClass with
#    WaitForFirstConsumer; the node-port range open to EKS_NODEPORT_CIDR (default this machine's address); the address
#    of a node in KUBECONFIG.entry (the app's NodePort entry; steps see it as FUZZOPS_ENTRY_HOST).
# Settings: EKS_REGION (required), EKS_INSTANCE_TYPE (m6i.xlarge), EKS_NODES (2), EKS_NODE_MAX (EKS_NODES+1), EKS_ACCOUNT
# (checked against the credentials), EKS_VPC and EKS_SUBNETS (comma-separated subnet ids; unset: eksctl makes a VPC),
# EKS_VERSION (1.34), EKS_NAME_PREFIX (sdo-lane-), EKS_TAG_PROJECT (sdo-lanes), EKS_MIRROR_PREFIX (sdo-mirror),
# EKS_IMAGE_PREFIX (sdo), EKS_MIRROR_UPSTREAMS.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
log() { echo "[eks-lane] $*" >&2; }
die() { log "$*"; exit 1; }

REGION=${EKS_REGION:-${AWS_REGION:-}}
[ -n "$REGION" ] || die "EKS_REGION is required"
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION
PREFIX=${EKS_NAME_PREFIX:-sdo-lane-}
MIRROR_PREFIX=${EKS_MIRROR_PREFIX:-sdo-mirror}
IMAGE_PREFIX=${EKS_IMAGE_PREFIX:-sdo}
UPSTREAMS=${EKS_MIRROR_UPSTREAMS:-docker.io ghcr.io quay.io registry.k8s.io}

account() {
  local a
  a=$(aws sts get-caller-identity --query Account --output text) || die "no usable AWS credentials"
  if [ -n "${EKS_ACCOUNT:-}" ] && [ "$a" != "$EKS_ACCOUNT" ]; then die "credentials belong to account $a, expected $EKS_ACCOUNT"; fi
  echo "$a"
}
registry() { echo "$(account).dkr.ecr.$REGION.amazonaws.com"; }

cluster_exists() { eksctl get cluster --name "$1" --region "$REGION" >/dev/null 2>&1; }

# AZ: every lane is in one zone (a volume is zonal, and a lane's claims must land next to its pods).
AZ=${EKS_AZ:-${REGION}a}

write_config() {
  local name=$1 f=$2
  {
    cat <<YAML
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: $name
  region: $REGION
  version: "${EKS_VERSION:-1.34}"
  tags: {Project: ${EKS_TAG_PROJECT:-sdo-lanes}, sdo-lane: $name}
upgradePolicy: {supportType: STANDARD}
YAML
    if [ -n "${EKS_VPC:-}" ]; then
      echo "vpc:"
      echo "  id: $EKS_VPC"
      echo "  subnets:"
      echo "    public:"
      local s az
      for s in $(echo "${EKS_SUBNETS:-}" | tr ',' ' '); do
        az=$(aws ec2 describe-subnets --subnet-ids "$s" --query 'Subnets[0].AvailabilityZone' --output text)
        echo "      $az: {id: $s}"
      done
      echo "  clusterEndpoints: {publicAccess: true, privateAccess: true}"
    fi
    cat <<YAML
accessConfig: {authenticationMode: API_AND_CONFIG_MAP, bootstrapClusterCreatorAdminPermissions: true}
addons:
  - {name: vpc-cni, configurationValues: '{"enableNetworkPolicy":"true"}', resolveConflicts: overwrite}
  - {name: coredns}
  - {name: kube-proxy}
  - {name: eks-pod-identity-agent}
  - {name: metrics-server}
  - {name: aws-ebs-csi-driver}
managedNodeGroups:
  - name: workers
    instanceType: ${EKS_INSTANCE_TYPE:-m6i.xlarge}
    desiredCapacity: ${EKS_NODES:-2}
    availabilityZones: [$AZ]
    minSize: 0
    maxSize: ${EKS_NODE_MAX:-$((${EKS_NODES:-2} + 1))}
    volumeSize: 100
    volumeType: gp3
    privateNetworking: false
    tags: {Project: ${EKS_TAG_PROJECT:-sdo-lanes}, sdo-lane: $name}
    iam:
      attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
        - arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy
YAML
  } >"$f"
}

# label_nodes: the first node is the infra node, the rest (or the only one) are workers (CC5).
label_nodes() {
  local kc=$1 nodes first n i=0
  nodes=$(kubectl --kubeconfig "$kc" get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  first=$(echo "$nodes" | head -1)
  for n in $nodes; do
    if [ "$n" = "$first" ]; then kubectl --kubeconfig "$kc" label node "$n" node-role.kubernetes.io/control-plane= --overwrite >/dev/null; fi
    if [ "$n" != "$first" ] || [ "$(echo "$nodes" | wc -l)" -eq 1 ]; then kubectl --kubeconfig "$kc" label node "$n" node-role.kubernetes.io/worker= --overwrite >/dev/null; fi
    i=$((i + 1))
  done
}

wait_nodes() {
  local kc=$1 want=$2 end=$((SECONDS + 600))
  until [ "$(kubectl --kubeconfig "$kc" get nodes --no-headers 2>/dev/null | grep -c ' Ready')" -ge "$want" ]; do
    [ $SECONDS -lt $end ] || die "fewer than $want Ready nodes after 10 minutes"
    sleep 10
  done
}

# token_kubeconfig ADMIN OUT NAME LANE: a kubeconfig of a cluster-admin service account; its context is kind-LANE.
token_kubeconfig() {
  local admin=$1 out=$2 name=$3 lane=$4
  kubectl --kubeconfig "$admin" -n kube-system create serviceaccount sdo-lane-admin --dry-run=client -o yaml | kubectl --kubeconfig "$admin" apply -f - >/dev/null
  kubectl --kubeconfig "$admin" create clusterrolebinding sdo-lane-admin --clusterrole=cluster-admin --serviceaccount=kube-system:sdo-lane-admin --dry-run=client -o yaml | kubectl --kubeconfig "$admin" apply -f - >/dev/null
  kubectl --kubeconfig "$admin" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Secret
metadata:
  name: sdo-lane-admin-token
  namespace: kube-system
  annotations: {kubernetes.io/service-account.name: sdo-lane-admin}
type: kubernetes.io/service-account-token
YAML
  local token="" end=$((SECONDS + 60))
  until [ -n "$token" ]; do
    token=$(kubectl --kubeconfig "$admin" -n kube-system get secret sdo-lane-admin-token -o jsonpath='{.data.token}' | base64 -d 2>/dev/null || true)
    [ -n "$token" ] || { [ $SECONDS -lt $end ] || die "no service account token after 60 s"; sleep 2; }
  done
  local server ca
  server=$(kubectl --kubeconfig "$admin" config view --raw --minify -o jsonpath='{.clusters[0].cluster.server}')
  ca=$(kubectl --kubeconfig "$admin" config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
  umask 077
  cat >"$out.tmp" <<YAML
apiVersion: v1
kind: Config
clusters:
- name: $name
  cluster: {server: $server, certificate-authority-data: $ca}
users:
- name: sdo-lane-admin
  user: {token: $token}
contexts:
- name: kind-$lane
  context: {cluster: $name, user: sdo-lane-admin}
current-context: kind-$lane
YAML
  mv "$out.tmp" "$out"
}

# storage_class: gp3 through the EBS CSI driver is the default class and binds when a pod is placed (WaitForFirstConsumer),
# so a claim is made in the zone of the node that uses it.
storage_class() {
  local kc=$1
  kubectl --kubeconfig "$kc" annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null 2>&1 || true
  kubectl --kubeconfig "$kc" apply -f - >/dev/null <<YAML
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations: {storageclass.kubernetes.io/is-default-class: "true"}
provisioner: ebs.csi.aws.com
parameters: {type: gp3}
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
allowVolumeExpansion: true
YAML
}

# open_nodeports NAME: the manager reaches the app's NodePorts on the nodes: the cluster security group (which managed
# nodes use) admits tcp 30000-32767 from EKS_NODEPORT_CIDR (default: this machine's public address).
open_nodeports() {
  local name=$1 sg cidr=${EKS_NODEPORT_CIDR:-}
  if [ -z "$cidr" ]; then cidr="$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')/32"; fi
  sg=$(aws eks describe-cluster --name "$name" --region "$REGION" --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' --output text)
  aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port 30000-32767 --cidr "$cidr" >/dev/null 2>&1 || true # already open
}

# entry_host KUBECONFIG: an address of a Ready node the manager reaches (ExternalIP, else InternalIP), kept in KUBECONFIG.entry.
entry_host() {
  local kc=$1 ip
  ip=$(kubectl --kubeconfig "$kc" get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
  [ -n "$ip" ] || ip=$(kubectl --kubeconfig "$kc" get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
  [ -n "$ip" ] || die "no node address for the entry host"
  echo "$ip" >"$kc.entry"
}

apply_node_agent() {
  local kc=$1 ecr
  ecr=$(registry)
  sed -e "s|@ECR@|$ecr|g" -e "s|@REGION@|$REGION|g" -e "s|@PREFIX@|$MIRROR_PREFIX|g" -e "s|@UPSTREAMS@|$UPSTREAMS|g" "$here/eks/node-agent.yaml" |
    kubectl --kubeconfig "$kc" apply -f - >/dev/null
  kubectl --kubeconfig "$kc" -n kube-system rollout status daemonset/sdo-lane-node-agent --timeout=300s >/dev/null || die "the node agent is not ready on every node"
}

network_policy_enforced() {
  kubectl --kubeconfig "$1" -n kube-system get daemonset aws-node -o jsonpath='{.spec.template.spec.containers[*].args}' | grep -- '--enable-network-policy=true' >/dev/null
}

# cleanup_failed_up: a cluster this `up` made and could not finish is deleted, so nothing bills.
cleanup_failed_up() {
  local rc=$?
  if [ $rc -ne 0 ] && [ "${CREATED:-0}" = 1 ]; then
    log "cluster-up failed: deleting the half-built cluster $CNAME"
    eksctl delete cluster --name "$CNAME" --region "$REGION" --wait --disable-nodegroup-eviction >&2 || log "DELETE FAILED: $CNAME may still bill"
  fi
  [ -z "${CFG:-}" ] || rm -f "$CFG"
}

cmd_up() {
  local lane=${1:?lane} kc=${2:?kubeconfig} evidence=${3:-}
  local name=$PREFIX$lane admin="$kc.admin"
  CREATED=0 CNAME=$name CFG=
  account >/dev/null
  mkdir -p "$(dirname "$kc")"
  [ -z "$evidence" ] || mkdir -p "$evidence"
  if cluster_exists "$name"; then
    log "cluster $name exists: attaching"
  else
    CFG=$(mktemp)
    write_config "$name" "$CFG"
    CREATED=1
    log "creating cluster $name (${EKS_NODES:-2} x ${EKS_INSTANCE_TYPE:-m6i.xlarge})"
    trap cleanup_failed_up EXIT
    eksctl create cluster -f "$CFG" --kubeconfig "$admin" >&2
  fi
  [ "$CREATED" = 1 ] || aws eks update-kubeconfig --name "$name" --region "$REGION" --kubeconfig "$admin" >/dev/null
  chmod 600 "$admin"
  wait_nodes "$admin" "${EKS_NODES:-2}"
  network_policy_enforced "$admin" || die "the cluster does not enforce NetworkPolicy: vpc-cni needs enableNetworkPolicy=true"
  label_nodes "$admin"
  kubectl --kubeconfig "$admin" -n kube-system delete pdb coredns metrics-server --ignore-not-found >/dev/null 2>&1 || true
  token_kubeconfig "$admin" "$kc" "$name" "$lane"
  storage_class "$kc"
  open_nodeports "$name"
  entry_host "$kc"
  apply_node_agent "$kc"
  echo "EKS_LANE_READY cluster=$name instance=${EKS_INSTANCE_TYPE:-m6i.xlarge} nodes=${EKS_NODES:-2}"
}

cmd_down() {
  local lane=${1:?lane} kc=${2:-}
  local name=$PREFIX$lane t0=$SECONDS
  if cluster_exists "$name"; then
    [ -z "$kc" ] || [ ! -f "$kc.admin" ] || kubectl --kubeconfig "$kc.admin" delete pvc --all-namespaces --all --wait=false >/dev/null 2>&1 || true
    eksctl delete cluster --name "$name" --region "$REGION" --wait --disable-nodegroup-eviction >&2
  else
    log "cluster $name does not exist"
  fi
  [ -z "$kc" ] || rm -f "$kc" "$kc.admin" "$kc.entry"
  echo "EKS_LANE_DELETED cluster=$name seconds=$((SECONDS - t0))"
}

# split_ref IMAGE: "<registry/path> <tag>" with Docker's implicit registry and library/ spelled out.
split_ref() {
  local image=$1 first full name tag
  case $image in *@*) die "$image: a digest reference cannot be published under a declared name";; esac
  first=${image%%/*}
  if [[ $image != */* ]]; then full=docker.io/library/$image
  elif [[ $first != *.* && $first != *:* && $first != localhost ]]; then full=docker.io/$image
  else full=$image; fi
  name=${full%:*}; tag=${full##*:}
  if [[ $full != *:* || $tag == */* ]]; then name=$full; tag=latest; fi
  echo "$name $tag"
}

ecr_login() {
  aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$(registry)" >/dev/null
}

ecr_push() { # LOCAL REPOSITORY TAG -> digest
  local local_img=$1 repo=$2 tag=$3 ecr
  ecr=$(registry)
  aws ecr describe-repositories --region "$REGION" --repository-names "$repo" >/dev/null 2>&1 ||
    aws ecr create-repository --region "$REGION" --repository-name "$repo" --image-scanning-configuration scanOnPush=false \
      --tags "Key=Project,Value=${EKS_TAG_PROJECT:-sdo-lanes}" >/dev/null
  docker tag "$local_img" "$ecr/$repo:$tag"
  docker push --quiet "$ecr/$repo:$tag" >/dev/null
  aws ecr describe-images --region "$REGION" --repository-name "$repo" --image-ids "imageTag=$tag" --query 'imageDetails[0].imageDigest' --output text
}

cmd_push() {
  [ $# -gt 0 ] || die "push IMAGE..."
  ecr_login
  local img name tag up digest
  for img in "$@"; do
    read -r name tag <<<"$(split_ref "$img")"
    up=${name%%/*}
    case " $UPSTREAMS " in *" $up "*) ;; *) die "$img: the lane mirrors [$UPSTREAMS], not $up";; esac
    digest=$(ecr_push "$img" "$MIRROR_PREFIX/$name" "$tag")
    echo "pushed $img -> $MIRROR_PREFIX/$name:$tag $digest"
  done
}

cmd_pin() {
  [ $# -gt 0 ] || die "pin IMAGE..."
  ecr_login
  local img name tag digest
  for img in "$@"; do
    read -r name tag <<<"$(split_ref "$img")"
    digest=$(ecr_push "$img" "$IMAGE_PREFIX/${name##*/}" "$tag")
    echo "$(registry)/$IMAGE_PREFIX/${name##*/}@$digest"
  done
}

cmd_nodes() {
  local kc=${1:?kubeconfig} script=${2:?script} pods pod
  pods=$(kubectl --kubeconfig "$kc" -n kube-system get pods -l app.kubernetes.io/name=sdo-lane-node-agent -o jsonpath='{.items[*].metadata.name}')
  [ -n "$pods" ] || die "no node agent pod"
  for pod in $pods; do
    kubectl --kubeconfig "$kc" -n kube-system exec -i "$pod" -c agent -- sh -c "$script"
  done
}

# cmd_exists LANE: exit 0 when the lane's cluster exists, 1 when AWS says it does not, 2 when it cannot be told (a failed
# call is never "gone": the manager forgets a lane it believes is gone, and its cluster would bill on).
cmd_exists() {
  local name=$PREFIX${1:?lane} err
  if err=$(aws eks describe-cluster --name "$name" --region "$REGION" --query cluster.status --output text 2>&1); then
    exit 0
  fi
  case $err in *ResourceNotFoundException*) exit 1;; esac
  log "cannot tell whether $name exists: ${err: -200}"
  exit 2
}

cmd_fence() {
  local kc=${1:?kubeconfig}
  echo "$(kubectl --kubeconfig "$kc" config view --minify -o jsonpath='{.clusters[0].cluster.server}') $(kubectl --kubeconfig "$kc" get namespace kube-system -o jsonpath='{.metadata.uid}')"
}

verb=${1:-}
[ $# -eq 0 ] || shift
case $verb in
  up) cmd_up "$@";;
  down) cmd_down "$@";;
  push) cmd_push "$@";;
  pin) cmd_pin "$@";;
  nodes) cmd_nodes "$@";;
  fence) cmd_fence "$@";;
  exists) cmd_exists "$@";;
  *) echo "usage: $0 up|down|push|pin|nodes|fence ..." >&2; exit 2;;
esac
