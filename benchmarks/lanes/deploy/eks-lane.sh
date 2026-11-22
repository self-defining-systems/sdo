#!/usr/bin/env bash
# One lane as an Amazon EKS cluster; the provider `eks` of lanes. Runs on the manager's machine (eksctl, aws, kubectl,
# docker on PATH, AWS credentials in the environment); there is no ssh host.
#
#   eks-lane.sh up NAME KUBECONFIG [EVIDENCE_DIR]   # create (idempotent: an existing cluster is attached) and prepare
#   eks-lane.sh down NAME [KUBECONFIG]              # delete the cluster, its node group and the kubeconfig
#   eks-lane.sh push IMAGE...                       # make nodes resolve each local image under its declared name (ECR mirror)
#   eks-lane.sh pin IMAGE...                        # push a pinned image to ECR; prints <registry>/<repo>@sha256:...
#   eks-lane.sh prepull KUBECONFIG IMAGE...         # the node agent pre-pulls these on every node, now and on node join
#   eks-lane.sh nodes KUBECONFIG SCRIPT             # run sh -c SCRIPT as root on every node (certs.d at /certs, /var/lib/sdo-lane at /lane)
#   eks-lane.sh exists NAME                         # exit 0 the cluster exists, 1 it does not, 2 cannot tell
#   eks-lane.sh host-setup                          # make the account's shared network and roles (once per account and region, idempotent)
#   eks-lane.sh host-teardown                       # remove them (refused while a lane cluster exists)
#   eks-lane.sh fence KUBECONFIG                    # print "<api server> <kube-system uid>" the kubeconfig reaches
#
# The VPC, two public subnets, the cluster and node IAM roles and the node launch template (gp3 root volume) are not made per
# lane: `host-setup` makes them once as the CloudFormation stack EKS_SHARED_STACK (default sdo-lanes-shared), `up` makes it
# first when it is missing, and records the ids in EKS_STATE_DIR/shared-<region>.env when that is set. A lane then makes
# only its control plane and its node group (direct aws eks calls), and `down` deletes only those; `host-teardown` removes
# the shared stack. A cluster an earlier version made with eksctl is still attached and deleted with eksctl.
#
# What `up` makes (the cross-cutting items of an EKS lane; each was a failure or a finding of the first lane):
#  - cluster NAME_PREFIX+NAME (default sdo-lane-NAME), STANDARD support, add-ons = what a kind lane has: vpc-cni with the
#    NetworkPolicy agent enabled (CC3), coredns, kube-proxy, metrics-server, pod identity, EBS CSI. Nothing else: SDO admission
#    rejects foreign namespaces. The add-ons are created while the nodes boot, and `up` waits for their pods, not for the
#    add-on API status, which lags them by minutes.
#  - a managed node group of EKS_NODES x EKS_INSTANCE_TYPE, scalable 0..EKS_NODE_MAX, labels: the first node is the
#    infra node (node-role.kubernetes.io/control-plane), every other node, or the only one, is a worker
#    (node-role.kubernetes.io/worker, CC5).
#  - kubeconfig: a service-account token (no aws binary or expiring exec credential needed by what uses it), the
#    context named kind-<lane> because workloads derive the context from the lane name. The admin kubeconfig stays at
#    KUBECONFIG.admin (0600).
#  - the node agent (deploy/eks/node-agent.yaml): a containerd registry mirror to ECR for EKS_MIRROR_UPSTREAMS (default
#    "docker.io ghcr.io quay.io registry.k8s.io") so app images keep their declared names (CC1, CC9), and the node-write seam (CC4).
#  - on any failure after the cluster was created the half-built cluster is deleted, so nothing bills.
#  - one availability zone (EKS_AZ, default <region>a); local-path (node-local, WaitForFirstConsumer) as the
#    default StorageClass, gp3 (EBS CSI) available; the node-port range open to EKS_NODEPORT_CIDR (default this machine's address); the address
#    of a node in KUBECONFIG.entry (the app's NodePort entry; steps see it as FUZZOPS_ENTRY_HOST).
# Settings: EKS_REGION (required), EKS_INSTANCE_TYPE (m6i.xlarge), EKS_NODES (2), EKS_NODE_MAX (EKS_NODES+1), EKS_ACCOUNT
# (checked against the credentials), EKS_SHARED_STACK (sdo-lanes-shared), EKS_STATE_DIR (where host-setup records the ids),
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

# stage NAME: a measured stage of `up`, seconds since the up began (cumulative), on stderr and in KUBECONFIG.stages
# (one "name seconds" line each) so the run record can carry them.
STAGE_T0=$SECONDS
stage() {
  echo "EKS_STAGE $1 $((SECONDS - STAGE_T0))" >&2
  [ -z "${STAGE_FILE:-}" ] || echo "$1 $((SECONDS - STAGE_T0))" >>"$STAGE_FILE"
}

cluster_exists() { aws eks describe-cluster --name "$1" --region "$REGION" >/dev/null 2>&1; }

SHARED_STACK=${EKS_SHARED_STACK:-sdo-lanes-shared}
TAG_PROJECT=${EKS_TAG_PROJECT:-sdo-lanes}

# AZ: every lane is in one zone (a volume is zonal, and a lane's claims must land next to its pods).
AZ=${EKS_AZ:-${REGION}a}

stack_status() {
  aws cloudformation describe-stacks --stack-name "$SHARED_STACK" --query 'Stacks[0].StackStatus' --output text 2>/dev/null || true
}

shared_wait() {
  aws cloudformation wait stack-create-complete --stack-name "$SHARED_STACK" >&2 ||
    die "the shared stack $SHARED_STACK did not complete (see its CloudFormation events); host-teardown removes it"
}

# ensure_shared: the shared stack exists and is complete (made here when missing; a concurrent up that lost the race waits for it).
ensure_shared() {
  local st err
  st=$(stack_status)
  case $st in
    CREATE_COMPLETE | UPDATE_COMPLETE) ;;
    CREATE_IN_PROGRESS) shared_wait ;;
    "")
      log "making the shared network and roles ($SHARED_STACK)"
      if err=$(aws cloudformation create-stack --stack-name "$SHARED_STACK" --template-body "file://$here/eks/shared.yaml" \
        --capabilities CAPABILITY_IAM --tags "Key=Project,Value=$TAG_PROJECT" 2>&1 >/dev/null); then
        shared_wait
      else
        st=$(stack_status)
        case $st in CREATE_IN_PROGRESS | CREATE_COMPLETE) shared_wait ;; *) die "creating $SHARED_STACK: ${err: -300}" ;; esac
      fi
      ;;
    *) die "the shared stack $SHARED_STACK is $st: run host-teardown, then host-setup" ;;
  esac
  load_shared
}

# load_shared: SH_* from the stack outputs, and the one subnet of this lane's zone.
load_shared() {
  local out k v
  out=$(aws cloudformation describe-stacks --stack-name "$SHARED_STACK" --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text) ||
    die "cannot read the outputs of $SHARED_STACK"
  while IFS=$'\t' read -r k v; do
    case $k in
      VpcId) SH_VPC=$v ;;
      SubnetA) SH_SUBNET_A=$v ;;
      SubnetB) SH_SUBNET_B=$v ;;
      ClusterRoleArn) SH_CLUSTER_ROLE=$v ;;
      NodeRoleArn) SH_NODE_ROLE=$v ;;
      NodeLaunchTemplateId) SH_NODE_TEMPLATE=$v ;;
      NodeLaunchTemplateVersion) SH_NODE_TEMPLATE_VERSION=$v ;;
    esac
  done <<<"$out"
  [ -n "${SH_VPC:-}" ] && [ -n "${SH_SUBNET_A:-}" ] && [ -n "${SH_SUBNET_B:-}" ] && [ -n "${SH_CLUSTER_ROLE:-}" ] && [ -n "${SH_NODE_ROLE:-}" ] &&
    [ -n "${SH_NODE_TEMPLATE:-}" ] && [ -n "${SH_NODE_TEMPLATE_VERSION:-}" ] || die "the outputs of $SHARED_STACK are incomplete"
  case $AZ in
    "${REGION}a") SH_SUBNET=$SH_SUBNET_A ;;
    "${REGION}b") SH_SUBNET=$SH_SUBNET_B ;;
    *) die "EKS_AZ $AZ: the shared network has subnets in ${REGION}a and ${REGION}b" ;;
  esac
  if [ -n "${EKS_STATE_DIR:-}" ]; then
    mkdir -p "$EKS_STATE_DIR"
    printf 'EKS_SHARED_STACK=%s\nEKS_SHARED_VPC=%s\nEKS_SHARED_SUBNETS=%s,%s\nEKS_SHARED_CLUSTER_ROLE=%s\nEKS_SHARED_NODE_ROLE=%s\nEKS_SHARED_NODE_TEMPLATE=%s\n' \
      "$SHARED_STACK" "$SH_VPC" "$SH_SUBNET_A" "$SH_SUBNET_B" "$SH_CLUSTER_ROLE" "$SH_NODE_ROLE" "$SH_NODE_TEMPLATE" >"$EKS_STATE_DIR/shared-$REGION.env"
  fi
}

cmd_host_setup() {
  account >/dev/null
  ensure_shared
  echo "EKS_SHARED vpc=$SH_VPC subnets=$SH_SUBNET_A,$SH_SUBNET_B cluster_role=$SH_CLUSTER_ROLE node_role=$SH_NODE_ROLE"
}

cmd_host_teardown() {
  account >/dev/null
  local lanes st
  lanes=$(aws eks list-clusters --region "$REGION" --query clusters --output text | tr '\t' '\n' | grep "^$PREFIX" || true)
  [ -z "$lanes" ] || die "lane clusters still exist, delete them first: $(echo $lanes)"
  st=$(stack_status)
  if [ -z "$st" ]; then
    log "no shared stack $SHARED_STACK"
  else
    aws cloudformation delete-stack --stack-name "$SHARED_STACK"
    aws cloudformation wait stack-delete-complete --stack-name "$SHARED_STACK" >&2 || die "deleting $SHARED_STACK did not complete"
  fi
  [ -z "${EKS_STATE_DIR:-}" ] || rm -f "$EKS_STATE_DIR/shared-$REGION.env"
  echo "EKS_SHARED_REMOVED stack=$SHARED_STACK"
}

# create_lane_cluster NAME: the control plane, then the node group and the add-ons together (the add-ons need nodes only to
# become active, not to be created), on the shared network and roles.
ADDONS="vpc-cni coredns kube-proxy eks-pod-identity-agent metrics-server aws-ebs-csi-driver"
create_lane_cluster() {
  local name=$1 a
  aws eks create-cluster --name "$name" --region "$REGION" --kubernetes-version "${EKS_VERSION:-1.34}" --role-arn "$SH_CLUSTER_ROLE" \
    --resources-vpc-config "subnetIds=$SH_SUBNET_A,$SH_SUBNET_B,endpointPublicAccess=true,endpointPrivateAccess=false" \
    --access-config authenticationMode=API_AND_CONFIG_MAP,bootstrapClusterCreatorAdminPermissions=true \
    --upgrade-policy supportType=STANDARD --tags "Project=$TAG_PROJECT,sdo-lane=$name" >/dev/null
  aws eks wait cluster-active --name "$name" --region "$REGION"
  stage control_plane_active
  aws eks create-nodegroup --cluster-name "$name" --nodegroup-name workers --region "$REGION" --subnets "$SH_SUBNET" \
    --node-role "$SH_NODE_ROLE" --instance-types "${EKS_INSTANCE_TYPE:-m6i.xlarge}" --ami-type AL2023_x86_64_STANDARD \
    --scaling-config "minSize=0,maxSize=${EKS_NODE_MAX:-$((${EKS_NODES:-2} + 1))},desiredSize=${EKS_NODES:-2}" \
    --launch-template "id=$SH_NODE_TEMPLATE,version=$SH_NODE_TEMPLATE_VERSION" --tags "Project=$TAG_PROJECT,sdo-lane=$name" >/dev/null
  for a in $ADDONS; do
    if [ "$a" = vpc-cni ]; then
      aws eks create-addon --cluster-name "$name" --region "$REGION" --addon-name vpc-cni --resolve-conflicts OVERWRITE \
        --configuration-values '{"enableNetworkPolicy":"true"}' >/dev/null
    else
      aws eks create-addon --cluster-name "$name" --region "$REGION" --addon-name "$a" --resolve-conflicts OVERWRITE >/dev/null
    fi
  done
  open_nodeports "$name"
  aws eks wait nodegroup-active --cluster-name "$name" --nodegroup-name workers --region "$REGION"
  stage nodegroup_active
  wait_addons "$name" vpc-cni # the NetworkPolicy check below needs it; the other add-ons settle while the rest of up runs
  stage vpc_cni_active
}

# wait_addons NAME [ADDON...]: the add-ons (default all) are ACTIVE (DEGRADED is waited out: its pods run once the nodes join; a failed one is an error).
wait_addons() {
  local name=$1 a st end=$((SECONDS + 600))
  shift
  for a in ${*:-$ADDONS}; do
    while :; do
      st=$(aws eks describe-addon --cluster-name "$name" --addon-name "$a" --region "$REGION" --query addon.status --output text)
      case $st in
        ACTIVE) break ;;
        CREATE_FAILED) die "add-on $a of $name is $st" ;; # DEGRADED is normal until its pods run on the new nodes
      esac
      [ $SECONDS -lt $end ] || die "add-on $a of $name is $st after 10 minutes"
      sleep 5
    done
  done
}

# addons_running KUBECONFIG: the add-on workloads run. The add-on API status (ACTIVE) lags the pods by minutes (measured: 7 min
# after everything runs), so the lane waits for the workloads, not for the status.
addons_running() {
  local kc=$1 d
  for d in coredns metrics-server ebs-csi-controller; do
    kubectl --kubeconfig "$kc" -n kube-system rollout status "deployment/$d" --timeout=300s >/dev/null || die "add-on deployment $d is not running"
  done
}

# delete_cluster NAME: a lane's node group and control plane only; the shared network and roles stay. A cluster eksctl made
# (its own CloudFormation stack) is deleted by eksctl, which owns its stacks.
delete_cluster() {
  local name=$1 g groups
  if aws cloudformation describe-stacks --stack-name "eksctl-$name-cluster" >/dev/null 2>&1; then
    eksctl delete cluster --name "$name" --region "$REGION" --wait --disable-nodegroup-eviction >&2
    return
  fi
  groups=$(aws eks list-nodegroups --cluster-name "$name" --region "$REGION" --query nodegroups --output text | tr '\t' ' ')
  for g in $groups; do
    aws eks wait nodegroup-active --cluster-name "$name" --nodegroup-name "$g" --region "$REGION" >/dev/null 2>&1 || true # one still being made cannot be deleted
    aws eks delete-nodegroup --cluster-name "$name" --nodegroup-name "$g" --region "$REGION" >/dev/null
  done
  for g in $groups; do aws eks wait nodegroup-deleted --cluster-name "$name" --nodegroup-name "$g" --region "$REGION"; done
  aws eks wait cluster-active --name "$name" --region "$REGION" >/dev/null 2>&1 || true
  aws eks delete-cluster --name "$name" --region "$REGION" >/dev/null
  aws eks wait cluster-deleted --name "$name" --region "$REGION"
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

# storage_class: local-path (node-local, binds when a pod is placed) is the default class, as on a kind lane. SDO mounts the
# same ReadWriteOnce claims from several pods (controller, workers, standbys) and relies on the claim to put them on its
# node; an EBS volume attaches to one node and does not place its pods, so a pod on the other node waits forever
# (Multi-Attach). gp3 stays available (not the default) for a workload that wants a durable volume.
storage_class() {
  local kc=$1
  kubectl --kubeconfig "$kc" annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null 2>&1 || true
  kubectl --kubeconfig "$kc" apply -f - >/dev/null <<YAML
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
  annotations: {storageclass.kubernetes.io/is-default-class: "false"}
provisioner: ebs.csi.aws.com
parameters: {type: gp3}
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
allowVolumeExpansion: true
YAML
  kubectl --kubeconfig "$kc" apply -f "$here/eks/local-path-storage.yaml" >/dev/null
  kubectl --kubeconfig "$kc" -n local-path-storage rollout status deployment/local-path-provisioner --timeout=180s >/dev/null ||
    die "the local-path provisioner is not ready"
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

# scale_up_if_needed NAME: an attached cluster whose node group was scaled down (by hand or by an idle policy) gets its nodes back.
scale_up_if_needed() {
  local name=$1 want=${EKS_NODES:-2} have
  have=$(aws eks describe-nodegroup --cluster-name "$name" --nodegroup-name workers --region "$REGION" --query nodegroup.scalingConfig.desiredSize --output text)
  if [ "$have" != "$want" ]; then
    log "node group of $name is at $have, not $want: scaling"
    aws eks update-nodegroup-config --cluster-name "$name" --nodegroup-name workers --region "$REGION" \
      --scaling-config "minSize=0,maxSize=${EKS_NODE_MAX:-$((want + 1))},desiredSize=$want" >/dev/null
    aws eks wait nodegroup-active --cluster-name "$name" --nodegroup-name workers --region "$REGION"
  fi
}

# cleanup_failed_up: a cluster this `up` made or attached and could not finish is deleted, so nothing bills: an up is
# all-or-nothing, and a lane is rebuilt from nothing (about 18 minutes) rather than left half-made.
cleanup_failed_up() {
  local rc=$?
  if [ $rc -ne 0 ] && [ "${OWNED:-0}" = 1 ]; then
    log "cluster-up failed: deleting the half-built cluster $CNAME"
    delete_cluster "$CNAME" >&2 || log "DELETE FAILED: $CNAME may still bill"
  fi
}

cmd_up() {
  local lane=${1:?lane} kc=${2:?kubeconfig} evidence=${3:-}
  local name=$PREFIX$lane admin="$kc.admin"
  CREATED=0 OWNED=0 CNAME=$name
  trap 'exit 143' TERM INT # a manager stopping the step still runs the cleanup below
  trap cleanup_failed_up EXIT
  account >/dev/null
  mkdir -p "$(dirname "$kc")"
  STAGE_FILE="$kc.stages"
  : >"$STAGE_FILE"
  [ -z "$evidence" ] || mkdir -p "$evidence"
  if cluster_exists "$name"; then
    log "cluster $name exists: attaching"
    OWNED=1
  else
    ensure_shared
    stage shared_ready
    CREATED=1 OWNED=1
    log "creating cluster $name (${EKS_NODES:-2} x ${EKS_INSTANCE_TYPE:-m6i.xlarge})"
    create_lane_cluster "$name"
    stage cluster_and_nodegroup_created
  fi
  aws eks update-kubeconfig --name "$name" --region "$REGION" --kubeconfig "$admin" >/dev/null
  [ "$CREATED" = 1 ] || scale_up_if_needed "$name"
  chmod 600 "$admin"
  wait_nodes "$admin" "${EKS_NODES:-2}"
  stage nodes_ready
  network_policy_enforced "$admin" || die "the cluster does not enforce NetworkPolicy: vpc-cni needs enableNetworkPolicy=true"
  label_nodes "$admin"
  kubectl --kubeconfig "$admin" -n kube-system delete pdb coredns metrics-server --ignore-not-found >/dev/null 2>&1 || true
  token_kubeconfig "$admin" "$kc" "$name" "$lane"
  storage_class "$kc"
  [ "$CREATED" = 1 ] || open_nodeports "$name"
  entry_host "$kc"
  stage kubeconfig_storage_ports_ready
  apply_node_agent "$kc"
  stage node_agent_mirror_ready
  addons_running "$admin"
  stage addons_running
  stage up_total
  echo "EKS_LANE_READY cluster=$name instance=${EKS_INSTANCE_TYPE:-m6i.xlarge} nodes=${EKS_NODES:-2}"
}

cmd_down() {
  local lane=${1:?lane} kc=${2:-}
  local name=$PREFIX$lane t0=$SECONDS
  if cluster_exists "$name"; then
    [ -z "$kc" ] || [ ! -f "$kc.admin" ] || kubectl --kubeconfig "$kc.admin" delete pvc --all-namespaces --all --wait=false >/dev/null 2>&1 || true
    delete_cluster "$name" >&2
  else
    log "cluster $name does not exist"
  fi
  [ -z "$kc" ] || rm -f "$kc" "$kc.admin" "$kc.entry" "$kc.stages"
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
  if ! aws ecr describe-repositories --region "$REGION" --repository-names "$repo" >/dev/null 2>&1; then
    # lanes of a plan push the same declared images at once: the loser of the create has what it asked for
    out=$(aws ecr create-repository --region "$REGION" --repository-name "$repo" --image-scanning-configuration scanOnPush=false \
      --tags "Key=Project,Value=${EKS_TAG_PROJECT:-sdo-lanes}" 2>&1) ||
      case $out in *RepositoryAlreadyExistsException*) ;; *) echo "$out" >&2; return 1;; esac
  fi
  docker tag "$local_img" "$ecr/$repo:$tag"
  pushed=$(docker push "$ecr/$repo:$tag") || return 1
  # the digest of what this push stored: by tag it could be another lane's image pushed a moment later
  digest=$(printf '%s\n' "$pushed" | sed -n 's/.*digest: \(sha256:[0-9a-f]\{64\}\).*/\1/p' | tail -1)
  if [ -n "$digest" ]; then echo "$digest"; return 0; fi
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

# cmd_prepull KUBECONFIG IMAGE...: the lane's pre-pull list (ConfigMap sdo-lane-prepull in kube-system). The node agent pulls
# each image on every node, now and on every node that joins, through the mirror. An unqualified name is spelled out the way
# containerd resolves it; a reference with its own registry (an ECR digest pin) is kept.
cmd_prepull() {
  local kc=${1:?kubeconfig}; shift
  [ $# -gt 0 ] || die "prepull KUBECONFIG IMAGE..."
  local img name tag list=""
  for img in "$@"; do
    case $img in
      *@*) list+="$img"$'\n';;
      *) read -r name tag <<<"$(split_ref "$img")"; list+="$name:$tag"$'\n';;
    esac
  done
  kubectl --kubeconfig "$kc" -n kube-system create configmap sdo-lane-prepull --from-literal=images="$list" --dry-run=client -o yaml |
    kubectl --kubeconfig "$kc" apply -f - >/dev/null
  echo "EKS_PREPULL $# images"
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
  prepull) cmd_prepull "$@";;
  fence) cmd_fence "$@";;
  exists) cmd_exists "$@";;
  host-setup) cmd_host_setup;;
  host-teardown) cmd_host_teardown;;
  *) echo "usage: $0 up|down|push|pin|prepull|nodes|exists|fence|host-setup|host-teardown ..." >&2; exit 2;;
esac
