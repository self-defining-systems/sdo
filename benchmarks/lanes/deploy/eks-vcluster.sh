#!/usr/bin/env bash
# Lanes as virtual clusters on one long-lived EKS host cluster; the `lanes: vcluster` option of provider `eks`.
# Runs on the manager's machine (eksctl, aws, kubectl, helm, vcluster on PATH, AWS credentials in the environment).
#
#   eks-vcluster.sh host-up                       # create the host cluster once (idempotent: an existing one is attached, its nodes scaled up)
#   eks-vcluster.sh host-scale N                  # the system node group to N nodes (0: nothing but the control plane bills)
#   eks-vcluster.sh host-down                     # delete every lane's nodes, then the host cluster
#   eks-vcluster.sh up LANE KUBECONFIG [EVIDENCE] # a vcluster LANE with its own Karpenter nodes, in seconds on a warm host
#   eks-vcluster.sh down LANE [KUBECONFIG]        # delete the vcluster, its NodePool (its nodes) and the kubeconfig
#   eks-vcluster.sh exists LANE                   # exit 0 the lane exists, 1 it does not, 2 cannot tell
#   eks-vcluster.sh prepull KUBECONFIG IMAGE...   # add images to the host's pre-pull list (every node of the host pulls them)
#   eks-vcluster.sh nodes KUBECONFIG SCRIPT       # run sh -c SCRIPT as root on every node of the host
#   eks-vcluster.sh push|pin|fence ...            # as eks-lane.sh (ECR mirror; the fence is the lane's virtual kube-system)
#
# The host cluster (EKS_HOST_CLUSTER, kubeconfig EKS_HOST_KUBECONFIG) has one managed node group "system" (label
# sdo-role=system, tainted sdo-role=system:NoSchedule, scalable 0..2) that holds Karpenter, CoreDNS, the node agent and
# the control plane of every vcluster, and Karpenter. A lane gets:
#  - a Karpenter NodePool lane-<lane>: EKS_NODES x EKS_INSTANCE_TYPE, labelled and tainted sdo-lane=<lane>, capped at
#    EKS_NODES x EKS_NODE_VCPUS, so its nodes are its own and release when the pool is deleted. Pause pods with a
#    low priority ask for the nodes the moment the lane is made, so they are ready by the time the lane's API is.
#  - a vcluster in host namespace <lane>: it syncs only the lane's real nodes, tolerates the lane's taint on every pod it
#    syncs, sees the host's StorageClass local-path, and its API server is a NodePort of the host (the kubeconfig has
#    context kind-<lane>).
# A lane on a vcluster has no real kube-system and no node access (nodes and kube-system are the host's): plans that
# need them go to a full-cluster eks lane (docs/eks.md, "Lanes as virtual clusters").
# Settings: EKS_REGION, EKS_ACCOUNT, EKS_HOST_CLUSTER, EKS_HOST_KUBECONFIG (required); EKS_INSTANCE_TYPE (m6i.xlarge),
# EKS_NODES (2), EKS_NODE_VCPUS (4), EKS_AZ (<region>a), EKS_SYSTEM_INSTANCE (m6i.xlarge), EKS_VERSION (1.34),
# EKS_VPC / EKS_SUBNETS (as eks-lane.sh), KARPENTER_VERSION (1.6.0), VCLUSTER_VERSION (0.37.7), EKS_HOST_LAST=1 (the
# last lane of the host went: `down` scales the system node group to 0), EKS_DOWN_WAIT (240 s for a lane's nodes to go),
# EKS_NODEPORT_CIDR (as eks-lane.sh).
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
log() { echo "[eks-vcluster] $*" >&2; }
die() { log "$*"; exit 1; }

REGION=${EKS_REGION:-${AWS_REGION:-}}
[ -n "$REGION" ] || die "EKS_REGION is required"
export AWS_REGION=$REGION AWS_DEFAULT_REGION=$REGION
HOST=${EKS_HOST_CLUSTER:-}
HK=${EKS_HOST_KUBECONFIG:-}
AZ=${EKS_AZ:-${REGION}a}
NODES=${EKS_NODES:-2}
ITYPE=${EKS_INSTANCE_TYPE:-m6i.xlarge}
VCPUS=${EKS_NODE_VCPUS:-4}
KARPENTER=${KARPENTER_VERSION:-1.6.0}
VCVER=${VCLUSTER_VERSION:-0.37.7}
TAG=${EKS_TAG_PROJECT:-sdo-lanes}

hk() { kubectl --kubeconfig "$HK" "$@"; }
need_host() { [ -n "$HOST" ] && [ -n "$HK" ] || die "EKS_HOST_CLUSTER and EKS_HOST_KUBECONFIG are required"; }

account() {
  local a
  a=$(aws sts get-caller-identity --query Account --output text) || die "no usable AWS credentials"
  if [ -n "${EKS_ACCOUNT:-}" ] && [ "$a" != "$EKS_ACCOUNT" ]; then die "credentials belong to account $a, expected $EKS_ACCOUNT"; fi
  echo "$a"
}

STAGE_T0=$SECONDS
stage() { # NAME: seconds since the verb began, on stderr and in $STAGE_FILE
  echo "EKS_STAGE $1 $((SECONDS - STAGE_T0))" >&2
  [ -z "${STAGE_FILE:-}" ] || echo "$1 $((SECONDS - STAGE_T0))" >>"$STAGE_FILE"
}

host_exists() { eksctl get cluster --name "$HOST" --region "$REGION" >/dev/null 2>&1; }

write_host_config() {
  local f=$1
  {
    cat <<YAML
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: $HOST
  region: $REGION
  version: "${EKS_VERSION:-1.34}"
  tags: {Project: $TAG, sdo-host: $HOST}
upgradePolicy: {supportType: STANDARD}
iam: {withOIDC: true}
karpenter:
  version: "$KARPENTER"
  createServiceAccount: true
  withSpotInterruptionQueue: false
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
  - {name: coredns, configurationValues: '{"tolerations":[{"operator":"Exists"}]}', resolveConflicts: overwrite}
  - {name: kube-proxy}
  - {name: eks-pod-identity-agent}
  - {name: metrics-server, configurationValues: '{"tolerations":[{"operator":"Exists"}]}', resolveConflicts: overwrite}
managedNodeGroups:
  - name: system
    instanceType: ${EKS_SYSTEM_INSTANCE:-m6i.xlarge}
    desiredCapacity: 1
    minSize: 0
    maxSize: 2
    availabilityZones: [$AZ]
    volumeSize: 50
    volumeType: gp3
    privateNetworking: false
    labels: {sdo-role: system}
    taints:
      - {key: sdo-role, value: system, effect: NoSchedule}
    tags: {Project: $TAG, sdo-host: $HOST}
    iam:
      attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
YAML
  } >"$f"
}

system_nodes() { # the system node group's desired size
  aws eks describe-nodegroup --cluster-name "$HOST" --nodegroup-name system --region "$REGION" --query nodegroup.scalingConfig.desiredSize --output text
}

scale_system() { # N
  local want=$1
  [ "$(system_nodes)" = "$want" ] && return 0
  log "system node group of $HOST to $want"
  # a PodDisruptionBudget of the add-ons holds the last node in Terminating:Wait (a drain that cannot evict)
  if [ "$want" = 0 ]; then hk -n kube-system delete pdb coredns metrics-server --ignore-not-found >/dev/null 2>&1 || true; fi
  eksctl scale nodegroup --cluster "$HOST" --name system --nodes "$want" --nodes-min 0 --nodes-max 2 --region "$REGION" >&2
}

wait_system() { # a Ready system node
  local end=$((SECONDS + 600))
  until [ "$(hk get nodes -l sdo-role=system --no-headers 2>/dev/null | grep -c ' Ready')" -ge 1 ]; do
    [ $SECONDS -lt $end ] || die "no Ready system node after 10 minutes"
    sleep 5
  done
}

# lock: host-wide decisions (scale up, scale to zero) are made under one lock so a lane coming up and the last lane going
# cannot cross.
lock_host() {
  mkdir -p "$(dirname "$HK")"
  exec 9>"$(dirname "$HK")/.host.lock"
  flock 9
}

# discovery_tags: Karpenter finds subnets and the security group by tag.
discovery_tags() {
  local vpc sg subnets
  vpc=$(aws eks describe-cluster --name "$HOST" --region "$REGION" --query cluster.resourcesVpcConfig.vpcId --output text)
  sg=$(aws eks describe-cluster --name "$HOST" --region "$REGION" --query cluster.resourcesVpcConfig.clusterSecurityGroupId --output text)
  subnets=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" "Name=availability-zone,Values=$AZ" "Name=map-public-ip-on-launch,Values=true" --query 'Subnets[].SubnetId' --output text)
  [ -n "$subnets" ] || die "no public subnet of $vpc in $AZ"
  # shellcheck disable=SC2086
  aws ec2 create-tags --resources $subnets $sg --tags "Key=karpenter.sh/discovery,Value=$HOST" >/dev/null
  echo "$sg"
}

install_karpenter() {
  local role
  # eksctl installs Karpenter (namespace karpenter, its service account and the node role it made); this makes it fit the
  # tainted system node: one replica that tolerates it
  role=$(aws iam get-role --role-name "eksctl-KarpenterNodeRole-$HOST" --query Role.RoleName --output text 2>/dev/null || true)
  if [ -z "$role" ] || [ "$role" = None ]; then
    role=$(aws eks describe-nodegroup --cluster-name "$HOST" --nodegroup-name system --region "$REGION" --query nodegroup.nodeRole --output text)
    role=${role##*/}
  fi
  # Its service account and values are eksctl's (--reuse-values): setting serviceAccount.create here would delete the account.
  helm status karpenter --namespace karpenter --kubeconfig "$HK" >/dev/null 2>&1 || die "Karpenter is not installed on $HOST: the host cluster is made by host-up (eksctl installs it)"
  helm upgrade karpenter oci://public.ecr.aws/karpenter/karpenter --version "$KARPENTER" --namespace karpenter \
    --kubeconfig "$HK" --reuse-values --set replicas=1 --set controller.resources.requests.cpu=200m --set controller.resources.requests.memory=512Mi \
    --set 'tolerations[0].operator=Exists' --set 'nodeSelector.sdo-role=system' --wait --timeout 10m >&2
  hk apply -f - >/dev/null <<YAML
apiVersion: karpenter.k8s.aws/v1
kind: EC2NodeClass
metadata: {name: sdo-lane}
spec:
  role: $role
  amiSelectorTerms: [{alias: al2023@latest}]
  subnetSelectorTerms: [{tags: {karpenter.sh/discovery: $HOST}}]
  securityGroupSelectorTerms: [{tags: {karpenter.sh/discovery: $HOST}}]
  associatePublicIPAddress: true
  blockDeviceMappings: [{deviceName: /dev/xvda, ebs: {volumeSize: 100Gi, volumeType: gp3}}]
  tags: {Project: $TAG, sdo-host: $HOST}
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: sdo-lane-capacity}
value: -10
globalDefault: false
description: pause pods that ask Karpenter for a lane's nodes; any real pod preempts them
YAML
}

apply_storage_and_agent() {
  local ecr
  ecr="$(account).dkr.ecr.$REGION.amazonaws.com"
  # local-path provisions node-local volumes on the lane's tainted nodes: its helper pods tolerate every taint
  sed -e '/- key: node.kubernetes.io\/disk-pressure/,/effect: NoSchedule/c\        - operator: Exists' "$here/eks/local-path-storage.yaml" | hk apply -f - >/dev/null
  hk -n local-path-storage patch deployment local-path-provisioner --type merge -p '{"spec":{"template":{"spec":{"tolerations":[{"operator":"Exists"}]}}}}' >/dev/null
  hk -n local-path-storage rollout status deployment/local-path-provisioner --timeout=180s >/dev/null || die "the local-path provisioner is not ready"
  hk annotate storageclass gp2 storageclass.kubernetes.io/is-default-class=false --overwrite >/dev/null 2>&1 || true
  sed -e "s|@ECR@|$ecr|g" -e "s|@REGION@|$REGION|g" -e "s|@PREFIX@|${EKS_MIRROR_PREFIX:-sdo-mirror}|g" \
    -e "s|@UPSTREAMS@|${EKS_MIRROR_UPSTREAMS:-docker.io ghcr.io quay.io registry.k8s.io}|g" "$here/eks/node-agent.yaml" | hk apply -f - >/dev/null
  hk -n kube-system rollout status daemonset/sdo-lane-node-agent --timeout=300s >/dev/null || die "the node agent is not ready on every node"
}

open_nodeports() { # SG: the manager reaches the lanes' NodePorts on the nodes
  local sg=$1 cidr=${EKS_NODEPORT_CIDR:-}
  if [ -z "$cidr" ]; then cidr="$(curl -fsS --max-time 10 https://checkip.amazonaws.com | tr -d '[:space:]')/32"; fi
  aws ec2 authorize-security-group-ingress --group-id "$sg" --protocol tcp --port 30000-32767 --cidr "$cidr" >/dev/null 2>&1 || true # already open
}

cmd_host_up() {
  need_host
  CREATED=0
  trap 'exit 143' TERM INT
  account >/dev/null
  mkdir -p "$(dirname "$HK")"
  STAGE_FILE="$HK.stages"; : >"$STAGE_FILE"
  if host_exists; then
    log "host cluster $HOST exists: attaching"
    aws eks update-kubeconfig --name "$HOST" --region "$REGION" --kubeconfig "$HK" >/dev/null
    chmod 600 "$HK"
    scale_system 1
  else
    local cfg
    cfg=$(mktemp); write_host_config "$cfg"
    log "creating host cluster $HOST"
    eksctl create cluster -f "$cfg" --kubeconfig "$HK" >&2 || { rm -f "$cfg"; die "eksctl create cluster failed: $HOST may be half-made; run host-down"; }
    rm -f "$cfg"; chmod 600 "$HK"; CREATED=1
    stage host_cluster_created
  fi
  wait_system
  stage system_node_ready
  local sg
  sg=$(discovery_tags)
  install_karpenter
  stage karpenter_ready
  apply_storage_and_agent
  open_nodeports "$sg"
  stage storage_agent_ports_ready
  stage host_up_total
  echo "EKS_HOST_READY cluster=$HOST created=$CREATED"
}

cmd_host_scale() {
  need_host
  local n=${1:?nodes}
  lock_host
  scale_system "$n"
}

cmd_host_down() {
  need_host
  if host_exists; then
    # nodes Karpenter made are not in the cluster's stack: delete them first or the VPC cannot go
    hk delete nodepool --all --wait=true --timeout=300s >&2 2>/dev/null || true
    eksctl delete cluster --name "$HOST" --region "$REGION" --wait --disable-nodegroup-eviction >&2
  else
    log "host cluster $HOST does not exist"
  fi
  rm -f "$HK" "$HK.stages"
  echo "EKS_HOST_DELETED cluster=$HOST"
}

lane_nodepool() { # LANE
  local lane=$1
  hk apply -f - >/dev/null <<YAML
apiVersion: karpenter.sh/v1
kind: NodePool
metadata: {name: lane-$lane, labels: {sdo-lane: "$lane"}}
spec:
  template:
    metadata: {labels: {sdo-lane: "$lane"}}
    spec:
      nodeClassRef: {group: karpenter.k8s.aws, kind: EC2NodeClass, name: sdo-lane}
      taints: [{key: sdo-lane, value: "$lane", effect: NoSchedule}]
      expireAfter: Never
      requirements:
        - {key: node.kubernetes.io/instance-type, operator: In, values: [$ITYPE]}
        - {key: karpenter.sh/capacity-type, operator: In, values: [on-demand]}
        - {key: topology.kubernetes.io/zone, operator: In, values: [$AZ]}
        - {key: kubernetes.io/arch, operator: In, values: [amd64]}
  disruption: {consolidationPolicy: WhenEmpty, consolidateAfter: 30m}
  limits: {cpu: "$((NODES * VCPUS))"}
---
apiVersion: v1
kind: Namespace
metadata: {name: sdo-capacity}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: capacity-$lane, namespace: sdo-capacity}
spec:
  replicas: $NODES
  selector: {matchLabels: {sdo-capacity: "$lane"}}
  template:
    metadata: {labels: {sdo-capacity: "$lane"}}
    spec:
      priorityClassName: sdo-lane-capacity
      terminationGracePeriodSeconds: 0
      tolerations: [{key: sdo-lane, operator: Equal, value: "$lane", effect: NoSchedule}]
      nodeSelector: {sdo-lane: "$lane"}
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
              labelSelector: {matchLabels: {sdo-capacity: "$lane"}}
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
          resources: {requests: {cpu: 50m, memory: 16Mi}}
YAML
}

lane_values() { # LANE ENTRY FILE
  local lane=$1 entry=$2 f=$3
  cat >"$f" <<YAML
controlPlane:
  statefulSet:
    scheduling:
      nodeSelector: {sdo-role: system}
      tolerations: [{key: sdo-role, operator: Equal, value: system, effect: NoSchedule}]
  service:
    spec:
      type: NodePort
  proxy:
    extraSANs: [$entry]
sync:
  toHost:
    pods:
      enforceTolerations: ["sdo-lane=$lane:NoSchedule"]
  fromHost:
    nodes:
      enabled: true
      selector:
        labels: {sdo-lane: "$lane"}
    storageClasses:
      enabled: true
rbac:
  clusterRole:
    extraRules:
    - apiGroups: [""]
      resources: ["nodes/proxy", "nodes/stats"]
      verbs: ["get"]
policies:
  podSecurityStandard: ${VCLUSTER_POD_SECURITY:-privileged}
  resourceQuota:
    enabled: false
  limitRange:
    enabled: true
    default: null
    defaultRequest:
      cpu: 50m
      memory: 64Mi
YAML
}

system_entry() { # an address of the system node the manager reaches
  local ip
  ip=$(hk get nodes -l sdo-role=system -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
  [ -n "$ip" ] || die "the system node has no external address"
  echo "$ip"
}

write_lane_kubeconfig() { # LANE ENTRY OUT
  local lane=$1 entry=$2 out=$3 port tmp
  port=$(hk -n "$lane" get svc "$lane" -o jsonpath='{.spec.ports[?(@.name=="https")].nodePort}')
  [ -n "$port" ] || return 1
  tmp="$out.tmp.$$"
  KUBECONFIG=$HK vcluster connect "$lane" -n "$lane" --print --server "https://$entry:$port" --background-proxy=false \
    --kube-config-context-name "kind-$lane" >"$tmp"
  chmod 600 "$tmp" && mv "$tmp" "$out"
}

wait_lane_nodes() { # LANE: the lane's own nodes are Ready
  local lane=$1 end=$((SECONDS + 600))
  until [ "$(hk get nodes -l "sdo-lane=$lane" --no-headers 2>/dev/null | grep -c ' Ready')" -ge "$NODES" ]; do
    [ $SECONDS -lt $end ] || die "fewer than $NODES Ready nodes for lane $lane after 10 minutes"
    sleep 3
  done
}

# label_nodes LANE: the first node is the infra node, the rest (or the only one) are workers (as an eks lane); the
# vcluster copies the labels of the lane's nodes.
label_nodes() {
  local lane=$1 nodes first n
  nodes=$(hk get nodes -l "sdo-lane=$lane" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
  first=$(echo "$nodes" | head -1)
  for n in $nodes; do
    if [ "$n" = "$first" ]; then hk label node "$n" node-role.kubernetes.io/control-plane= --overwrite >/dev/null; fi
    if [ "$n" != "$first" ] || [ "$(echo "$nodes" | wc -l)" -eq 1 ]; then hk label node "$n" node-role.kubernetes.io/worker= --overwrite >/dev/null; fi
  done
}

cmd_up() {
  need_host
  local lane=${1:?lane} kc=${2:?kubeconfig} evidence=${3:-}
  CNAME=$lane CKC=$kc
  trap 'exit 143' TERM INT
  trap 'rc=$?; if [ $rc -ne 0 ]; then log "lane-up failed: removing $CNAME"; cmd_down_quiet "$CNAME" "$CKC"; fi' EXIT
  account >/dev/null
  mkdir -p "$(dirname "$kc")"
  STAGE_FILE="$kc.stages"; : >"$STAGE_FILE"
  [ -z "$evidence" ] || mkdir -p "$evidence"
  host_exists || die "host cluster $HOST does not exist: run \`lanes host adopt\` first"
  aws eks update-kubeconfig --name "$HOST" --region "$REGION" --kubeconfig "$HK" >/dev/null 2>&1 || true
  lock_host
  scale_system 1
  wait_system
  flock -u 9
  stage host_ready
  lane_nodepool "$lane"
  stage nodes_requested
  local entry vals
  entry=$(system_entry)
  if hk -n "$lane" get statefulset "$lane" >/dev/null 2>&1; then
    log "lane $lane exists: rewriting its kubeconfig"
  else
    vals=$(mktemp); lane_values "$lane" "$entry" "$vals"
    KUBECONFIG=$HK vcluster create "$lane" -n "$lane" --chart-version "$VCVER" --values "$vals" --connect=false --background-proxy=false >&2
    rm -f "$vals"
  fi
  hk label namespace "$lane" sdo-lane-ns=true --overwrite >/dev/null
  stage vcluster_created
  local i
  for i in $(seq 1 120); do
    if write_lane_kubeconfig "$lane" "$entry" "$kc" 2>/dev/null && kubectl --kubeconfig "$kc" --context "kind-$lane" --request-timeout=10s get ns >/dev/null 2>&1; then break; fi
    [ "$i" = 120 ] && die "lane $lane: API server not ready after 10 minutes"
    sleep 3
  done
  stage lane_api_ready
  wait_lane_nodes "$lane"
  label_nodes "$lane"
  hk -n sdo-capacity delete deployment "capacity-$lane" --ignore-not-found --wait=false >/dev/null
  stage nodes_ready
  local ip
  ip=$(hk get nodes -l "sdo-lane=$lane" -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}')
  [ -n "$ip" ] || die "no node address for the entry host"
  echo "$ip" >"$kc.entry"
  cp "$HK" "$kc.host" && chmod 600 "$kc.host"
  stage up_total
  echo "EKS_LANE_READY cluster=$lane host=$HOST instance=$ITYPE nodes=$NODES"
}

# cmd_down_quiet LANE KUBECONFIG: delete what `up` made, without waiting for the nodes.
cmd_down_quiet() {
  local lane=$1 kc=${2:-}
  KUBECONFIG=$HK vcluster delete "$lane" -n "$lane" --delete-namespace --wait=false >&2 || true
  hk delete namespace "$lane" --ignore-not-found --wait=false >&2 || true
  hk -n sdo-capacity delete deployment "capacity-$lane" --ignore-not-found --wait=false >&2 || true
  hk delete nodepool "lane-$lane" --ignore-not-found --wait=false >&2 || true
  [ -z "$kc" ] || rm -f "$kc" "$kc.entry" "$kc.stages" "$kc.host"
}

cmd_down() {
  need_host
  local lane=${1:?lane} kc=${2:-} t0=$SECONDS end
  trap - EXIT
  if ! host_exists; then
    log "host cluster $HOST does not exist: nothing to delete"
    [ -z "$kc" ] || rm -f "$kc" "$kc.entry" "$kc.stages" "$kc.host"
    echo "EKS_LANE_DELETED cluster=$lane seconds=0"; return 0
  fi
  aws eks update-kubeconfig --name "$HOST" --region "$REGION" --kubeconfig "$HK" >/dev/null 2>&1 || true
  cmd_down_quiet "$lane" "$kc"
  # the lane's nodes are released when Karpenter has terminated them: wait for that so the lane's cost ends where it says
  end=$((SECONDS + ${EKS_DOWN_WAIT:-240}))
  until [ "$(hk get nodes -l "sdo-lane=$lane" --no-headers 2>/dev/null | wc -l)" -eq 0 ]; do
    [ $SECONDS -lt $end ] || { log "nodes of $lane still terminating after ${EKS_DOWN_WAIT:-240} s"; break; }
    sleep 3
  done
  if [ "${EKS_HOST_LAST:-0}" = 1 ]; then
    lock_host
    if [ "$(hk get nodepool --no-headers 2>/dev/null | wc -l)" -eq 0 ]; then scale_system 0; fi
  fi
  echo "EKS_LANE_DELETED cluster=$lane seconds=$((SECONDS - t0))"
}

cmd_exists() {
  need_host
  local lane=${1:?lane} err
  if err=$(hk get namespace "$lane" -o name 2>&1); then exit 0; fi
  case $err in *NotFound*) exit 1;; esac
  log "cannot tell whether $lane exists: ${err: -200}"
  exit 2
}

# split_ref IMAGE: "<registry/path> <tag>" with Docker's implicit registry and library/ spelled out (as eks-lane.sh).
split_ref() {
  local image=$1 first full name tag
  first=${image%%/*}
  if [[ $image != */* ]]; then full=docker.io/library/$image
  elif [[ $first != *.* && $first != *:* && $first != localhost ]]; then full=docker.io/$image
  else full=$image; fi
  name=${full%:*}; tag=${full##*:}
  if [[ $full != *:* || $tag == */* ]]; then name=$full; tag=latest; fi
  echo "$name $tag"
}

cmd_prepull() {
  need_host
  local kc=${1:?kubeconfig}; shift
  [ $# -gt 0 ] || die "prepull KUBECONFIG IMAGE..."
  local have
  have=$(hk -n kube-system get configmap sdo-lane-prepull -o jsonpath='{.data.images}' 2>/dev/null || true)
  local tmp img name tag
  tmp=$(mktemp)
  {
    printf '%s\n' "$have"
    for img in "$@"; do
      case $img in
        *@*) echo "$img";;
        *) read -r name tag <<<"$(split_ref "$img")"; echo "$name:$tag";;
      esac
    done
  } | grep -v '^$' | sort -u >"$tmp"
  hk -n kube-system create configmap sdo-lane-prepull --from-file=images="$tmp" --dry-run=client -o yaml | hk apply -f - >/dev/null
  rm -f "$tmp"
  echo "EKS_PREPULL $# images"
}

cmd_nodes() {
  need_host
  [ $# -ge 2 ] || die "nodes KUBECONFIG SCRIPT"
  exec bash "$here/eks-lane.sh" nodes "$HK" "$2"
}

verb=${1:-}
[ $# -eq 0 ] || shift
case $verb in
  host-up) cmd_host_up "$@";;
  host-scale) cmd_host_scale "$@";;
  host-down) cmd_host_down "$@";;
  up) cmd_up "$@";;
  down) cmd_down "$@";;
  exists) cmd_exists "$@";;
  prepull) cmd_prepull "$@";;
  nodes) cmd_nodes "$@";;
  push|pin|fence) exec bash "$here/eks-lane.sh" "$verb" "$@";;
  *) echo "usage: $0 host-up|host-scale|host-down|up|down|exists|prepull|nodes|push|pin|fence ..." >&2; exit 2;;
esac
