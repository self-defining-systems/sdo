# Hotel latent-fault catalog

Audited at SDO `496845489`, SREGym `0a32595f82633ec0b6676eb1333a10ef9e9f1778`
and applications `d1a7e02eb873520d2df3eea9f81f54eda5eb149d`. SREGym was read
from the supplied `codex-composite/third_party/sregym` checkout, without mutation.
Below, `problems/` and `oracles/` mean `sregym/conductor/` in that revision;
variant counts come from `problems/variant_specs.py` (manual aliases do not
add targets). The eight service targets are frontend, geo, profile, rate,
recommendation, search, reservation and user.

**Latent is relative to the accepted health objective and a 180-second detection
window.** The recorded runs below use the readiness/endpoint `health-objective`,
not a functional traffic SLO. Their accepted detector manifest and source are
under each run's `application_workspace/.sdo/diagnostics/`. Numeric targetPort
changes preserve ready endpoints; that objective never compares ports. The
checked-in assurance seed additionally installs `traffic-health` and
`service-endpoints`: do not call a traffic-detected fault latent or silently
substitute that seed for the recorded structural-health seed.

Feed references (all in `sdo/operator/runtime/`):

- **F:** `kubernetes_cache.go:93`–103 lists the complete automatic drift watches:
  ConfigMap, Service, Pod, Deployment, ReplicaSet, Endpoints, EndpointSlice,
  **NetworkPolicy**. `kubernetes_cache.go:369`–390 implements those informers.
  The cache is restricted to the application namespace (`kubernetes_cache.go:115`).
- **O:** a watch alone does not admit drift. `drift.go:397`–403 requires fields
  owned by `sdo-deployer`; `program_read_view.go:109`–114 requires a matching
  accepted manifest. New injector-owned objects usually satisfy neither.
- **S:** `drift.go:292`–311 sweeps learned programs independently of change
  classification; `drift.go:342` names their findings `drift/<program>`.
  Existing programs cannot establish the first-catch learning claim.

## Classes suitable for this claim

| Fault / source | Changed kinds; feed today | Mitigation oracle | Targets / variants | Latency evidence; usable now? |
| --- | --- | --- | --- | --- |
| Numeric targetPort mismatch (`problems/target_port.py:12`) | Service; watched (F), existing source field (O) | `TargetPortMisconfigMitigationOracle`: original targetPort restored and listed containers ready; alternate working ports are rejected | 8 services × bad port 9999 = 8 | **Observed latent** on reservation, geo, rate; remaining targets inferred from the same injection and detector. **Usable; primary stream.** |
| Deny ingress or egress (`problems/network_policy_block.py:37`) | NetworkPolicy; **already watched** (F), but injection creates an unowned object (O) | `NetworkPolicyMitigationOracle`: current rollout, target endpoints, fresh TCP connection and frontend recommendations request | 8 services × 2 directions × 3 names = 48 one-direction variants | **Inferred latent**: no pod restart; structural objective does not test connectivity. **First catch blocked by ownership/source admission**, not a missing NetworkPolicy informer. |
| Deny both directions (same source) | NetworkPolicy; watched, same admission gap | Same functional oracle | 8 × 1 direction × 3 names = 24 | **Observed historical miss**, compose row 9 (with targetPort). Current assurance objective explicitly reports `network-policy-total-isolation`; not latent with that objective. |
| Polluted frontend endpoints (`problems/service_wrong_pod_selection_hotel_reservation.py:11`) | Service, Deployment and derived Pods/Endpoints; watched (F) | `WrongPodSelectionMitigationOracle` checks endpoint ownership and routing, unlike endpoint-count grading | 1 fixed pair: frontend:5000 + search:8082 | **Inferred latent** after rollout: ready endpoints still exist. Injection changes a workload template, so an early availability finding is possible. Candidate for a single-fault pilot; no 3 distinct registered targets for repetition. |

The automatic stream-probe resolver (`benchmarks/sregym/fastloop/fault_spec_worker.py`)
supports targetPort, NetworkPolicy, image, readiness and wrong-selector faults.
The last three are health-visible here: repeat2 image rows 1/3/6 open through
`health-objective`; compose readiness row 3 and wrong-selector row 6 do likewise.
Selector mutation `swap_to_other_service` and ConfigMap `clear_data` were removed
from SREGym's variants because their original oracles passed with faults active;
they are not extra usable latent classes.

## Other registry-reachable latent candidates

These registered single faults can be injected/graded by fastloop without a
composite probe. **All are code-inferred, not measured 180-second negatives.**
They are listed to avoid treating absence of a health rule as live evidence.
Deployment mutations can transiently reduce availability, so those candidates
need a pilot before inclusion in a latent stream. Source names below identify
the owning problem file and attached oracle; no new fault variants are invented.

| Problem family (`problems/<name>.py`) | Changed kinds; feed (F/O) | Oracle can grade? | Registered targets / variants; exclusion |
| --- | --- | --- | --- |
| `wrong_dns_policy` | Deployment → Pods; watched | `DNSResolutionMitigationOracle`, DNS probe | 8 services × 2 DNS modes = 16; rollout/startup may be health-visible |
| `service_dns_resolution_failure` | CoreDNS ConfigMap/Deployment in kube-system, or app NetworkPolicy | DNS probe | 8 services × 2 rcodes (CoreDNS) + 8 deny-egress = 24; CoreDNS outside feed namespace, policy admission gap |
| `stale_coredns_config` | kube-system ConfigMap/Deployment; outside feed namespace | DNS probe | 1 hotel variant; needs scoped system-namespace feed |
| `configmap_drift` | ConfigMap + Deployment → Pods; watched | `MissingCmKeyMitigationOracle`, required keys | geo only (1); missing GeoMongoAddress can also break startup |
| `hpa_missing_effective_cpu_request` | Deployment → Pods, HPA; HPA unwatched | `HPAControlPlaneMitigationOracle`, effective requests and scaling condition | frontend only (1); needs HPA view/feed for invariant, rollout pilot |
| `file_descriptor_exhaustion` | Deployment → Pods plus process FD flood; workload objects watched | `FDMitigationOracle`, readiness plus 15s without FD-error logs; unreadable logs become empty, so false success is possible | 8 services × 2 limits = 16; needs verified saturation and rollout/health pilot, no pure snapshot proof of FD usage |
| `ephemeral_port_range_hotel_reservation` | Deployment sysctls → Pods; watched | `WorkloadOracle`, functional workload | 2 scopes × 2 severities = 4, only frontend/all targets; rollout pilot |
| `ingress_misroute` | Ingress + proxy Services and ingress-controller resources; Ingress unwatched | `IngressMisrouteMitigationOracle`, backend routing | 1 /api → frontend-service/recommendation-service; needs Ingress feed/view and accepted source |
| `expired_tls_hotel_reservation` | Secret + Ingress, ingress-controller resources; Secret/Ingress unwatched | `ExpiredTlsMitigationOracle`, certificate expiry (removal/no TLS also accepted) | 1 frontend certificate; needs Ingress/Secret feed/view and certificate primitive |
| `cronjob_sidecar_blocks_completion` | CronJob → Job → Pods; only Pods watched | `CronJobSidecarBlocksCompletionMitigationOracle`, completion/native sidecar and backlog | 1 audit-log-archiver; add CronJob/Job feed/view and source admission |
| `finalizer_deadlock_controller` | ClusterRole, ClusterRoleBinding, ServiceAccount, ConfigMaps, controller Deployment/Pods; only last three kinds watched | `FinalizerDeadlockControllerMitigationOracle`, finalization progress | 1 controller target × 3 RBAC modes; add RBAC feed/view; new objects lack accepted source |
| `dev_shm_exhaustion_hotel_reservation` | New worker Deployment → Pods; watched but no source/ownership | `DevShmMitigationOracle`, worker shared memory and health | 1 auxiliary worker; structural objective inventories app deployments, so may miss worker crash; admission gap |
| `node_conntrack_exhaustion` | Helper Deployments/Pods + host conntrack state; objects watched, host state absent | `ConntrackMitigationOracle`, conntrack and fresh connections | 1 node scenario; needs host observation, not just more kinds |
| `calico_route_reflector_label_drift` | Node labels + Calico BGP resources and helper workloads; root-cause kinds unwatched | `CalicoRouteReflectorMitigationOracle`, BGP/connectivity | 1 topology; needs Node/Calico feed/view, Calico and multiple nodes |
| `node_clock_drift` | Helper Pods, TLS Secret, frontend template + host clock; only workload kinds watched | `NodeClockDriftMitigationOracle`, restored time | 1 node scenario; unsupported on emulated clusters; needs host-time observation |
| `revoke_auth`, `storage_user_unregistered` | MongoDB state via exec; no Kubernetes spec change | **No reliable latent grading:** generic `MitigationOracle` checks pod readiness | 2 DB targets each (geo/rate); functional auth oracle and database observation needed |
| `silent_data_corruption`, `khaos_faults` | Device/syscall state; helper objects do not expose root cause | Alert/workload oracle attached, not proven for latent integrity | corruption: 3 DB targets × 3 strategies × 2 rates × 3 timings = 54; generic Khaos: 1 default target per fault, 23 single and 5 compound variants; requires Khaos, no snapshot invariant |

Other hotel registry families deliberately remove ready replicas/endpoints
(image/probe, missing Service/ConfigMap, resource/scheduling/storage failures,
admission outage/TLS/cumulative timeout, PSA recreation). They are not established
latent faults. Slow liveness churn may evade a particular window but is not a
confirmed latent class. BlueprintHotelReservation retry-storm/GC experiments use
a different application/health setup and are outside this hotel stream.

## Recorded evidence

Evidence root: `results/playbook-programs/`; row indices are zero-based.
Read `results/<run-id>/incidents.jsonl`, and join `incident_id` to the outcome
in `application_workspace/.sdo/outcomes/` for opening detector IDs.

| Run / result ID | Rows | Injection → detection; opening detector; oracle |
| --- | --- | --- |
| fastloop-repeat2 / 20261021T102709Z-sdo | 0 reservation, 2 geo, 5 rate targetPort | null; no incident in 180s; oracle false for each |
| same | 1 rate, 3 search, 6 recommendation image | 0.539 / 0.600 / 0.539 s; `health-objective`; oracle true |
| fastloop-compose / 20261021T111327Z-sdo | 0 geo targetPort; 9 search deny-both + reservation targetPort | null; no incident in 180s; oracle false (both components in row 9) |
| same | 3 user readiness; 6 profile wrong selector | 0.757 / 1.660 s; `health-objective`; oracle true |
| fastloop-driftlive3 / 20261021T163200Z-sdo | 0 reservation targetPort | 42.412 s; `drift/investigator`; oracle true |
| same | 2 geo targetPort | timing null despite a recorded incident; outcome opens with `drift/investigator`; oracle true. Count as caught, timing unknown, not a zero-second catch or a program sweep. |

These runs establish targetPort misses and investigator catches, **not** learned
sweep reuse. Mixed incidents that open through a companion's health finding do
not establish latent detection. No catalog candidate is promoted to measured
latent merely because a composite oracle eventually passes.

## Primary stream and coverage limit

`pbprog_latent.json` has ten standalone incidents:

| Position | Fault | Why |
| --- | --- | --- |
| 1 | reservation targetPort 9999 | measured latent; first catch/learning opportunity |
| 2 | recommendation targetPort 9999 | unseen target; transfer opportunity |
| 3 | rate nonexistent image tag | measured health-visible control |
| 4 | geo targetPort 9999 | measured latent; unseen target |
| 5 | search targetPort 9999 | unseen target |
| 6 | rate targetPort 9999 | measured latent after control recovery |
| 7 | frontend targetPort 9999 | different port (5000 vs backend 808x) |
| 8 | user readiness wrong_path, 1 replica | measured health-visible control |
| 9 | profile targetPort 9999 | unseen target |
| 10 | user targetPort 9999 | unseen target after control recovery |

Controls are interleaved; faults are never composed. All eight registered
hotel targetPort targets occur once. There is only one **evidenced, repeatable,
currently first-catch-admissible** class, so the stream cannot yet test transfer
across fault classes. Other targets' latency is inferred until measured.

No feed-only stream is added: NetworkPolicy already has an informer; the next
policy task needs explicit handling of newly introduced, unowned/source-absent
objects (and tests), or an accepted permissive policy fixture before injection.
Ingress/Secret, CronJob/Job, HPA and RBAC candidates need the kind/namespace/view
changes listed above as well as source admission. A watch-only patch is not
sufficient. Run the primary stream with `--detection-timeout 180` and
`--continue-on-error`, preserving the recorded structural-health objective;
report misses, health catches, investigator catches and learned sweep catches
separately. This catalog and stream have not been run live.
