# Detector catalog

Each item says what to check, which source fact it comes from, and the input that expresses it. Inputs:

- **Snapshot**: the controller's watched Kubernetes objects: Pod, Service, Deployment, ReplicaSet, StatefulSet,
  DaemonSet, Job, CronJob, ConfigMap, PersistentVolumeClaim, ServiceAccount, HorizontalPodAutoscaler,
  PodDisruptionBudget, Endpoints, EndpointSlice, NetworkPolicy, Ingress, Event, Node (cluster-scoped), and Secret
  metadata (name, type, labels and data key names, never values). Event-driven and free.
- **Traffic**: synthetic HTTP scenarios and workloads (`sdo.dev/operator/sdk/traffic`), a few requests per second.
- **Links**: fresh protocol probes between Services, declared in the `links` workload: tcp, http, grpc (health
  check), redis, memcached, mongodb, postgres or mysql. A probe proves the server answers its protocol, not that a
  query succeeds.
- **Usage**: kubelet container and volume usage (`sdo.dev/operator/sdk/usage`): CPU, memory working set and RSS
  with requests and limits, optional CPU throttling, and PVC capacity, use and inodes, over a rolling window of
  recent samples. No observability stack needed; collected only while a detector watches `usage.Watch`.
- **Logs**: bounded container-log pattern counts (`sdo.dev/operator/sdk/logs`): RE2 patterns per query, matched
  and total line counts per container per cycle, a few short excerpts, truncation flags, and optionally the tail
  of the previous terminated container. At most 64 KiB per container per cycle; collected only while a detector
  watches `logs.Watch`.
- **Metrics**: threshold rules (`sdo.dev/operator/sdk/metrics`) over a bound source, in its dialect. Only when the
  task lists a metrics source ([telemetry](telemetry.md)). Use only series whose exporter you can find in source, and
  the source's explicit `publish` block.

The [SDK reference](sdk.md) and [traffic reference](traffic.md) have the exact types: its SDK reference, and its traffic reference for
scenarios, workloads and links.

## Already covered (do not author)

- `service-endpoints`: Services that expect endpoints and have none ready.
- `workload-health`: failed, Unknown, stuck Pending or unschedulable pods; crash-looping, image-pull,
  config-error, OOMKilled, errored, waiting, restarting or unready containers; stuck init containers; Deployments
  below desired availability or past their progress deadline.
- `service-reachability`: Service ports that refuse connections while their pods are Ready.
- `traffic-health` and `traffic-links`: generated from the workloads you author.
- `metrics-alerts`: firing alerts of alert-capable sources in the application namespace, when a bound telemetry source serves alerts.
  This includes alert rules the application ships, once its backend loads them.
- `cluster-wiring`: Service selectors matching no pods or several workloads, named target ports no container
  declares, missing pod-template references and keys (ConfigMap, Secret, PVC, ServiceAccount), unbound PVCs,
  missing Ingress backends, NetworkPolicies isolating all ingress or egress of pods behind a Service,
  ExternalName Services pointing at missing in-namespace Services, and, while a user-facing probe fails, pods that
  started before a Secret they read last changed (metadata only, never values).
- `workload-controllers`: StatefulSets and DaemonSets below desired ready or stuck on an old revision, failed
  Jobs, overdue CronJobs.
- `warning-events`: lasting mount, scheduling, back-off, probe, eviction and OOM warnings on application objects
  that are unhealthy now; a Ready or completed pod has none.
- `node-health`: nodes running application pods that stay NotReady past the self-heal hold.
- `resource-saturation`: memory working set above 90% of its limit, PVC bytes or inodes above 90% or forecast
  to run out (CPU throttling only when enabled).
- Controller drift: live fields that differ from the accepted source, and foreign changes. These are clues: they
  never open an incident.

Spend your detectors on what only the source can tell: the layers below list those.

## 1. User journeys (Traffic, critical)

- Each user-facing read route: a scenario whose step checks content that the healthy application returns
  (`.Contains`). Draw parameters from seeded records.
- Each write route: the skill's write-read probe in the `probe-write` workload, with a `Marker`, a test identity
  and a following step that reads back what was written (`SaveJSON`, `FromPrevious`).
- Login and session flows: chain the steps the way a client does.
- The web page and its static assets: one GET each, checking a marker the page always contains.
- Failures reported inside a success status: check the body and declare `traffic.FaultWrongBody`.
- Latency: set the SLO from a budget the source states (see methods) and declare `traffic.FaultSlow`.

## 2. Per-service localization (Traffic, Links)

- Each internal HTTP API: a scenario aimed at that Service directly, with `DependsOn` naming its downstream
  Services. When a journey fails, these show which hop broke.
- Each service-to-service call the source declares, data stores included: a link with the protocol the caller
  uses, following the link rules in the onboarding agent task. Use `tcp` only when no protocol fits.
- A grpc link works on any gRPC server: one without the health service answers UNIMPLEMENTED, which counts as
  reachable.
- Not expressible yet: request-level calls to gRPC, Thrift or other non-HTTP APIs, and UDP. A grpc link proves
  the server answers gRPC, not that a method works.

## 3. Declared versus actual from the source (Snapshot, warn)

The generic wiring, workload, event, node and saturation checks above already cover Kubernetes structure.
Add what needs the application's own source:

- **ConfigMap content:** a key the code reads is missing, or does not parse in the format the loader expects
  (JSON, YAML, number, duration, host:port). Check structure, never literal values.
- **Named peers:** an env, arg or config value that names another Service and port points to a Service that
  does not exist, or that does not expose that port. Resolve names against the snapshot at detection time.
- **NetworkPolicy against real callers:** a policy that selects an application pod denies ingress from a
  caller the source shows talks to it. The generic check only catches policies that deny everything.
- **Replica outliers:** one pod's restart count, readiness or event count differs sharply from its siblings
  under the same owner.
- **Scaling and disruption the app depends on:** an autoscaler at its maximum while its target is saturated; a
  disruption budget with zero allowed disruptions for longer than a rollout.

## 4. Capacity the application declares (Usage, Metrics, warn)

`resource-saturation` already judges memory and volumes against Kubernetes limits. Add limits the source
declares: worker pools, connection pools, queue capacities or cache sizes, from exported metrics or usage, and
CPU throttling for latency-sensitive services when the source shows tight CPU limits
(`usage.Requirements{Throttling: true}`). Build findings with `usage.ThresholdFinding` or the metrics threshold
detector so the observed value, limit and window are in Evidence.

## 5. Data stores and queues

- Data presence: the read-later probe of the skill. It reads data written earlier through the application.
- Store and queue health from exporters the source deploys (Metrics), against their healthy range.
- Store reachability: a link with the store's protocol (mongodb, redis, memcached, postgres, mysql) from each
  Service that uses it.
- Not expressible yet: authenticated queries, data contents outside the application, and message brokers.

## 6. Application metrics (Metrics)

- Error ratio and latency from counters and histograms the code exports, against the healthy level.
- Absence: a series that the healthy application always reports disappears, for example a scrape target going
  down. Write the query so absence yields a value, as with `absent()`.

## 7. Logs (Logs, warn)

- Known-bad lines: patterns built from the error strings the source logs (`log.Error` calls and fatal paths).
  Fire on `logs.Appeared`, so a line
  that also appears while healthy never pages.
- Error rate: `logs.Rate` of error-level lines above the healthy rate. With truncated evidence the rate is a
  lower bound: a high value still counts, a low one proves nothing.
- Silence: `logs.Silent` for a workload that logs steadily while healthy (for example a request log) and goes
  quiet while Running.
- Keep patterns specific: one regex per failure the source names, not one per log line.

## 8. Change windows and traces

Not expressible yet: no detector input compares golden signals before and after a change, or reads traces.
