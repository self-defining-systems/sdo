# Detector catalog

Each item says what to check, which source fact it comes from, and the input that expresses it. Inputs:

- **Snapshot**: the controller's watched Kubernetes objects (Pod, Service, Deployment, ReplicaSet, ConfigMap,
  Endpoints, EndpointSlice, NetworkPolicy, Event). Event-driven and free.
- **Traffic**: synthetic HTTP scenarios and workloads (`sdo.dev/operator/sdk/traffic`), a few requests per second.
- **Links**: fresh TCP dials between Services, declared in the `links` workload.
- **Usage**: kubelet container and volume usage (`sdo.dev/operator/sdk/usage`): CPU, memory working set and RSS
  with requests and limits, optional CPU throttling, and PVC capacity, use and inodes, over a rolling window of
  recent samples. No observability stack needed; collected only while a detector watches `usage.Watch`.
- **Metrics**: PromQL threshold rules (`sdo.dev/operator/sdk/metrics`). Only when the task says a metrics source
  is configured, and only over series whose exporter you can find in the source.

The SDK reference in `sdo-detector-authoring` has the exact types.

## Already covered (do not author)

- `service-endpoints`: Services that expect endpoints and have none ready.
- `workload-health`: failed, Unknown, stuck Pending or unschedulable pods; crash-looping, image-pull,
  config-error, OOMKilled, errored, waiting, restarting or unready containers; stuck init containers; Deployments
  below desired availability or past their progress deadline.
- `service-reachability`: Service ports that refuse connections while their pods are Ready.
- `traffic-health` and `traffic-links`: generated from the workloads you author.
- `metrics-alerts`: firing Prometheus alerts in the application namespace, when a metrics source is configured.
  This includes alert rules the application ships, once its Prometheus loads them.
- Controller drift: live fields that differ from the accepted source, and foreign changes. These open their own
  incidents.

## 1. User journeys (Traffic, critical)

- Each user-facing read route: a scenario whose step checks content that the healthy application returns
  (`.Contains`). Draw parameters from seeded records.
- Each write route: a journey workload with `SideEffectWriteWithCleanup`, a `Marker`, and a following step that
  reads back what was written (`SaveJSON`, `FromPrevious`). The read-back is the data-correctness check.
- Login and session flows: chain the steps the way a client does.
- The frontend page and its static assets: one GET each, checking a marker the page always contains.
- Failures reported inside a success status: check the body and declare `traffic.FaultWrongBody`.
- Latency: set the SLO from a budget the source states (see methods) and declare `traffic.FaultSlow`.

## 2. Per-service localization (Traffic, Links)

- Each internal HTTP API: a scenario aimed at that Service directly, with `DependsOn` naming its downstream
  Services. When a journey fails, these show which hop broke.
- Each service-to-service call the source declares: a link, following the link rules in the judge task.
- Not expressible yet: request-level probes of gRPC, Thrift or other non-HTTP APIs. Links prove only that a
  TCP connection opens.

## 3. Declared versus actual in Kubernetes (Snapshot, warn)

- **Selector:** a Service selector matches no pods, or matches pods owned by more than one Deployment.
- **Ports:** a Service `targetPort` (numeric or named) is not a container port of the pods it selects. An
  EndpointSlice port disagrees with its Service port.
- **ConfigMap references:** a Deployment references a ConfigMap or key that does not exist. Use
  `sdk.ConfigMapReferencesForDeployment`; skip references marked optional.
- **ConfigMap content:** a key the code reads is missing, or does not parse in the format the loader expects
  (JSON, YAML, number, duration, host:port). Check structure, never literal values.
- **Named peers:** an env, arg or config value that names another Service and port points to a Service that
  does not exist, or that does not expose that port. Resolve names against the snapshot at detection time.
- **NetworkPolicy:** a policy that selects an application pod denies ingress from a caller that the
  references above show talks to it. Also flag a policy that denies all ingress to pods behind a Service.
- **Warning events:** probe failures, back-off, failed mounts, failed scheduling, evictions and OOM kills on
  application objects, above the healthy rate (normally zero). Use `RecentEventsFor`.
- **Replica outliers:** one pod's restart count, readiness or event count differs sharply from its siblings
  under the same owner.
- Not expressible yet from the snapshot: StatefulSet, DaemonSet, Job, CronJob, PersistentVolumeClaim,
  HorizontalPodAutoscaler, Ingress, Secret and Node state. Use Metrics over kube-state-metrics series when a
  source exports them; otherwise record a blind spot.

## 4. Saturation and capacity (Usage, warn)

- CPU throttling: `usage.ThrottledFraction` above about 0.25. Declare `usage.Requirements{Throttling: true}`; it
  costs one extra kubelet read per node per cycle.
- Memory: `usage.RatioToLimit` of the working set above about 0.9, or `usage.TimeToThreshold` reaching the limit
  within an hour (a leak).
- Volumes: available bytes or inodes forecast to reach zero within a few hours.
- A container without a limit has no ratio: compare against its request or its siblings instead, and say so in
  the evidence.
- Build findings with `usage.ThresholdFinding` so the observed value, limit, window and forecast are in Evidence.
- Kubernetes objects the snapshot lacks (CronJob success, failed Jobs, autoscaler at maximum): Metrics over
  kube-state-metrics series when a source exports them; otherwise record a blind spot.

## 5. Data stores and queues

- Data presence: a journey that reads a seeded record through the application (layer 1). It catches lost,
  wiped or unreachable data end to end.
- Store and queue health from exporters the source deploys (Metrics): replication state, cache hit ratio
  falling, connections near maximum, consumer lag growing, dead-letter growth.
- Not expressible yet: native-protocol pings (Mongo, Redis, memcached, SQL, Kafka) and reads of a store's own
  records outside the application.

## 6. Application metrics (Metrics)

- Error ratio and latency from counters and histograms the code exports, against the healthy level.
- Absence: a series that the healthy application always reports disappears, for example a scrape target going
  down. Write the query so absence yields a value, as with `absent()`.

## 7. Logs, change windows, traces

Not expressible yet: no detector input reads logs, compares golden signals before and after a change, or reads
traces. Error strings found in the source still belong in playbooks as diagnosis hints.
