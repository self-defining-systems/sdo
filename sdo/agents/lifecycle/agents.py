"""Structured, fresh-session agents used during SDO lifecycle bootstrap."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Protocol

from pydantic import BaseModel, ConfigDict, Field

from libs.agent_cli.structured import (
    AccessMode,
    AgentProvider,
    StructuredTurnError,
    StructuredTurnTimeout,
    run_structured_turn,
)

if TYPE_CHECKING:
    from pathlib import Path

    from agentshim import CommandExecutor


class LifecycleAgentError(RuntimeError):
    """Raised when a lifecycle agent cannot produce a valid structured handoff."""


class TopologyResourceDTO(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: str = Field(min_length=1)
    name: str = Field(min_length=1)
    namespace: str = Field(min_length=1)
    source: str = Field(min_length=1)
    dependencies: list[str]


class HealthJudgeDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")

    failure_patterns: list[str] = Field(min_length=1)
    detector_ids: list[str] = Field(min_length=1)


class HealthJudgeArtifact(HealthJudgeDraft):
    session_id: str = Field(min_length=1)


#: Health-judge instructions for direct synthetic traffic edits.
TRAFFIC_AUTHORING = """Synthetic traffic (health-judge owned). For an application that serves HTTP, also author
end-to-end synthetic traffic in the checkout under `.sdo/diagnostics/traffic/` (full API: references/traffic.md of the
`sdo-detector-authoring` skill). Ground it in the application source and
the accepted architecture summary: open the user-facing entrypoint's route handlers (its HTTP mux or
router) and derive paths it really serves, their methods, and parameter values its handlers accept, including the
records the application seeds at startup. Cover the main user journeys, read and write paths both when the
application has write paths. An isolated prober runs these journeys continuously at a few requests per second; the
traffic health detector opens an incident when a scenario violates its SLO, and a responder's repair is accepted only
when the same scenarios recover. Every scenario must therefore succeed against the healthy application.

1. Generators: Go package `generators` in `.sdo/diagnostics/traffic/generators/` exporting
   `func Scenarios() traffic.Catalog` (import "sdo.dev/operator/sdk/traffic"). Each traffic.Scenario has ID,
   Description, Target {Service, Port} (a source-backed Service and a port it exposes), DependsOn (the Services on
   the request path per the architecture summary, for localization), SideEffect (traffic.SideEffectRead,
   SideEffectIdempotentWrite, or SideEffectWriteWithCleanup with Cleanup steps), Marker (writes only: a string such
   as "sdo-synthetic" that appears in every write request and names dedicated synthetic data), Steps, and Detects
   (extra fault classes: traffic.FaultWrongBody when a step checks the body, traffic.FaultSlow). A simple step is one
   line: `{Name: "list", Endpoint: traffic.GET("/items", traffic.Params{"from": traffic.DateRange("2024-01-01",
   "2024-01-31"), "to": traffic.DaysAfter("from", 1, 3)}).Contains("expected text")}`. Other parameter
   generators: Const, OneOf, IntBetween, FloatBetween, FromSeed(count, render), FromSeedPair(key, count, first,
   second) for matching credentials, FromPrevious(key); chain steps with .SaveJSON(field, key) and "{key}" path
   segments. Use .Contains(...) whenever the application reports failures inside a successful status. Generators only
   build requests and check responses: never import net, os, or the clock, and draw randomness only from the rng the
   engine passes. Never read or modify real users' data beyond the fixtures the application's own source seeds.
2. Workloads: a file `.sdo/diagnostics/traffic/workloads/<name>.yaml` with apiVersion sdo.dev/v1alpha1, kind
   TrafficWorkload, name equal to the file name, purpose, ratePerSecond, and scenarios [{id, weight}]. Write
   `health` (purpose health-probe, ratePerSecond at most 4, read scenarios only unless a write is idempotent) and
   `verify` (purpose verify-burst, ratePerSecond about 12, duration 3s, the same scenarios). Optional: timeout (2s),
   slo {window, minSamples, maxErrorRate, maxTimeoutRate, latencyPercentile, maxLatency}, and journey workloads
   (purpose journey, bounded duration) for writes that should not run continuously.
3. Firing policy: a traffic-health detector's window is re-evaluated on every probe poll (about every 500ms), far
   faster than the 2-evaluation firing threshold, so a brief data-plane stall (observed around 3s, roughly 1 in
   6-10 pod-network changes on a small cluster) can otherwise reach it and dispatch a responder for nothing.
   `traffic.NewDetector` therefore defaults `Persistence.MinDuration` to `traffic.DefaultHealthMinDuration` (9s) for
   any health-class detector that leaves it unset, so a scenario must violate its SLO continuously for that long,
   not just reach the evaluation count, before it fires; real faults still fire within it plus about one poll. Do
   not set `Persistence.MinDuration` back to a lower value for a traffic-health detector.
4. Link reachability: a user journey can stay healthy while one of the services it depends on has become impossible
   to reach afresh, because the caller keeps using connections it opened earlier. For each service-to-service
   dependency the application source declares (a client dial, a configured peer address, an environment variable or
   config value naming another service and port, and the architecture summary's dependency edges), add one link to
   workloads/links.yaml: apiVersion sdo.dev/v1alpha1, kind TrafficWorkload, name `links`,
   purpose `link-probe`, and `links: [{from: <calling Service>, to: <called Service>, port: <the port the called
   Service exposes>, protocol: <how the caller talks to it>}]`. Both names must be source-backed Services in the
   manifest inventory and the port the one the caller really uses, taken from the source, not guessed. Include the
   application's own data stores; do not declare links to services the application does not run itself. Choose the
   protocol the caller uses: http (optional http.path and http.statusMin/statusMax), grpc (health check; grpc.tls
   when required), redis, memcached, mongodb, postgres or mysql; use tcp only when none fits. These probes prove the
   server answers its protocol, including auth-required replies, not that queries succeed. Optional: interval
   (default 1s), timeout (default 1s), failures (default 5, the consecutive failed fresh probes that report an
   edge). No scenarios and no generator code are involved; the
   controller installs the detector, which decides deterministically from the prober's dials and names the failing
   edge. Do not encode any particular cause of an unreachable edge.
Omit synthetic traffic only for an application that serves no HTTP."""

#: The task explicitly describes available metrics and publishing capabilities.
METRICS_AUTHORING = """Metrics (optional input). When the supplied task.metrics_source.query_url is non-empty,
a health detector may judge application metrics: import "sdo.dev/operator/sdk/metrics",
declare `Watches: []sdk.WatchKind{metrics.Watch}`, and build it with `metrics.NewThresholdDetector(spec, rules)`.
Each rule names a query ID and a PromQL expression you ground in metrics the application actually exports, a
comparison, a threshold, and the label names that identify its Deployment, Pod or Service. The controller runs
each query once per cycle; the detector never does I/O, and an unreachable source is a detector error, not a
clear. Use task.metrics_source for availability and publishing instructions; without a query URL, omit these
detectors. The shared check installs the generic
alerts detector itself when a source is configured."""


class LifecycleAgentBackend(Protocol):
    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        architecture: str,
        resources: list[TopologyResourceDTO],
        correction_feedback: str | None,
    ) -> HealthJudgeArtifact: ...


class CodexLifecycleBackend:
    """Run each lifecycle handoff as a fresh Codex CLI session."""

    provider: ClassVar[AgentProvider] = "codex"

    def __init__(
        self,
        *,
        model: str | None = None,
        reasoning_effort: str = "medium",
        timeout_seconds: int = 900,
        executor: CommandExecutor | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("lifecycle agent timeout must be positive")
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_seconds = timeout_seconds
        self.executor = executor

    def run_health_judge(
        self,
        *,
        repository: Path,
        application: str,
        health_objective: str,
        architecture: str,
        resources: list[TopologyResourceDTO],
        correction_feedback: str | None,
    ) -> HealthJudgeArtifact:
        feedback = correction_feedback or "No validator feedback."
        prompt = f"""You are the independent SDO health judge for application {application!r}.
This is one fresh authoring session in an isolated proposal checkout.
Optional human-owned goal: {health_objective or "No goal supplied; infer application health from source."}
Go-generated manifest inventory:
{architecture}
Resources: {[resource.model_dump() for resource in resources]}
Validation feedback: {feedback}

Author deterministic Go detector code and deterministic Go tests using only sdo.dev/operator/sdk's
DetectionContext snapshot methods and Kubernetes API types already available to SDO diagnostics. The application
checkout is intentionally not expected to contain the controller SDK. The trusted SDK reference is
references/sdk.md of the `sdo-detector-authoring` skill (read it from the skill directory, which is the one place
outside this checkout you may read). Use the `sdo-operationalize` skill to decide which health detectors and
synthetic traffic the application needs.

Ground the assessment in the current application checkout and the supplied SDO references. Never inspect
sibling experiments, benchmark results, hidden fault labels, or external oracles.

Author ordinary health detectors with logical IDs of your choosing. Each exports New() sdk.Detector,
uses package-level Go functions, and has matching and near-miss tests. Choose watches, intervals,
persistence and batching appropriate to the source-specific check. Use normal manifests matching Spec().
There is no required detector ID, package, goal digest, provenance echo, or refinement round.

Never read environment variables, benchmark results, SREGym data, verdict files, hidden fault labels, or any external
oracle. Never call an LLM at detector runtime. Edit the final health detector manifest, detector.go and
detector_test.go directly under
`.sdo/diagnostics/detectors/health/<id>/` in this checkout. Author the complete
`.sdo/playbooks/health-objective/README.md` with front matter: schema_version: 1, owner: responder,
fault_class: health-objective-failure, originating_incident: lifecycle-bootstrap, then repair guidance using
logical artifact IDs and parameter placeholders such as <AFFECTED_RESOURCE>. Update the playbook index
`.sdo/playbooks/README.md` to list the playbooks. Preserve existing incident detectors and all other playbooks,
human goal, architecture, outcomes, schema-version and diagnostics go.mod. Health manifests use apiVersion:
sdo.dev/v1alpha1, kind: DetectorManifest, sdkVersion: v0.1, constructor: New, class: health,
owner: health_judge, and normal Spec values (possiblePlaybooks: [health-objective] is the default).
Never put physical operational-memory paths in authored Go or narrative; use logical IDs there.
Return metadata and logical detector_ids only; the checkout carries all artifact contents.
Run `sdo-operator memory ls` to inspect existing memory and `sdo-operator memory check` after editing.
Fix every named diagnostic and rerun until it exits zero within your turn budget. The environment is already
set to SDO_ACTOR=health_judge and SDO_PHASE=initial with the accepted SDO_BASE_COMMIT; never override it.
The Go broker repeats the same validation independently. Its explicit bootstrap step installs the generic
service-endpoints, workload-health, service-reachability, cluster-wiring, workload-controllers,
warning-events, node-health, resource-saturation, runtime-crashes and traffic-* detectors
(the last from the authored workloads);
do not author those packages. service-endpoints already reports Services without ready endpoints.
service-reachability already reports Service ports that refuse connections while their pods are Ready.
workload-health already reports failed, Unknown, stuck Pending or unschedulable pods; crash-looping,
image-pull, config-error, OOMKilled, errored, waiting, restarting or unready containers; stuck init
containers; and Deployments below their desired availability or past their progress deadline.
cluster-wiring checks Service selectors and declared target ports, template dependencies and keys, PVC binding,
Ingress backends, total NetworkPolicy isolation and in-namespace ExternalName targets. workload-controllers
checks StatefulSet/DaemonSet availability and stuck revisions, failed Jobs and overdue CronJobs. warning-events
checks persistent recent mount, scheduling, backoff, probe, eviction and OOM warnings. node-health checks
application nodes for readiness and pressure. resource-saturation checks memory limits and PVC bytes/inodes
including forecasts. runtime-crashes checks newly appearing narrow runtime crash signatures with previous logs.
Do not re-implement these generic checks in health-objective or separate detectors; spend your turn on
application-specific evidence of the human objective.
Choose the checks from the human objective, the application's source and its own topology, not from a catalogue of
fault types.
Derive what a check depends on from the objects observed at detection time (the relationships the manifest inventory
and the active topology record) rather than hard-coding object names, so each check stays valid when objects are
recreated, and include matching and near-miss tests for every check.

The controller evaluates one configured runtime namespace. A source manifest namespace of "default" means the
resource will be applied into that configured namespace; it is not a literal runtime namespace. Never embed the
literal string "default" in detector source. Use DetectionContext.Namespace() whenever synthesizing an ObjectRef or
checking for a required resource that is absent, and use each observed object's Namespace for resources that exist.

{TRAFFIC_AUTHORING}
{METRICS_AUTHORING}
Write traffic generator Go and workload YAML files directly in their final directories.
The shared memory check installs and checks the generic detector for each health-probe or link-probe workload.
It compiles and tests the complete candidate, including the health detector and traffic generators.
"""
        draft, session_id = self._execute(repository, prompt, HealthJudgeDraft, sandbox="workspace-write")
        return HealthJudgeArtifact(**draft.model_dump(), session_id=session_id)

    def _execute(
        self,
        repository: Path,
        prompt: str,
        output_type: type[HealthJudgeDraft],
        *,
        sandbox: AccessMode = "read-only",
    ) -> tuple[HealthJudgeDraft, str]:
        role = f"{self.provider.capitalize()} lifecycle session"
        try:
            turn = run_structured_turn(
                self.provider,
                prompt,
                output_schema=output_type.model_json_schema(),
                cwd=repository,
                access=sandbox,
                model=self.model,
                reasoning_effort=self.reasoning_effort,
                timeout_seconds=self.timeout_seconds,
                executor=self.executor,
            )
        except StructuredTurnTimeout as exc:
            raise LifecycleAgentError(f"{role} timed out after {self.timeout_seconds}s") from exc
        except StructuredTurnError as exc:
            raise LifecycleAgentError(f"{role} failed: {exc}") from exc
        try:
            output = output_type.model_validate_json(turn.output_json)
        except ValueError as exc:
            raise LifecycleAgentError(f"invalid structured {role} output: {exc}") from exc
        return output, turn.session_id


class ClaudeLifecycleBackend(CodexLifecycleBackend):
    """Run lifecycle handoffs as fresh structured Claude Code sessions."""

    provider: ClassVar[AgentProvider] = "claude"
