"""Render the application telemetry a typed task grants, and what the reader may do with it."""

from typing import Literal

from sdo.contracts import TelemetryContext, TelemetrySource

Reader = Literal["designer", "responder"]

_INSTRUMENTATION = (
    "Needed instrumentation is a source repair a responder captures as a patch (sdo-patching); "
    "never edit application source; use sdo-operationalize for it."
)


def telemetry_context(context: TelemetryContext, reader: Reader = "designer") -> str:
    """The task's telemetry sources, dialects, granted capabilities, mode and the reader's authority.

    The designer (health judge) may bind sources only while the mode is open. Responders always
    receive the bindings read-only.
    """

    mode = context.mode or "open"
    if mode == "floor":
        return (
            "Task telemetry (mode floor): no application telemetry sources; "
            "the Kubernetes API and SDO probes only; omit telemetry detectors."
        )
    if not context.sources:
        return f"Task telemetry (mode {mode}): no source is bound; omit telemetry detectors. " + _authority(
            mode, reader, context
        )
    parts = [f"Task telemetry (mode {mode})."]
    parts.extend(_source_line(source) for source in context.sources)
    parts.append(_authority(mode, reader, context))
    parts.append(_INSTRUMENTATION)
    return " ".join(parts)


def _source_line(source: TelemetrySource) -> str:
    traits = [", ".join(source.kinds) or "no kinds", source.dialect or source.adapter]
    if source.default:
        traits.append("default")
    if source.capabilities:
        traits.append("+".join(source.capabilities))
    if source.endpoint:
        traits.append(source.endpoint)
    if source.origin == "operator":
        traits.append("operator-provided")
    line = f"Source {source.id}: {', '.join(traits)}."
    if source.publish.scrape_annotations:
        line += (
            ' Pods annotated prometheus.io/scrape "true", prometheus.io/port "<metrics port>" '
            'and prometheus.io/path "/metrics" are scraped.'
        )
    if source.publish.otlp_metrics_url:
        line += f" OTLP HTTP metrics URL: {source.publish.otlp_metrics_url}."
    return line


def _authority(mode: str, reader: Reader, context: TelemetryContext) -> str:
    reads = f" Cluster reads: {', '.join(context.capabilities)}." if context.capabilities else ""
    if reader == "responder":
        return "Bindings are read-only." + reads
    if mode == "pinned":
        return "Sources are pinned: query them, never write bindings." + reads
    return _designer_open(context, reads)


def _designer_open(context: TelemetryContext, reads: str) -> str:
    schema = "schema: sdo-operationalize references/telemetry.md"
    if any(source.origin == "operator" for source in context.sources):
        bind = (
            "Operator-provided sources are already bound: name them by id in detector queries; "
            "do not redefine them or write .sdo/diagnostics/telemetry/ for them. Bind another source in "
            f".sdo/diagnostics/telemetry/sources.yaml only when no listed source can answer ({schema})."
        )
    else:
        bind = (
            "Bind a source in .sdo/diagnostics/telemetry/sources.yaml only when no listed source can answer "
            f"({schema})."
        )
    return bind + reads + _enforced(context)


def _enforced(context: TelemetryContext) -> str:
    """What the broker's check enforces on the designer's binding and backends files."""

    if context.recipes:
        offered = ", ".join(
            recipe.name + (f" (declares {'+'.join(recipe.declares)})" if recipe.declares else "")
            for recipe in context.recipes
        )
        parts = [f" Backend recipes offered for .sdo/diagnostics/telemetry/backends.yaml: {offered}; no parameters."]
    else:
        parts = [" No backend recipe is offered: do not write .sdo/diagnostics/telemetry/backends.yaml."]
    grants = []
    if context.granted_namespaces:
        grants.append("namespaces " + ", ".join(context.granted_namespaces))
    if context.granted_hosts:
        grants.append("external hosts " + ", ".join(context.granted_hosts))
    if context.granted_credentials:
        grants.append("credentials " + ", ".join(context.granted_credentials))
    if grants:
        parts.append(" Binding endpoints may name only: " + "; ".join(grants) + ".")
    else:
        parts.append(" The environment grants no endpoint for a designer binding.")
    return "".join(parts)
