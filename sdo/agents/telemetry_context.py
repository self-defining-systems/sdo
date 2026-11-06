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
    return "You may bind more sources in .sdo/diagnostics/telemetry/sources.yaml." + reads
