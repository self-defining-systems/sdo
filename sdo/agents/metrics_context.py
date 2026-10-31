"""Render only the metrics capabilities explicitly granted by a typed task."""

from sdo.contracts import MetricsSource


def metrics_context(source: MetricsSource) -> str:
    if not source.query_url:
        return "Task metrics_source: metrics source not configured; omit metrics detectors."
    parts = [f"Task metrics_source: query URL {source.query_url}."]
    if source.annotated_pod_scraping:
        parts.append(
            'SDO Prometheus scrapes application pods annotated prometheus.io/scrape: "true", '
            'prometheus.io/port: "<metrics port>", and prometheus.io/path: "/metrics" (or your export path).'
        )
    if source.otlp_metrics_url:
        parts.append(f"Publish OTLP HTTP metrics to {source.otlp_metrics_url} (full metrics endpoint).")
    parts.append(
        "Add needed instrumentation to application source through the normal source-repair path; "
        "use sdo-operationalize for instrumentation and sdo.dev/operator/sdk/metrics for deterministic detectors."
    )
    return " ".join(parts)
