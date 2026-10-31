from sdo.agents.metrics_context import metrics_context
from sdo.contracts import MetricsSource


def test_metrics_capabilities_are_explicit():
    assert "not configured" in metrics_context(MetricsSource())
    external = metrics_context(MetricsSource(query_url="http://external"))
    assert "http://external" in external
    assert "prometheus.io/scrape" not in external
    owned = metrics_context(
        MetricsSource(
            query_url="http://owned",
            annotated_pod_scraping=True,
            otlp_metrics_url="http://owned/api/v1/otlp/v1/metrics",
        )
    )
    for text in (
        "prometheus.io/scrape",
        "prometheus.io/port",
        "prometheus.io/path",
        "http://owned/api/v1/otlp/v1/metrics",
        "source-repair",
    ):
        assert text in owned
