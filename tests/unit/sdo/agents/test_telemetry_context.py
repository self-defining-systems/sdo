from sdo.agents.telemetry_context import telemetry_context
from sdo.contracts import TelemetryContext, TelemetryPublish, TelemetrySource


def _prom(**kwargs) -> TelemetrySource:
    return TelemetrySource(
        id="prom",
        kinds=["metrics"],
        adapter="prometheus",
        dialect="PromQL (Prometheus HTTP API v1)",
        default=True,
        capabilities=["alerts", "range"],
        origin="operator",
        endpoint="http://prom:9090",
        **kwargs,
    )


def test_unbound_and_floor_contexts_say_to_omit_telemetry_detectors():
    assert "no source is bound" in telemetry_context(TelemetryContext(mode="open"))
    floor = telemetry_context(TelemetryContext(mode="floor", sources=[_prom()]))
    assert "no application telemetry" in floor
    assert "prom" not in floor


def test_sources_carry_dialect_capabilities_and_publishing_blocks():
    text = telemetry_context(
        TelemetryContext(
            mode="open",
            capabilities=["pod-discovery"],
            sources=[
                _prom(
                    publish=TelemetryPublish(
                        scrape_annotations=True, otlp_metrics_url="http://prom:9090/api/v1/otlp/v1/metrics"
                    )
                )
            ],
        )
    )
    for expected in (
        "Source prom",
        "PromQL (Prometheus HTTP API v1)",
        "alerts+range",
        "default",
        "prometheus.io/scrape",
        "prometheus.io/port",
        "prometheus.io/path",
        "http://prom:9090/api/v1/otlp/v1/metrics",
        "pod-discovery",
        ".sdo/diagnostics/telemetry/sources.yaml",
        "source repair",
    ):
        assert expected in text
    assert "prometheus.io/scrape" not in telemetry_context(TelemetryContext(mode="open", sources=[_prom()]))


def test_reader_authority_follows_mode_and_role():
    open_context = TelemetryContext(mode="open", sources=[_prom()])
    assert "You may bind more sources" in telemetry_context(open_context)
    pinned = telemetry_context(TelemetryContext(mode="pinned", sources=[_prom()]))
    assert "never write bindings" in pinned
    assert "may bind" not in pinned
    for mode in ("open", "pinned"):
        responder = telemetry_context(TelemetryContext(mode=mode, sources=[_prom()]), "responder")
        assert "Bindings are read-only" in responder
        assert "sources.yaml" not in responder
