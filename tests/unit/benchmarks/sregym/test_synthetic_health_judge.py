from pathlib import Path

from benchmarks.sregym.agent_script.synthetic_health_judge import record_unprobed_edges

REGION = """# inventory

<!-- sdo:application-map:begin -->
```yaml
version: 1
edges:
  - {from: web, to: api, evidence: [manifest], source: "Deployment/web env API_URL"}
  - {from: api, to: db, evidence: [manifest], source: x, unprobed: kept}
operations: []
probes: []
```
<!-- sdo:application-map:end -->
"""


def test_scripted_designer_records_every_seeded_edge_unprobed(tmp_path: Path) -> None:
    arch = tmp_path / "arch.md"
    arch.write_text(REGION)
    record_unprobed_edges(arch)
    text = arch.read_text()
    expected = '"Deployment/web env API_URL", unprobed: the scripted designer writes no probes}'
    assert "{from: web, to: api, evidence: [manifest], source: " + expected in text
    assert "unprobed: kept}" in text
    assert text.count("unprobed:") == 2
    assert text.startswith("# inventory\n")
    assert text.endswith("<!-- sdo:application-map:end -->\n")


def test_scripted_designer_leaves_a_map_with_operations_alone(tmp_path: Path) -> None:
    arch = tmp_path / "arch.md"
    arch.write_text(REGION.replace("operations: []", "operations:\n  - id: x"))
    before = arch.read_text()
    record_unprobed_edges(arch)
    assert arch.read_text() == before
