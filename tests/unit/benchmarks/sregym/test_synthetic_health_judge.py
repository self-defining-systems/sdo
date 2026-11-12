from pathlib import Path

from benchmarks.sregym.agent_script.synthetic_health_judge import record_unprobed_components

REGION = """# inventory

<!-- sdo:application-map:begin -->
```yaml
version: 1
components:
  - {name: web, workload: "Deployment/web", services: ["web"], holds_data: false, evidence: [manifest]}
  - {name: db, unprobed: kept, workload: "StatefulSet/db", services: ["db"], holds_data: true, data: {engine: postgres}}
edges:
  - {from: web, to: db, evidence: [manifest], source: "Deployment/web env DB"}
operations: []
probes: []
```
<!-- sdo:application-map:end -->
"""


def test_scripted_designer_records_every_seeded_component_unprobed(tmp_path: Path) -> None:
    arch = tmp_path / "arch.md"
    arch.write_text(REGION)
    record_unprobed_components(arch)
    text = arch.read_text()
    assert "  - {name: web, unprobed: the scripted designer writes no probes, workload: " in text
    assert "{name: db, unprobed: kept, workload:" in text
    assert text.count("unprobed:") == 2
    assert '{from: web, to: db, evidence: [manifest], source: "Deployment/web env DB"}' in text
    assert text.startswith("# inventory\n")
    assert text.endswith("<!-- sdo:application-map:end -->\n")


def test_scripted_designer_leaves_a_map_with_operations_alone(tmp_path: Path) -> None:
    arch = tmp_path / "arch.md"
    arch.write_text(REGION.replace("operations: []", "operations:\n  - id: x"))
    before = arch.read_text()
    record_unprobed_components(arch)
    assert arch.read_text() == before
