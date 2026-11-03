from benchmarks.sregym.analysis.regrade import regrade_row


def test_saved_receipt_restores_path_but_never_invents_field_evidence():
    row = {
        "injection": {"state": "injected", "started_at": "2026-01-01T00:00:00Z"},
        "detected_at": "2026-01-01T00:00:10Z",
        "resolved_at": "2026-01-01T00:00:30Z",
        "fault_components": [{"fault": 0, "incident": 0, "problem_id": "synthetic", "injection_order": 0}],
        "resolution_path": None,
    }
    result = regrade_row(row, {"resolution_path": "deterministic"})
    assert result["resolution_path"] == "deterministic"
    assert result["faults_unknown"] == 1
    assert result["faults_reverted"] == 0
    assert result["injection_to_resolution_s"] == 30
    assert result["detection_to_resolution_s"] == 20
