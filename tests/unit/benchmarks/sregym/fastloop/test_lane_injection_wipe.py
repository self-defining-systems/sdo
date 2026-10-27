"""Lane facts, rather than copied journals, decide whether application data can be reused."""

from pathlib import Path

import pytest

from benchmarks.sregym.fastloop.cli import _application_needs_redeploy
from benchmarks.sregym.fastloop.fault_driver import SregymFaultDriver
from benchmarks.sregym.fastloop.wipe import AttemptMarker, LaneInjectionState, tier2_reason
from benchmarks.sregym.faults.probe_catalog import DATA_LEVEL_CLASSES
from tests.unit.benchmarks.sregym.fastloop.test_fastloop_fault_driver import FakeWorker
from tests.unit.benchmarks.sregym.fastloop.test_wipe import faulted_lane, tier2


@pytest.mark.parametrize("current", [(), ("mongo: revoke_auth_mongodb:rate-db",)])
def test_real_injection_survives_seeded_journal_and_run_directory_switch(tmp_path: Path, current) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")
    driver = SregymFaultDriver(
        FakeWorker(),
        namespace="hotel",
        injection_state=state,
        data_faults={"mongo": ("revoke_auth_mongodb:rate-db",)},
    )
    driver.inject("mongo")
    driver.recover()  # Verified harness recovery does not prove PVC data was reset.
    # Repeat-only reruns seed their journal from another run. That is not lane history.
    (tmp_path / "journal.json").write_text('{"previous": "clean-selector-row"}')
    reason = tier2_reason(
        AttemptMarker(tmp_path / "repeat-only"),
        lambda: None,
        list,
        lane_state=LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel"),
        current_data_faults=current,
    )
    receipt, deployed = tier2(faulted_lane(), reason)
    assert receipt.tier2_ran
    assert len(deployed) == 1
    assert receipt.tier2_trigger == ("current_row_data_faults" if current else "lane_last_injection_data_faults")


def test_non_data_injection_on_known_lane_keeps_warm_app(tmp_path: Path) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")
    SregymFaultDriver(FakeWorker(), namespace="hotel", injection_state=state, data_faults={"selector": ()}).inject(
        "selector"
    )
    assert tier2_reason(AttemptMarker(tmp_path / "other-run"), lambda: None, list, lane_state=state) is None


@pytest.mark.parametrize("contents", [None, "broken", "{}", '{"version": 99}'])
def test_unknown_lane_history_forces_recreation(tmp_path: Path, contents: str | None) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")
    if contents is not None:
        state.path.write_text(contents)
    reason = tier2_reason(AttemptMarker(tmp_path), lambda: None, list, lane_state=state)
    assert reason is not None
    assert reason.trigger.value == "lane_injection_unknown"


def test_request_failure_leaves_unknown_lane_state_before_side_effects(tmp_path: Path) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")

    class FailingWorker(FakeWorker):
        def request(self, op, **kwargs):
            assert state.reason().trigger.value == "lane_injection_unknown"
            raise RuntimeError("worker died after applying the fault")

    with pytest.raises(RuntimeError):
        SregymFaultDriver(
            FailingWorker(), namespace="hotel", injection_state=state, data_faults={"selector": ()}
        ).inject("selector")
    assert state.reason().trigger.value == "lane_injection_unknown"


def test_incoming_data_fault_forces_tier2_even_after_clean_injection(tmp_path: Path) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")
    SregymFaultDriver(FakeWorker(), namespace="hotel", injection_state=state, data_faults={"selector": ()}).inject(
        "selector"
    )
    reason = tier2_reason(
        AttemptMarker(tmp_path),
        lambda: None,
        list,
        lane_state=state,
        current_data_faults=("mongo: revoke_auth_mongodb:rate-db",),
    )
    receipt, _ = tier2(faulted_lane(), reason)
    assert receipt.tier2_trigger == "current_row_data_faults"
    assert receipt.current_row_data_faults == ("mongo: revoke_auth_mongodb:rate-db",)


@pytest.mark.parametrize("class_name", sorted(DATA_LEVEL_CLASSES))
def test_every_catalog_data_class_flows_from_real_driver_to_wipe(tmp_path: Path, class_name: str) -> None:
    from benchmarks.sregym.faults.description import FaultSpec, ProblemDescription
    from benchmarks.sregym.faults.probe_catalog import CLASS_KINDS

    description = ProblemDescription(
        problem_id="data",
        composite=False,
        faults=(
            FaultSpec(kind=CLASS_KINDS[class_name], component="synthetic-db", class_name=class_name, declared_index=0),
        ),
        injection_order=(0,),
    )
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")
    SregymFaultDriver(
        FakeWorker(), namespace="hotel", injection_state=state, data_faults={"data": description.data_level_faults()}
    ).inject("data")
    receipt, _ = tier2(faulted_lane(), tier2_reason(AttemptMarker(tmp_path), lambda: None, list, lane_state=state))
    assert receipt.tier2_trigger == "lane_last_injection_data_faults"
    assert receipt.previous_row_data_faults == tuple(f"data: {name}" for name in description.data_level_faults())


def test_composite_records_all_components_as_one_lane_injection(tmp_path: Path) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")

    class CompositeWorker(FakeWorker):
        def request(self, op, **kwargs):
            reply = super().request(op, **kwargs)
            position = int(kwargs.get("compose", False))
            return {**reply, "fault": position, "faults": [position]}

    driver = SregymFaultDriver(
        CompositeWorker(),
        namespace="hotel",
        injection_state=state,
        data_faults={"data": ("revoke_auth_mongodb:rate-db",), "selector": ()},
    )
    driver.inject_composite(("data", "selector"))
    assert state.reason().data_faults == ("data: revoke_auth_mongodb:rate-db",)
    # A later clean injection replaces, rather than appends to, the lane history.
    driver.inject("selector")
    assert state.reason() is None


def test_cli_row_start_uses_lane_facts_and_incoming_faults(tmp_path: Path) -> None:
    from types import SimpleNamespace

    environment = SimpleNamespace(kubeconfig=tmp_path / "lane.kubeconfig", namespace="hotel")
    marker = AttemptMarker(tmp_path / "seeded-repeat")
    reason = _application_needs_redeploy(environment, marker, ("data: revoke_auth_mongodb:rate-db",))
    assert reason.trigger.value == "current_row_data_faults"
    reason = _application_needs_redeploy(environment, marker)
    assert reason.trigger.value == "lane_injection_unknown"


def test_undescribed_injection_cannot_retain_an_older_clean_record(tmp_path: Path) -> None:
    state = LaneInjectionState(tmp_path / "lane.kubeconfig", "hotel")
    driver = SregymFaultDriver(FakeWorker(), namespace="hotel", injection_state=state, data_faults={"selector": ()})
    driver.inject("selector")
    assert state.reason() is None
    driver.inject("not-in-catalog")
    assert state.reason().trigger.value == "lane_injection_unknown"
