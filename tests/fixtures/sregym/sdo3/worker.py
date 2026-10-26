"""Real SREGym v2 worker/composite with deterministic fake platform I/O."""

import contextlib
import sys
from types import SimpleNamespace

from sregym.conductor.problems.base import Problem
from sregym.conductor.problems.colocated_composite import ColocatedComposite
from sregym.conductor.problems.composition.spec import FaultDraw, Footprint, Tier
from sregym.fastloop.created_objects import ClusterObject
from sregym.fastloop.worker import FastloopWorker, serve
from sregym.utils.decorators import mark_fault_injected

EVENTS = []


class Inventory:
    """In-memory cluster-scoped objects; a lane starts with only its app namespace."""

    def __init__(self):
        self.objects = {ClusterObject("Namespace", "hotel-reservation")}

    def list_objects(self):
        return frozenset(self.objects)

    def delete(self, obj):
        self.objects.discard(obj)


INVENTORY = Inventory()


class Fault(Problem):
    def __init__(self, name, invalid=False, broken_cleanup=False):
        super().__init__(SimpleNamespace(namespace="hotel-reservation", name="Hotel Reservation"))
        self.name = self.root_cause = name
        self.invalid = invalid
        self.repaired = False
        self.broken_cleanup = broken_cleanup
        self.mitigation_oracle = SimpleNamespace(evaluate=self.grade, scope_to_fault=lambda: None)

    def grade(self):
        EVENTS.append(["oracle", self.name])
        return {"success": self.repaired}

    def inject_fault(self):
        EVENTS.append(["inject", self.name])
        if self.name == "installs-controller":
            # Like IngressMisroute: a controller in its own namespace that recover_fault leaves.
            INVENTORY.objects |= {ClusterObject("Namespace", "ingress-nginx"), ClusterObject("IngressClass", "nginx")}

    def injection_failure(self):
        return {"synthetic_invalid": True} if self.invalid else None

    @mark_fault_injected
    def recover_fault(self):
        EVENTS.append(["recover", self.name])
        if self.broken_cleanup:
            raise RuntimeError("synthetic cleanup failure")
        self.repaired = True


def factory(problem_id):
    if problem_id != "nested":
        return Fault(problem_id, invalid=problem_id == "invalid", broken_cleanup=problem_id == "broken")
    parts = [Fault("declared-first"), Fault("injected-first")]
    draws = [
        FaultDraw("first", "hotel_reservation", (), (Footprint("Deployment", "geo"),), Tier.ORDINARY),
        FaultDraw("second", "hotel_reservation", (), (Footprint("Deployment", "rate"),), Tier.RESTARTS_PODS),
    ]
    result = ColocatedComposite(
        parts, draws=draws, cluster=SimpleNamespace(kubectl=object(), read_specs=lambda ns: ({}, {}))
    )
    result.inject_pause_s = result.liveness_settle_s = 0
    return result


class Worker(FastloopWorker):
    def handle(self, request):
        if request["op"] == "events":
            return {"events": EVENTS.copy()}
        if request["op"] == "inventory":
            return {"objects": sorted(str(obj) for obj in INVENTORY.list_objects())}
        return super().handle(request)


if __name__ == "__main__":
    replies = sys.stdout
    with contextlib.redirect_stdout(sys.stderr):
        serve(
            Worker(
                problem_factory=factory,
                deployer=lambda *args: None,
                health_probe=lambda ns: {"healthy": True},
                cluster_factory=lambda *args: ("fake", "fake"),
                cluster_inventory=INVENTORY,
            ),
            sys.stdin,
            replies,
        )
