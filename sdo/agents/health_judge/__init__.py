"""Built-in independent health judge agent implementation."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sdo.agents.health_judge.worker import run_judge_task

__all__ = ["run_judge_task"]


def __getattr__(name: str) -> Any:
    if name == "run_judge_task":
        from sdo.agents.health_judge.worker import run_judge_task

        return run_judge_task
    raise AttributeError(name)
