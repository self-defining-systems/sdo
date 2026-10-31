"""Versioned JSON/YAML script contract. Unknown fields fail closed."""

from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .template import validate_references, validate_step_references

Role = Literal["source-deployment", "health-judge", "responder", "reflector"]


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Command(Model):
    kind: Literal["command"]
    command: str = Field(min_length=1)
    capture_json: bool = False
    input_json: dict[str, Any] | str | None = None
    exit_code: int = 0
    contains: str | None = None
    timeout: float = Field(default=60.0, gt=0, le=300)
    attempts: int = Field(default=1, ge=1, le=60)
    interval: float = Field(default=0.0, ge=0, le=30)


class Tool(Model):
    kind: Literal["tool"]
    argv: list[str] = Field(min_length=1)
    exit_code: int = 0
    contains: str | None = None

    timeout: float = Field(default=60.0, gt=0, le=300)

    @field_validator("argv")
    @classmethod
    def tool_only(cls, value: list[str]) -> list[str]:
        if value[0] not in {"sdo", "sdo-operator"}:
            raise ValueError("tool argv must start with sdo or sdo-operator")
        return value


class FileStep(Model):
    path: str

    @field_validator("path")
    @classmethod
    def checkout_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            not value
            or path.is_absolute()
            or any(part in {"..", ".git"} for part in path.parts)
            or str(path) != value
            or value == "."
            or "\\" in value
        ):
            raise ValueError("file path must be normalized and inside the checkout, outside .git")
        return value


class Write(FileStep):
    kind: Literal["write"]
    text: str
    executable: bool = False


class Delete(FileStep):
    kind: Literal["delete"]


class Wait(Model):
    kind: Literal["wait"]
    seconds: float = Field(ge=0, le=60)


class Exit(Model):
    """End the CLI process without publishing a structured answer."""

    kind: Literal["exit"]
    code: int = Field(ge=1, le=255)


Step = Annotated[Command | Tool | Write | Delete | Wait | Exit, Field(discriminator="kind")]


class ScriptTurn(Model):
    role: Role
    incident: str | None = None
    index: int = Field(default=0, ge=0)
    steps: list[Step] = Field(default_factory=list)
    result: dict[str, Any]


class AgentScript(Model):
    version: Literal[1]
    turns: list[ScriptTurn] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_selectors(self) -> Self:
        validate_references(self.model_dump())
        for turn in self.turns:
            for index, step in enumerate(turn.steps):
                validate_step_references(step.model_dump(), limit=index)
            validate_step_references(turn.result, limit=len(turn.steps))
        keys = [(turn.role, turn.incident, turn.index) for turn in self.turns]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate turn selector")
        return self

    def select(self, role: str, incident: str | None, index: int) -> ScriptTurn:
        candidates = [
            turn for turn in self.turns if turn.role == role and turn.index == index and turn.incident == incident
        ]
        if not candidates:
            candidates = [
                turn for turn in self.turns if turn.role == role and turn.index == index and turn.incident is None
            ]
        if len(candidates) != 1:
            raise ValueError(f"expected exactly one script for {role}/{incident}/{index}")
        return candidates[0]
