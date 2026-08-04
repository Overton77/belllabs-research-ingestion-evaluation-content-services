from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from app.domain.control_plane.canonical import sha256_digest

PayloadT = TypeVar("PayloadT")


class SemanticInputPayload(BaseModel):
    """Canonical, immutable JSON input consumed by one exact semantic handler."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_ref: str = Field(min_length=1)
    payload_json: str = Field(min_length=2)
    payload_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_canonical_payload(self) -> SemanticInputPayload:
        value = json.loads(self.payload_json)
        canonical = _canonical_payload_json(value)
        if canonical != self.payload_json:
            raise ValueError("semantic input payload must use canonical JSON encoding")
        if sha256_digest(value) != self.payload_digest:
            raise ValueError("semantic input payload digest mismatch")
        return self

    @classmethod
    def from_value(cls, *, schema_ref: str, value: Any) -> SemanticInputPayload:
        payload_json = _canonical_payload_json(value)
        return cls(
            schema_ref=schema_ref,
            payload_json=payload_json,
            payload_digest=sha256_digest(value),
        )

    def decode(self, adapter: TypeAdapter[PayloadT]) -> PayloadT:
        return adapter.validate_json(self.payload_json)


class SemanticHandlerBinding(BaseModel):
    """Exact handler revision and its frozen, schema-bound semantic input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    handler_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    handler_revision: int = Field(ge=1)
    input: SemanticInputPayload
    output_contract_ref: str = Field(min_length=1)
    operation_execution_binding_ref: str | None = Field(default=None, min_length=1)

    @property
    def exact_handler_ref(self) -> str:
        return f"{self.handler_id}@{self.handler_revision}"


class StageHandlerBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    stage_id: str = Field(min_length=1)
    handler: SemanticHandlerBinding


class GoalOperationHandlerBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_class: str = Field(min_length=1)
    handler: SemanticHandlerBinding


class RunSemanticInputBinding(BaseModel):
    """One immutable, run-scoped routing document frozen before Temporal launch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_id: str = Field(pattern=r"^semantic-binding:[0-9a-f]{64}$")
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    blueprint_family: Literal["StageGraph", "GoalDirected"]
    effective_configuration_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    blueprint_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    stage_handlers: tuple[StageHandlerBinding, ...] = ()
    workflow_evaluator: SemanticHandlerBinding | None = None
    goal_operation_handlers: tuple[GoalOperationHandlerBinding, ...] = ()
    goal_verifier: SemanticHandlerBinding | None = None
    goal_handoff: SemanticHandlerBinding | None = None
    operation_execution_binding_refs: tuple[str, ...] = ()
    created_at: datetime
    binding_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_binding(self) -> RunSemanticInputBinding:
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("semantic input binding creation time must be timezone-aware")
        expected_id = semantic_binding_id(
            self.request_scope,
            self.run_id,
            self.effective_configuration_digest,
            self.blueprint_digest,
        )
        if self.binding_id != expected_id:
            raise ValueError("semantic input binding identity does not match its run authority")
        if sha256_digest(_binding_content(self)) != self.binding_digest:
            raise ValueError("semantic input binding digest mismatch")

        stage_ids = [item.stage_id for item in self.stage_handlers]
        operation_classes = [item.operation_class for item in self.goal_operation_handlers]
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("semantic input binding contains duplicate stage handlers")
        if len(operation_classes) != len(set(operation_classes)):
            raise ValueError("semantic input binding contains duplicate goal operation handlers")
        expected_operation_refs = _operation_execution_binding_refs(self)
        if self.operation_execution_binding_refs != expected_operation_refs:
            raise ValueError(
                "semantic input binding Operation Execution Binding references "
                "differ from its exact handlers"
            )
        if self.blueprint_family == "StageGraph":
            if not self.stage_handlers:
                raise ValueError("StageGraph semantic binding requires stage handlers")
            if (
                self.goal_operation_handlers
                or self.goal_verifier is not None
                or self.goal_handoff is not None
            ):
                raise ValueError("StageGraph semantic binding cannot contain GoalDirected handlers")
        else:
            if self.stage_handlers or self.workflow_evaluator is not None:
                raise ValueError("GoalDirected semantic binding cannot contain StageGraph handlers")
            if (
                not self.goal_operation_handlers
                or self.goal_verifier is None
                or self.goal_handoff is None
            ):
                raise ValueError(
                    "GoalDirected semantic binding requires operation, verifier, "
                    "and handoff handlers"
                )
        return self

    @classmethod
    def create(
        cls,
        *,
        request_scope: str,
        run_id: str,
        blueprint_family: Literal["StageGraph", "GoalDirected"],
        effective_configuration_digest: str,
        blueprint_digest: str,
        created_at: datetime,
        stage_handlers: tuple[StageHandlerBinding, ...] = (),
        workflow_evaluator: SemanticHandlerBinding | None = None,
        goal_operation_handlers: tuple[GoalOperationHandlerBinding, ...] = (),
        goal_verifier: SemanticHandlerBinding | None = None,
        goal_handoff: SemanticHandlerBinding | None = None,
    ) -> RunSemanticInputBinding:
        values: dict[str, Any] = {
            "binding_id": semantic_binding_id(
                request_scope,
                run_id,
                effective_configuration_digest,
                blueprint_digest,
            ),
            "request_scope": request_scope,
            "run_id": run_id,
            "blueprint_family": blueprint_family,
            "effective_configuration_digest": effective_configuration_digest,
            "blueprint_digest": blueprint_digest,
            "stage_handlers": stage_handlers,
            "workflow_evaluator": workflow_evaluator,
            "goal_operation_handlers": goal_operation_handlers,
            "goal_verifier": goal_verifier,
            "goal_handoff": goal_handoff,
            "operation_execution_binding_refs": tuple(
                sorted(
                    {
                        route.operation_execution_binding_ref
                        for route in (
                            *(item.handler for item in stage_handlers),
                            *(item.handler for item in goal_operation_handlers),
                            *((workflow_evaluator,) if workflow_evaluator is not None else ()),
                            *((goal_verifier,) if goal_verifier is not None else ()),
                            *((goal_handoff,) if goal_handoff is not None else ()),
                        )
                        if route.operation_execution_binding_ref is not None
                    }
                )
            ),
            "created_at": created_at,
        }
        values["binding_digest"] = sha256_digest(values)
        return cls.model_validate(values)


def semantic_binding_id(
    request_scope: str,
    run_id: str,
    effective_configuration_digest: str,
    blueprint_digest: str,
) -> str:
    digest = sha256_digest(
        {
            "request_scope": request_scope,
            "run_id": run_id,
            "effective_configuration_digest": effective_configuration_digest,
            "blueprint_digest": blueprint_digest,
        }
    )
    return f"semantic-binding:{digest.removeprefix('sha256:')}"


def _binding_content(binding: RunSemanticInputBinding) -> dict[str, Any]:
    return binding.model_dump(mode="python", exclude={"binding_digest"})


def _operation_execution_binding_refs(
    binding: RunSemanticInputBinding,
) -> tuple[str, ...]:
    routes = (
        *(item.handler for item in binding.stage_handlers),
        *(item.handler for item in binding.goal_operation_handlers),
        *((binding.workflow_evaluator,) if binding.workflow_evaluator is not None else ()),
        *((binding.goal_verifier,) if binding.goal_verifier is not None else ()),
        *((binding.goal_handoff,) if binding.goal_handoff is not None else ()),
    )
    return tuple(
        sorted(
            {
                route.operation_execution_binding_ref
                for route in routes
                if route.operation_execution_binding_ref is not None
            }
        )
    )


def _canonical_payload_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
