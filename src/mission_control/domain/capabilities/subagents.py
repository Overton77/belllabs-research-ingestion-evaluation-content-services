"""``mc.subagent_profile.v1``: a provider-neutral delegate definition (SPEC-01).

Defined once, projected as ``.cursor/agents/<name>.md``, ``.claude/agents/<name>.md``,
``.codex/agents/<name>.md`` or a Deep Agents ``SubAgent`` dict.
"""

from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SUBAGENT_PROFILE_SCHEMA: Final = "mc.subagent_profile.v1"

# Names a lane already reserves for its built-in agents; a profile may not shadow them.
BUILTIN_SUBAGENT_NAMES = frozenset(
    {
        "explore",
        "shell",
        "bash",
        "browser",
        "debug",
        "computerUse",
        "cursorGuide",
        "general-purpose",
    }
)
_BUILTIN_FOLDED = frozenset(name.lower() for name in BUILTIN_SUBAGENT_NAMES)


class SubagentProfile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["mc.subagent_profile.v1"] = SUBAGENT_PROFILE_SCHEMA
    name: str = Field(min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    description: str = Field(min_length=1, max_length=1000)
    prompt: str | None = Field(default=None, min_length=1, max_length=8192)
    prompt_bundle_ref: str | None = Field(default=None, min_length=1)
    model: str = Field(default="inherit", min_length=1)
    tools: tuple[str, ...] = ()
    skills: tuple[str, ...] = ()
    mcp_servers: tuple[str, ...] = ()
    readonly: bool = False
    background: bool = False
    context_mode: Literal["isolated", "fork"] = "isolated"
    interrupt_on: tuple[str, ...] = ()
    max_turns: int | None = Field(default=None, ge=1, le=1000)

    @field_validator("name")
    @classmethod
    def not_a_lane_builtin(cls, value: str) -> str:
        if value.lower() in _BUILTIN_FOLDED:
            raise ValueError(f"subagent name {value!r} collides with a lane built-in agent")
        return value

    @model_validator(mode="after")
    def exactly_one_prompt(self) -> SubagentProfile:
        if (self.prompt is None) == (self.prompt_bundle_ref is None):
            raise ValueError("a subagent profile needs exactly one of prompt or prompt_bundle_ref")
        return self
