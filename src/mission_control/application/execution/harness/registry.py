"""Lane registry and dispatch (SPEC-07 section 3; FT-G1).

`LaneRegistry` maps a Lane Profile to its harness. It is built once per process: the worker
composition registers the executable lanes (`deep_agents` always; Cursor profiles when a
Cursor credential is bound), the API registers describe-only entries for `lane list` and
`lane describe`. `describe` results are cached because they are pure.

Admission: a profile whose describe is not `qualified` is refused unless the application's
policy allows unqualified lanes for local proof (`allow_unqualified`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import cast

from mission_control.application.execution.harness.protocol import (
    AgentHarness,
    OperationLane,
    UnsupportedHarnessOperations,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionBinding,
    OperationExecutionRequest,
)
from mission_control.domain.execution.lanes import (
    DEFAULT_LANE_PROFILE,
    LANE_OF_PROFILE,
    LANE_PROFILES,
    RUNTIME_OF_LANE,
    LaneDescribe,
    LaneProfileName,
)

# The application policy that admits unqualified lanes for a local proof (Settings field
# `mission_control_allow_unqualified_lanes`).
UNQUALIFIED_LANES_FLAG = "MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES"


class UnknownLaneProfile(LookupError):
    """No harness is registered for the requested lane profile."""

    def __init__(self, lane_profile: str, registered: Iterable[str]) -> None:
        super().__init__(
            f"lane profile {lane_profile!r} is not registered (registered: {sorted(registered)})"
        )
        self.lane_profile = lane_profile


class LaneNotQualified(PermissionError):
    """The lane profile is registered but not qualified, and policy forbids unqualified use."""

    def __init__(self, lane_profile: str) -> None:
        super().__init__(
            f"lane profile {lane_profile!r} is not qualified; admission requires a recorded "
            "qualification (docs/qualification/lanes/README.md, "
            f"`make lane-qualify PROFILE={lane_profile} LIVE=1`) or, for a local proof, "
            f"{UNQUALIFIED_LANES_FLAG}=true on the worker that runs the lane"
        )
        self.lane_profile = lane_profile


class LaneNotExecutable(TypeError):
    """The registered entry describes a lane but cannot execute operations in this process."""


class LaneRegistry:
    def __init__(
        self, harnesses: Iterable[AgentHarness] = (), *, allow_unqualified: bool = False
    ) -> None:
        self._harnesses: dict[str, AgentHarness] = {}
        self._describes: dict[str, LaneDescribe] = {}
        self._allow_unqualified = allow_unqualified
        for harness in harnesses:
            self.register(harness)

    def register(self, harness: AgentHarness) -> None:
        describe = harness.describe()
        profile = describe.lane_profile
        if profile in self._harnesses:
            raise ValueError(f"lane profile {profile} is already registered")
        self._harnesses[profile] = harness
        self._describes[profile] = describe

    @property
    def allow_unqualified(self) -> bool:
        return self._allow_unqualified

    def profiles(self) -> tuple[str, ...]:
        return tuple(sorted(self._harnesses))

    def for_profile(self, lane_profile: str) -> AgentHarness:
        try:
            return self._harnesses[lane_profile]
        except KeyError:
            raise UnknownLaneProfile(lane_profile, self._harnesses) from None

    def describe(self, lane_profile: str) -> LaneDescribe:
        self.for_profile(lane_profile)
        return self._describes[lane_profile]

    def describe_all(self) -> tuple[LaneDescribe, ...]:
        return tuple(self._describes[profile] for profile in self.profiles())

    def admit(self, lane_profile: str) -> AgentHarness:
        """The harness for `lane_profile`, refusing an unqualified one unless policy allows."""

        harness = self.for_profile(lane_profile)
        if not self._describes[lane_profile].qualified and not self._allow_unqualified:
            raise LaneNotQualified(lane_profile)
        return harness

    def lane_for(
        self, request: OperationExecutionRequest | OperationExecutionBinding
    ) -> OperationLane:
        """Dispatch by `execution_runtime` and `lane_profile` (SPEC-07 section 3).

        `native` and `deep_agent` attempts run on the `deep_agents` lane (its runtime adapter
        executes both placements today); a `cursor` attempt runs on its Cursor profile.
        """

        profile = lane_profile_for(request)
        harness = self.admit(profile)
        if not callable(getattr(harness, "execute", None)) or not callable(
            getattr(harness, "requires_checkpoint_lineage", None)
        ):
            raise LaneNotExecutable(f"lane profile {profile} cannot execute in this process")
        return cast(OperationLane, harness)

    def as_mapping(self) -> Mapping[str, LaneDescribe]:
        return dict(self._describes)


def lane_profile_for(request: OperationExecutionRequest | OperationExecutionBinding) -> str:
    profile = request.lane_profile or DEFAULT_LANE_PROFILE
    runtime = request.execution_runtime
    lane = LANE_OF_PROFILE.get(profile)
    if lane is None:
        raise UnknownLaneProfile(profile, LANE_OF_PROFILE)
    if runtime in {"native", "deep_agent"}:
        if lane != "deep_agents":
            raise ValueError(f"a {runtime} execution runtime runs on the deep_agents lane profile")
    elif RUNTIME_OF_LANE[lane] != runtime:
        raise ValueError(f"a {runtime} execution runtime requires a {runtime} lane profile")
    return profile


class DescribeOnlyLane(UnsupportedHarnessOperations):
    """A registry entry that publishes a lane's describe in a process that cannot run it
    (the public API lists and describes lanes; workers execute them)."""

    def __init__(self, describe: LaneDescribe) -> None:
        self._describe = describe

    def describe(self) -> LaneDescribe:
        return self._describe


def describe_only_registry(
    *,
    cursor_bound: bool,
    allow_unqualified: bool,
    claude_bound: bool = False,
    codex_bound: bool = False,
) -> LaneRegistry:
    """The registry a process that lists lanes but runs none publishes (the public API).

    It mirrors the worker composition: `deep_agents` always; each bound lane's profiles as
    unqualified stubs when that provider's credential is bound (`cursor_bound`,
    `claude_bound`, `codex_bound`). Hosted profiles ride on their lane's credential.
    """

    from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES

    lanes: list[AgentHarness] = [DescribeOnlyLane(DECLARED_LANE_MATRICES["deep_agents"])]
    bound_lanes = {
        "cursor": cursor_bound,
        "claude": claude_bound,
        "codex": codex_bound,
    }
    bound_profiles: tuple[LaneProfileName, ...] = tuple(
        profile
        for profile in LANE_PROFILES
        if profile != "deep_agents" and bound_lanes[LANE_OF_PROFILE[profile]]
    )
    lanes.extend(
        DescribeOnlyLane(DECLARED_LANE_MATRICES[profile].unqualified())
        for profile in bound_profiles
    )
    return LaneRegistry(lanes, allow_unqualified=allow_unqualified)


__all__ = [
    "UNQUALIFIED_LANES_FLAG",
    "DescribeOnlyLane",
    "LaneNotExecutable",
    "LaneNotQualified",
    "LaneRegistry",
    "UnknownLaneProfile",
    "describe_only_registry",
    "lane_profile_for",
]
