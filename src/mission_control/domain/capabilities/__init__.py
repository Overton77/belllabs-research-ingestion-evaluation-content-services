"""Provider-neutral cores of the agent-composition capability kinds (ADR-0023, SPEC-01).

These modules depend on nothing but Pydantic: the host-support matrix, Capability Pins,
the Hook Event vocabulary and hook script body, Subagent Profiles and Plugin manifests.
The catalog Definitions that carry them live beside the other Definitions in
``mission_control.domain.authoring.contracts``.
"""
