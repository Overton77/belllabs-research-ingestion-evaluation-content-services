"""MCP elicitation negotiation for the governed gateway and lane-forwarded requests (MP-11).

Elicitation is a negotiated client capability, not a universal permission gate (MCP
2025-11-25 "client/elicitation"). Facts used here come from the installed ``mcp==1.29.0``
package: ``ClientCapabilities.elicitation`` (``mcp/types.py`` ``ElicitationCapability`` with
optional ``form``/``url`` sub-capabilities; the client must support at least one mode),
``ServerSession.elicit_form``/``elicit_url`` (``mcp/server/session.py``) and
``ElicitResult.action`` in ``accept | decline | cancel`` with ``content`` only on a form
accept. The ``mcp`` client advertises both modes whenever an elicitation callback is set
(``mcp/client/session.py`` ``initialize``).

- ``negotiate_elicitation`` refuses a missing capability or mode with a typed
  ``GovernedRejected`` (``elicitation_unsupported`` / ``elicitation_mode_unsupported``).
- ``elicit_from_client`` asks the connected client and translates its answer; ``accept``,
  ``decline`` and ``cancel`` stay distinct, and ``accept`` is never proof that any downstream
  effect succeeded.
- ``to_elicit_result`` maps a broker reply for a *lane-forwarded* elicitation (origin
  ``mcp_elicitation``) back onto ``ElicitResult``: approve -> accept, deny -> decline,
  cancel and every system refusal -> cancel.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from fastmcp import Context
from mcp import types as mcp_types

from mission_control.application.execution.approvals import (
    ElicitationPrompt,
    NativeReply,
    is_secret_key,
)
from mission_control.application.execution.approvals_governed import (
    ElicitedInput,
    GovernedRejected,
)

# MCP 2025-11-25: an elicitation capability with neither sub-capability means form mode
# (backwards compatibility with 2025-06-18 clients that declared `elicitation: {}`).
_IMPLICIT_FORM: Final = "form"


def client_elicitation_modes(
    capabilities: mcp_types.ClientCapabilities | None,
) -> frozenset[str]:
    if capabilities is None or capabilities.elicitation is None:
        return frozenset()
    declared = capabilities.elicitation
    modes = {
        mode
        for mode, value in (("form", declared.form), ("url", declared.url))
        if value is not None
    }
    return frozenset(modes or {_IMPLICIT_FORM})


def context_capabilities(context: Context) -> mcp_types.ClientCapabilities | None:
    """The capabilities the connected client declared at initialize (None if unknown)."""

    try:
        params = context.session.client_params
    except RuntimeError:
        return None
    return params.capabilities if params is not None else None


def negotiate_elicitation(capabilities: mcp_types.ClientCapabilities | None, mode: str) -> None:
    modes = client_elicitation_modes(capabilities)
    if not modes:
        raise GovernedRejected(
            "elicitation_unsupported", "the MCP client did not declare the elicitation capability"
        )
    if mode not in modes:
        raise GovernedRejected(
            "elicitation_mode_unsupported",
            f"the MCP client supports elicitation modes {sorted(modes)}, not {mode}",
        )


def translate_elicit_result(
    result: mcp_types.ElicitResult, prompt: ElicitationPrompt
) -> ElicitedInput:
    """accept / decline / cancel stay distinct; accepted form content is schema-checked."""

    if result.action == "decline":
        return ElicitedInput("decline")
    if result.action == "cancel":
        return ElicitedInput("cancel")
    content: Mapping[str, Any] | None = result.content
    if prompt.mode == "url":
        # URL mode: consent to navigate; the interaction itself stays out of band.
        return ElicitedInput("accept", None)
    content = dict(content or {})
    unknown = set(content) - prompt.property_names()
    secret = sorted(name for name in content if is_secret_key(name))
    if unknown or secret:
        raise GovernedRejected(
            "invalid_arguments",
            "elicited content names fields outside the requested schema or credentials",
        )
    return ElicitedInput("accept", content)


async def elicit_from_client(
    context: Context, prompt: ElicitationPrompt, *, elicitation_id: str
) -> ElicitedInput:
    """Negotiate, then ask the connected client; never assumes the capability."""

    negotiate_elicitation(context_capabilities(context), prompt.mode)
    session = context.session
    related = context.request_id
    if prompt.mode == "form":
        assert prompt.requested_schema is not None
        result = await session.elicit_form(
            prompt.message, prompt.requested_schema, related_request_id=related
        )
    else:
        assert prompt.url is not None
        result = await session.elicit_url(
            prompt.message, prompt.url, elicitation_id, related_request_id=related
        )
    return translate_elicit_result(result, prompt)


def to_elicit_result(reply: NativeReply) -> mcp_types.ElicitResult:
    """A broker reply for a lane-forwarded elicitation, as the MCP result the lane returns."""

    action = reply.elicitation_action or "cancel"
    if reply.reason != "human_decision":
        action = "cancel"
    return mcp_types.ElicitResult(
        action=action,
        content=reply.elicitation_content if action == "accept" else None,
    )


__all__ = [
    "client_elicitation_modes",
    "context_capabilities",
    "elicit_from_client",
    "negotiate_elicitation",
    "to_elicit_result",
    "translate_elicit_result",
]
