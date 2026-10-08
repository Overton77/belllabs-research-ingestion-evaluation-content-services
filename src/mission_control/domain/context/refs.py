"""Durable reference forms the Context Packer reads (SPEC-02 stage handoff).

A producer's ``output_refs`` name only what the attempt registered: a captured workspace
candidate (``workspace-candidate://<candidate_id>``, custody evidence in the payload store) or a
promoted artifact (``artifact://<scope>/<run>/<artifact_id>``). Model-emitted strings that are
neither are not registered outputs.
"""

from __future__ import annotations

WORKSPACE_CANDIDATE_SCHEME = "workspace-candidate://"
ARTIFACT_SCHEME = "artifact://"


def workspace_candidate_ref(candidate_id: str) -> str:
    if not candidate_id or "/" in candidate_id:
        raise ValueError("workspace candidate ids are non-empty and contain no '/'")
    return f"{WORKSPACE_CANDIDATE_SCHEME}{candidate_id}"


def parse_workspace_candidate_ref(ref: str) -> str | None:
    if not ref.startswith(WORKSPACE_CANDIDATE_SCHEME):
        return None
    candidate_id = ref.removeprefix(WORKSPACE_CANDIDATE_SCHEME)
    return candidate_id if candidate_id and "/" not in candidate_id else None


def parse_artifact_ref(ref: str) -> tuple[str, str, str] | None:
    """``artifact://mc/<inst>/<app>/<tenant>/<run>/<artifact_id>`` -> (scope, run, artifact)."""

    if not ref.startswith(ARTIFACT_SCHEME):
        return None
    parts = ref.removeprefix(ARTIFACT_SCHEME).split("/")
    if len(parts) < 3 or not all(parts):
        return None
    return "/".join(parts[:-2]), parts[-2], parts[-1]


def durable_input_locator(object_ref: str, content_digest: str, size_bytes: int) -> str:
    """The ``<object_ref>#<sha256>:<size>`` form the workspace materializer retrieves."""

    return f"{object_ref}#{content_digest}:{size_bytes}"


def parse_durable_input_locator(locator: str) -> tuple[str, str, int]:
    """Split ``<object_ref>#<sha256:hex>:<size>`` into (object_ref, digest, size)."""

    object_ref, separator, rest = locator.partition("#")
    digest, _, size = rest.rpartition(":")
    if not object_ref or not separator or not digest or not size.isdigit():
        raise ValueError(
            "durable workspace input must be addressed as <object_ref>#<sha256>:<size>"
        )
    return object_ref, digest, int(size)
