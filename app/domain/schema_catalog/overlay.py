from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from app.domain.schema_catalog.errors import SemanticOverlayError
from app.domain.schema_catalog.models import SemanticOverlay


def load_semantic_overlay(source: bytes | str | Path) -> SemanticOverlay:
    """Load the governed JSON overlay; filesystem location is not part of its identity."""
    try:
        if isinstance(source, Path):
            payload = source.read_bytes()
        elif isinstance(source, str):
            payload = (
                source.encode("utf-8")
                if source.lstrip().startswith("{")
                else Path(source).read_bytes()
            )
        else:
            payload = source
        return SemanticOverlay.model_validate(json.loads(payload))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValidationError) as error:
        raise SemanticOverlayError(
            f"semantic overlay load failed: {type(error).__name__}"
        ) from error


def semantic_overlay_json_schema() -> dict[str, object]:
    """Return the machine-checkable contract used by authoring and CI tooling."""
    return SemanticOverlay.model_json_schema()
