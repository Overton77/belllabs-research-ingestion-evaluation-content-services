from __future__ import annotations


class CatalogCoreError(ValueError):
    """Base error for deterministic schema-catalog compilation."""


class CatalogParseError(CatalogCoreError):
    """The authoritative GraphQL SDL could not be parsed."""


class SemanticOverlayError(CatalogCoreError):
    """The governed semantic overlay is malformed or incompatible with the SDL."""


class CatalogValidationError(CatalogCoreError):
    """The parsed catalog violates a catalog invariant."""
