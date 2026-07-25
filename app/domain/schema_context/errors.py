class SchemaContextError(ValueError):
    """Base error for deterministic schema-context processing."""


class SchemaParseError(SchemaContextError):
    """The authoritative SDL could not be parsed."""


class SchemaSelectionValidationError(SchemaContextError):
    """A semantic selection violates its bound catalog or purpose."""


class SchemaCompatibilityError(SchemaContextError):
    """The deployment attestation is incompatible with the catalog SDL."""


class QueryIntentRejected(SchemaContextError):
    """A query intent is outside the admitted operation projection."""
