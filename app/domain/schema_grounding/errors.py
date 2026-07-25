from __future__ import annotations


class SchemaGroundingError(Exception):
    code = "schema_grounding_error"
    status_code = 422

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class SchemaSourceDigestMismatch(SchemaGroundingError):
    code = "schema_source_digest_mismatch"


class CatalogNondeterministic(SchemaGroundingError):
    code = "catalog_nondeterministic"
    status_code = 500


class CatalogPublicationConflict(SchemaGroundingError):
    code = "catalog_publication_conflict"
    status_code = 409


class SchemaGroundingRecordNotFound(SchemaGroundingError):
    code = "schema_grounding_record_not_found"
    status_code = 404


class SchemaDeploymentMismatch(SchemaGroundingError):
    code = "schema_deployment_mismatch"
    status_code = 409


class GraphCapabilityDenied(SchemaGroundingError):
    code = "graph_capability_denied"
    status_code = 403
