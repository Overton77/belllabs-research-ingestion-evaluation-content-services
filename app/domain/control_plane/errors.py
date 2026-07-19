from __future__ import annotations


class ControlPlaneError(Exception):
    code = "control_plane_error"
    status_code = 400

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class DefinitionNotFound(ControlPlaneError):
    code = "definition_not_found"
    status_code = 404


class DefinitionConflict(ControlPlaneError):
    code = "definition_conflict"
    status_code = 409


class ReferenceMismatch(ControlPlaneError):
    code = "reference_mismatch"


class RetiredDefinition(ControlPlaneError):
    code = "retired_definition"
    status_code = 409


class CompilationRejected(ControlPlaneError):
    code = "compilation_rejected"
    status_code = 422

    def __init__(self, message: str, decisions: tuple[object, ...] = ()) -> None:
        super().__init__(message)
        self.decisions = decisions


class UnsupportedExtension(CompilationRejected):
    code = "unsupported_extension"


class PayloadIntegrityError(ControlPlaneError):
    code = "payload_integrity_error"
    status_code = 500
