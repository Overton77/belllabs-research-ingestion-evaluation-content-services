from __future__ import annotations


class RunControlError(Exception):
    code = "run_control_error"
    status_code = 400

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class RunControlNotFound(RunControlError):
    code = "run_control_not_found"
    status_code = 404


class IdempotencyConflict(RunControlError):
    code = "idempotency_conflict"
    status_code = 409


class RunVersionConflict(RunControlError):
    code = "stale_run_version"
    status_code = 409


class AdmissionRejected(RunControlError):
    code = "admission_rejected"
    status_code = 422


class CommandRejected(RunControlError):
    code = "command_rejected"
    status_code = 422


class ConfigurationVerificationFailed(RunControlError):
    code = "configuration_verification_failed"
    status_code = 422
