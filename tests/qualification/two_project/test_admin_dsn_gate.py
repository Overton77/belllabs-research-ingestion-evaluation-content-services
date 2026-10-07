"""The mandatory common-schema proofs FAIL (never skip) without a disposable server."""

from __future__ import annotations

import pytest

from tests.qualification.two_project import conftest as qualification
from tests.qualification.two_project.disposable import ADMIN_DSN_ENV, require_admin_dsn


def test_missing_admin_dsn_fails_instead_of_skipping(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ADMIN_DSN_ENV, raising=False)
    with pytest.raises(pytest.fail.Exception, match=ADMIN_DSN_ENV):
        require_admin_dsn()


def test_non_loopback_admin_dsn_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ADMIN_DSN_ENV, "postgresql://postgres:x@db.example.invalid:5432/postgres")
    with pytest.raises(pytest.fail.Exception, match="loopback"):
        require_admin_dsn()


def test_incomplete_evidence_fails_with_recorded_phase() -> None:
    evidence = {"errors": [{"phase": "admin_dsn", "error": "unset"}], "apps": {}}
    with pytest.raises(pytest.fail.Exception, match=r"\[admin_dsn\] unset"):
        qualification.require(evidence, "apps/biotech/install")
