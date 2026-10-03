from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from mission_control.contracts.canonical import canonical_bytes, canonical_digest


def test_golden_bytes_preserve_arrays_and_sort_explicit_sets() -> None:
    fixture = {
        "schema_version": "mc.fixture.v1",
        "request_id": UUID("00000000-0000-0000-0000-000000000001"),
        "names": frozenset({"z", "a"}),
        "order": [2, 1],
        "text": "e\u0301",
        "decimal": Decimal("12.3400"),
        "at": datetime(2026, 10, 3, tzinfo=UTC),
    }
    assert (
        canonical_bytes(fixture)
        == (
            '{"at":"2026-10-03T00:00:00Z","decimal":"12.34","names":["a","z"],'
            '"order":[2,1],"request_id":"00000000-0000-0000-0000-000000000001",'
            '"schema_version":"mc.fixture.v1","text":"é"}'
        ).encode()
    )
    assert canonical_digest({"x": [1, 2]}) != canonical_digest({"x": [2, 1]})
    assert canonical_digest({"x": {1, 2}}) == canonical_digest({"x": {2, 1}})


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 2**63, -(2**63) - 1])
def test_out_of_contract_numbers_are_rejected(value: object) -> None:
    with pytest.raises(ValueError):
        canonical_bytes(value)


def test_unicode_key_collision_is_rejected_before_digesting() -> None:
    with pytest.raises(ValueError, match="collide"):
        canonical_bytes({"é": 1, "e\u0301": 2})


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone"):
        canonical_bytes(datetime(2026, 10, 3))


def test_decimal_canonicalization_does_not_round_through_ambient_context() -> None:
    assert canonical_bytes(Decimal("123456789012345678901234567890.00100")) == (
        b'"123456789012345678901234567890.001"'
    )
