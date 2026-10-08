"""Readiness accepts an installation upgraded in place (receipts of earlier releases)."""

from __future__ import annotations

import pytest

from mission_control.bootstrap.common_installation import receipts_belong_to_release


@pytest.mark.parametrize(
    ("receipts", "required", "accepted"),
    [
        ({"1.1.0"}, "1.1.0", True),
        # 1.0.0 applied 0001..0024, the 1.1.0 upgrade applied 0025..0030.
        ({"1.0.0", "1.1.0"}, "1.1.0", True),
        # A release without new migrations is attested without receipts of its own.
        ({"1.0.0"}, "1.0.1", True),
        # Receipts of a newer release than the binding pins are not this release.
        ({"1.0.0", "1.1.0"}, "1.0.0", False),
        ({"2.0.0"}, "1.1.0", False),
        ({"1.1.0", "unlabelled"}, "1.1.0", False),
        # Non-semantic (fixture) versions match exactly.
        ({"parity-1"}, "parity-1", True),
        ({"parity-1", "parity-0"}, "parity-1", False),
    ],
)
def test_receipts_belong_to_the_pinned_release(
    receipts: set[str], required: str, accepted: bool
) -> None:
    assert receipts_belong_to_release(receipts, required) is accepted
