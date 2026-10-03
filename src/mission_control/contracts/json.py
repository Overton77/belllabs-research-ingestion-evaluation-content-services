"""Strict JSON object parsing shared by HTTP and command-line request boundaries."""

from __future__ import annotations

import json
import math
from typing import Any


def parse_json_object(data: str | bytes) -> dict[str, Any]:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON object key")
            result[key] = value
        return result

    def invalid_constant(value: str) -> None:
        raise ValueError("nonfinite JSON number")

    def finite_float(value: str) -> float:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("nonfinite JSON number")
        return number

    result = json.loads(
        data,
        object_pairs_hook=pairs,
        parse_constant=invalid_constant,
        parse_float=finite_float,
    )
    if not isinstance(result, dict):
        raise ValueError("request must contain one JSON object")
    return result
