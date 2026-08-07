"""Minimal custom HTTP app for Block C qualification (no native /ok collision)."""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(
    title="BellLabs Block C qualification routes",
    version="0.0.0-qualification",
    docs_url=None,
    openapi_url=None,
)


@app.get("/v2/block-c/qualification")
async def qualification_marker() -> dict[str, str]:
    return {"surface": "block-c-qualification", "side_effects": "none"}
