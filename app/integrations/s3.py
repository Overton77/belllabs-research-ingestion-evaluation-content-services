from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import aioboto3

from app.config import Settings


def create_aws_session(settings: Settings) -> aioboto3.Session:
    """Use the normal AWS credential chain, including the user's authenticated CLI profile."""
    kwargs: dict[str, Any] = {"region_name": settings.aws_region}
    if settings.aws_profile:
        kwargs["profile_name"] = settings.aws_profile
    return aioboto3.Session(**kwargs)


@asynccontextmanager
async def s3_client(settings: Settings) -> AsyncIterator[Any]:
    """Yield an async S3 client and always close its underlying HTTP session."""
    session = create_aws_session(settings)
    async with session.client("s3") as client:
        yield client
