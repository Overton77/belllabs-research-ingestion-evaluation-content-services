"""One disposable-Mongo-database fixture, parameterised by a database-name prefix."""

from collections.abc import AsyncIterator, Callable
from typing import Any
from uuid import uuid4

import pytest
from pymongo import AsyncMongoClient


def disposable_mongo_database(prefix: str) -> Callable[..., Any]:
    """Build a `mongo_database` fixture: a unique database name, dropped after the test."""

    @pytest.fixture
    async def mongo_database(test_mongodb_uri: str) -> AsyncIterator[str]:
        name = f"{prefix}_{uuid4().hex[:12]}"
        yield name
        client: AsyncMongoClient[Any] = AsyncMongoClient(test_mongodb_uri)
        try:
            await client.drop_database(name)
        finally:
            await client.close()

    return mongo_database
