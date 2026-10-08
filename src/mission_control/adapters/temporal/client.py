"""One Temporal connection path for the API, the worker and the technical API (FT-G7).

temporalio 1.31 moved payload size warnings from the data converter to
`Client.connect(payload_limits=PayloadLimitsConfig(...))` and renamed the fields to
`payloads_warn_size` and `memo_warn_size`. Every Mission Control client is created here so
the limits, the target selection (local development server or Temporal Cloud) and the
transport security are decided once.

Target selection: the local `make temporal-up` server is the development default even when
`TEMPORAL_CLOUD_API_KEY` is present in the environment. Temporal Cloud is used only when the
operator selects `TEMPORAL_TARGET=cloud`; it then connects with `api_key=` and `tls=True` to
`TEMPORAL_ADDRESS` / `TEMPORAL_NAMESPACE`. The API key never leaves this module except as the
`Client.connect` argument, and `TemporalConnection` never renders it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Literal

from temporalio.client import Client, Plugin
from temporalio.service import PayloadLimitsConfig

from mission_control.bootstrap.settings import Settings

TemporalTarget = Literal["local", "cloud"]

# Mission Control keeps payloads small (references and digests, never bodies): warn well
# below the server's 2 MiB blob limit, and at the server's own memo warning size.
PAYLOADS_WARN_SIZE = 512 * 1024
MEMO_WARN_SIZE = 2 * 1024


class TemporalConnectionError(ValueError):
    """The selected Temporal target is incompletely or inconsistently configured."""


def mission_control_payload_limits() -> PayloadLimitsConfig:
    return PayloadLimitsConfig(
        payloads_warn_size=PAYLOADS_WARN_SIZE,
        memo_warn_size=MEMO_WARN_SIZE,
    )


@dataclass(frozen=True)
class TemporalConnection:
    """Where and how a client connects; `api_key` is excluded from `repr`."""

    target: TemporalTarget
    address: str
    namespace: str
    api_key: str | None = field(default=None, repr=False)

    @property
    def tls(self) -> bool:
        return self.target == "cloud"

    def describe(self) -> dict[str, str | bool]:
        """Operator-safe summary for preflight and logs (never the key)."""

        return {
            "target": self.target,
            "address": self.address,
            "namespace": self.namespace,
            "tls": self.tls,
            "api_key": "set" if self.api_key else "absent",
        }


def resolve_temporal_connection(
    settings: Settings,
    *,
    address: str | None = None,
    namespace: str | None = None,
) -> TemporalConnection:
    """Resolve the connection for `settings`, optionally pinned by a deployment's binding.

    `address`/`namespace` come from a deployment's `TemporalDeployment`; a Cloud target
    refuses a deployment that names another frontend than `TEMPORAL_ADDRESS`/
    `TEMPORAL_NAMESPACE`, so a worker never splits across two servers.
    """

    if settings.temporal_target == "cloud":
        if settings.temporal_cloud_api_key is None:
            raise TemporalConnectionError("TEMPORAL_TARGET=cloud requires TEMPORAL_CLOUD_API_KEY")
        key = settings.temporal_cloud_api_key.get_secret_value().strip()
        if not key:
            raise TemporalConnectionError("TEMPORAL_CLOUD_API_KEY is empty")
        if address is not None and address != settings.temporal_address:
            raise TemporalConnectionError(
                "deployment Temporal address differs from TEMPORAL_ADDRESS for the cloud target"
            )
        if namespace is not None and namespace != settings.temporal_namespace:
            raise TemporalConnectionError(
                "deployment Temporal namespace differs from TEMPORAL_NAMESPACE for the cloud target"
            )
        return TemporalConnection(
            target="cloud",
            address=settings.temporal_address,
            namespace=settings.temporal_namespace,
            api_key=key,
        )
    return TemporalConnection(
        target="local",
        address=address or settings.temporal_address,
        namespace=namespace or settings.temporal_namespace,
    )


async def connect_temporal(
    connection: TemporalConnection, *, plugins: Sequence[Plugin] = ()
) -> Client:
    """`Client.connect` with Mission Control's payload limits and the target's security."""

    return await Client.connect(
        connection.address,
        namespace=connection.namespace,
        api_key=connection.api_key,
        tls=connection.tls,
        plugins=list(plugins),
        payload_limits=mission_control_payload_limits(),
    )


async def create_temporal_client(
    settings: Settings, *, plugins: Sequence[Plugin] | None = None
) -> Client:
    """Connect to the configured Temporal target (local by default)."""

    return await connect_temporal(resolve_temporal_connection(settings), plugins=plugins or ())
