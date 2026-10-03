"""The boundary command delivery relay (RRM-009; RRM-007 review F6).

An accepted `pause`, `resume` or `satisfy_wait` is delivered inline by the API facade when
its Temporal transport is up. When inline delivery fails, the command stays `accepted` in the
ledger (REQ-CP-EXEC-006). The relay re-drives those runs, scope by scope and in acceptance
order, through the same `BoundaryInterventionService.redeliver`, so every accepted command is
eventually delivered in order and applied once (the target de-duplicates and the receipt
ledger never transitions twice). It reads only through run control; forced RLS confines each
read to the scope being relayed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass

from app.application.run_control.boundary_interventions import BoundaryInterventionService
from app.application.run_control.service import RunControlService

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RelayPass:
    """One pass over every relayed scope: the runs re-driven and the receipts recorded."""

    runs: tuple[tuple[str, str], ...]
    delivered: int


class BoundaryCommandRelay:
    def __init__(
        self,
        *,
        run_control: RunControlService,
        interventions: BoundaryInterventionService,
        request_scopes: Sequence[str],
        interval_seconds: float = 5.0,
        batch_size: int = 100,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("relay interval must be positive")
        self._run_control = run_control
        self._interventions = interventions
        self._scopes = tuple(dict.fromkeys(scope for scope in request_scopes if scope))
        self._interval = interval_seconds
        self._batch_size = batch_size

    @property
    def request_scopes(self) -> tuple[str, ...]:
        return self._scopes

    async def run_once(self) -> RelayPass:
        runs: list[tuple[str, str]] = []
        delivered = 0
        for scope in self._scopes:
            try:
                pending = await self._run_control.runs_with_pending_boundary_commands(
                    scope, limit=self._batch_size
                )
            except Exception:
                logger.exception(
                    "boundary relay could not list pending runs", extra={"scope": scope}
                )
                continue
            for run_id in pending:
                try:
                    statuses = await self._interventions.redeliver(scope, run_id)
                except Exception:
                    logger.exception(
                        "boundary relay redelivery failed", extra={"scope": scope, "run_id": run_id}
                    )
                    continue
                runs.append((scope, run_id))
                delivered += len(statuses)
        return RelayPass(runs=tuple(runs), delivered=delivered)

    async def run_forever(self) -> None:
        while True:
            await self.run_once()
            await asyncio.sleep(self._interval)


__all__ = ["BoundaryCommandRelay", "RelayPass"]
