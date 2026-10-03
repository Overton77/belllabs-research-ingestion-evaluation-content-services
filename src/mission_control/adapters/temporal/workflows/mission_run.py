"""Fresh Mission Control root registration over the qualified execution protocol."""

from __future__ import annotations

from typing import Any

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
    from mission_control.domain.programs.contracts import BellLabsRunInput


@workflow.defn(name="mc.mission_run.v1")
class MissionRunWorkflow(BellLabsRunWorkflow):
    @workflow.run
    async def run(self, run_input: BellLabsRunInput) -> Any:
        try:
            run_input.validate_mission_binding()
            if workflow.info().workflow_id != run_input.workflow_id:
                raise ValueError("Mission Control root identity differs from its admitted binding")
        except ValueError as error:
            raise ApplicationError(
                str(error), type="InvalidMissionBinding", non_retryable=True
            ) from error
        return await super().run(run_input)
