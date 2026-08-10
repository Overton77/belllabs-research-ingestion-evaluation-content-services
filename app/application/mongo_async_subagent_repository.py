from __future__ import annotations

from pymongo.errors import DuplicateKeyError

from app.application.async_subagents import AsyncSubagentError
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentExecution,
    ParentAsyncSubagentLink,
)
from app.models.operation_execution import (
    AsyncSubagentContractDocument,
    AsyncSubagentExecutionDocument,
    ParentAsyncSubagentLinkDocument,
)


class MongoAsyncSubagentDetailRepository:
    async def create_before_submit(
        self,
        request_scope: str,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        link: ParentAsyncSubagentLink,
    ) -> AsyncSubagentExecution:
        try:
            await AsyncSubagentContractDocument(
                request_scope=request_scope,
                contract_id=contract.contract_id,
                contract_digest=contract.contract_digest,
                payload=contract.model_dump(mode="json"),
                created_at=execution.created_at,
            ).insert()
        except DuplicateKeyError:
            prior_contract = await AsyncSubagentContractDocument.find_one(
                AsyncSubagentContractDocument.request_scope == request_scope,
                AsyncSubagentContractDocument.contract_id == contract.contract_id,
            )
            if prior_contract is None or prior_contract.contract_digest != contract.contract_digest:
                raise AsyncSubagentError("async contract identity collision") from None
        try:
            await AsyncSubagentExecutionDocument(
                request_scope=request_scope,
                child_execution_id=execution.child_execution_id,
                contract_id=execution.contract_id,
                parent_run_id=execution.parent_run_id,
                parent_operation_id=execution.parent_operation_id,
                execution_generation=execution.execution_generation,
                payload=execution.model_dump(mode="json"),
                updated_at=execution.updated_at,
            ).insert()
            await ParentAsyncSubagentLinkDocument(
                request_scope=request_scope,
                link_id=link.link_id,
                child_execution_id=link.child_execution_id,
                parent_run_id=link.parent_run_id,
                parent_operation_id=link.parent_operation_id,
                payload=link.model_dump(mode="json"),
                updated_at=link.updated_at,
            ).insert()
            return execution
        except DuplicateKeyError:
            prior = await self.get_execution(request_scope, execution.child_execution_id)
            if prior.contract_digest != execution.contract_digest:
                raise AsyncSubagentError("async execution identity collision") from None
            return prior

    async def get_execution(self, request_scope: str, child_execution_id: str) -> AsyncSubagentExecution:
        document = await AsyncSubagentExecutionDocument.find_one(
            AsyncSubagentExecutionDocument.request_scope == request_scope,
            AsyncSubagentExecutionDocument.child_execution_id == child_execution_id,
        )
        if document is None:
            raise AsyncSubagentError("async child execution not found")
        return AsyncSubagentExecution.model_validate(document.payload)

    async def get_link(self, request_scope: str, child_execution_id: str) -> ParentAsyncSubagentLink:
        document = await ParentAsyncSubagentLinkDocument.find_one(
            ParentAsyncSubagentLinkDocument.request_scope == request_scope,
            ParentAsyncSubagentLinkDocument.child_execution_id == child_execution_id,
        )
        if document is None:
            raise AsyncSubagentError("async child link not found")
        return ParentAsyncSubagentLink.model_validate(document.payload)

    async def save_execution(self, request_scope: str, execution: AsyncSubagentExecution) -> None:
        document = await AsyncSubagentExecutionDocument.find_one(
            AsyncSubagentExecutionDocument.request_scope == request_scope,
            AsyncSubagentExecutionDocument.child_execution_id == execution.child_execution_id,
        )
        if document is None:
            raise AsyncSubagentError("async child execution not found")
        document.payload = execution.model_dump(mode="json")
        document.updated_at = execution.updated_at
        await document.save()

    async def save_link(self, request_scope: str, link: ParentAsyncSubagentLink) -> None:
        document = await ParentAsyncSubagentLinkDocument.find_one(
            ParentAsyncSubagentLinkDocument.request_scope == request_scope,
            ParentAsyncSubagentLinkDocument.child_execution_id == link.child_execution_id,
        )
        if document is None:
            raise AsyncSubagentError("async child link not found")
        document.payload = link.model_dump(mode="json")
        document.updated_at = link.updated_at
        await document.save()
