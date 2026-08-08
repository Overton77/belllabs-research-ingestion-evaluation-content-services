from __future__ import annotations

from pathlib import Path
from typing import Any

import asyncpg

from app.experiments.langgraph_temporal_stagegraph.repository import ExperimentRepository

from .contracts import ClaimEvaluation, MissionPlan, ResearchUnitResult, sha256_text
from .evaluators import POLICY_VERSION


class SwarmEvidenceRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def save_plan(self, run_id: str, plan: MissionPlan) -> None:
        payload = plan.model_dump_json()
        await self.pool.execute(
            """INSERT INTO dynamic_research_swarm_experiment.mission_plans
                   (run_id, revision, plan_json, plan_sha256)
               VALUES($1, 1, $2::jsonb, $3) ON CONFLICT (run_id, revision) DO NOTHING""",
            run_id,
            payload,
            sha256_text(payload),
        )

    async def save_unit_evidence(
        self,
        run_id: str,
        result: ResearchUnitResult,
        evaluations: tuple[ClaimEvaluation, ...],
    ) -> None:
        async with self.pool.acquire() as connection, connection.transaction():
            for source in result.sources:
                await connection.execute(
                    """INSERT INTO dynamic_research_swarm_experiment.source_snapshots
                           (source_id, run_id, stage_id, url, title, text_content,
                            text_sha256, retrieved_at)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                       ON CONFLICT (run_id, source_id) DO NOTHING""",
                    source.source_id,
                    run_id,
                    result.unit.unit_id,
                    source.url,
                    source.title,
                    source.text,
                    source.text_sha256,
                    source.retrieved_at,
                )
            for claim, evaluation in zip(result.analysis.claims, evaluations, strict=True):
                claim_payload = claim.model_dump_json()
                await connection.execute(
                    """INSERT INTO dynamic_research_swarm_experiment.claims
                           (claim_id, run_id, unit_id, claim_json, disposition, source_id)
                       VALUES($1,$2,$3,$4::jsonb,$5,$6) ON CONFLICT (claim_id) DO NOTHING""",
                    evaluation.claim_id,
                    run_id,
                    result.unit.unit_id,
                    claim_payload,
                    evaluation.disposition,
                    claim.source_id,
                )
                await connection.execute(
                    """INSERT INTO dynamic_research_swarm_experiment.evaluations
                           (evaluation_id, claim_id, policy_version, report_json, report_sha256)
                       VALUES($1,$2,$3,$4::jsonb,$5) ON CONFLICT (evaluation_id) DO NOTHING""",
                    evaluation.evaluation_id,
                    evaluation.claim_id,
                    POLICY_VERSION,
                    evaluation.model_dump_json(),
                    evaluation.report_sha256,
                )

    async def evidence_timeline(self, run_id: str) -> dict[str, Any]:
        plans = await self.pool.fetch(
            """SELECT revision, plan_json, plan_sha256, created_at
               FROM dynamic_research_swarm_experiment.mission_plans
               WHERE run_id=$1 ORDER BY revision""",
            run_id,
        )
        sources = await self.pool.fetch(
            """SELECT source_id, stage_id, url, title, text_sha256, retrieved_at
               FROM dynamic_research_swarm_experiment.source_snapshots
               WHERE run_id=$1 ORDER BY stage_id, source_id""",
            run_id,
        )
        claims = await self.pool.fetch(
            """SELECT c.claim_id, c.unit_id, c.claim_json, c.disposition, c.source_id,
                      e.report_json, e.report_sha256
               FROM dynamic_research_swarm_experiment.claims c
               JOIN dynamic_research_swarm_experiment.evaluations e USING (claim_id)
               WHERE c.run_id=$1 ORDER BY c.unit_id, c.claim_id""",
            run_id,
        )
        return {
            "plans": [dict(row) for row in plans],
            "sources": [dict(row) for row in sources],
            "claims": [dict(row) for row in claims],
        }


async def setup_swarm_database(migration_dsn: str) -> None:
    repository = await ExperimentRepository.connect(migration_dsn)
    try:
        schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        await repository.pool.execute(schema)
    finally:
        await repository.close()
