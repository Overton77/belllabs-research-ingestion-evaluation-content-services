from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

import httpx
from deepagents import create_deep_agent
from temporalio import activity

from app.experiments.langgraph_temporal_stagegraph.contracts import (
    CompletionRecord,
    TemporalStageInput,
    TemporalStageResult,
    digest_text,
)
from app.experiments.langgraph_temporal_stagegraph.temporal_activities import (
    get_worker_repository,
)

from .contracts import (
    AcceptedClaim,
    FinalSynthesis,
    MissionPlan,
    ResearchUnitResult,
    ResearchUnitSpec,
    SourceBundle,
    SourceSnapshot,
    UnitAnalysis,
    sha256_text,
)


def _repair_mojibake(value: str) -> str:
    if not any(marker in value for marker in ("â€", "â€™", "Ã", "Â")):
        return value
    replacements = {
        "â€œ": "“",
        "â€\x9d": "”",
        "â€™": "’",
        "â€˜": "‘",
        "â€“": "–",
        "â€”": "—",
        "âˆ’": "−",
    }
    repaired = value
    for broken, replacement in replacements.items():
        repaired = repaired.replace(broken, replacement)
    if repaired != value:
        return repaired.replace("Â ", " ")
    try:
        return value.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return value


async def _tavily_search(query: str, *, unit_id: str, limit: int) -> SourceBundle:
    api_key = os.environ["TAVILY_API_KEY"]
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.post(
            "https://api.tavily.com/search",
            json={
                "api_key": api_key,
                "query": query[:400],
                "search_depth": "advanced",
                "max_results": limit,
                "chunks_per_source": 3,
                "include_answer": False,
                "include_raw_content": "text",
            },
        )
        response.raise_for_status()
        payload = response.json()
    sources: list[SourceSnapshot] = []
    for index, item in enumerate(payload.get("results", ())[:limit], start=1):
        text = _repair_mojibake(str(item.get("raw_content") or item.get("content") or "").strip())[
            :12_000
        ]
        url = str(item.get("url") or "")
        if not text or not url.startswith(("http://", "https://")):
            continue
        sources.append(
            SourceSnapshot(
                source_id=f"{unit_id}:S{index}",
                title=_repair_mojibake(str(item.get("title") or "Untitled source"))[:500],
                url=url[:2_000],
                retrieved_at=datetime.now(UTC),
                text=text,
                text_sha256=sha256_text(text),
            )
        )
    if not sources:
        raise RuntimeError(f"Tavily returned no usable sources for {query!r}")
    return SourceBundle(query=query, sources=tuple(sources))


async def _structured_agent(
    *, model: str, name: str, system_prompt: str, prompt: str, response_format: type[Any]
) -> Any:
    agent = create_deep_agent(
        model=model,
        tools=[],
        system_prompt=system_prompt,
        response_format=response_format,
        name=name,
    )
    result = await agent.ainvoke(
        {"messages": [{"role": "user", "content": prompt}]},
        config={
            "tags": ["dynamic-research-swarm", name],
            "metadata": {
                "temporal_workflow_id": activity.info().workflow_id,
                "stage_id": name,
            },
        },
    )
    structured = result.get("structured_response")
    if structured is None:
        raise RuntimeError(f"agent {name} returned no structured_response")
    return structured


def _source_context(bundle: SourceBundle, max_chars: int = 20_000) -> str:
    blocks = [
        f"SOURCE {source.source_id}\nTITLE: {source.title}\nURL: {source.url}\nTEXT:\n{source.text}"
        for source in bundle.sources
    ]
    return "\n\n".join(blocks)[:max_chars]


@activity.defn
async def execute_swarm_stage(request: TemporalStageInput) -> TemporalStageResult:
    activity.heartbeat("swarm-stage-started")
    payload = json.loads(request.prompt)
    kind = payload["kind"]
    if kind == "bootstrap":
        output = await _tavily_search(
            payload["objective"], unit_id="bootstrap", limit=payload["max_sources"]
        )
    elif kind == "plan":
        bundle = SourceBundle.model_validate(payload["bootstrap"])
        output = await _structured_agent(
            model=request.model,
            name="mission_planner",
            response_format=MissionPlan,
            system_prompt=(
                "You design bounded research missions. Decompose the objective into two or three "
                "independent, source-searchable units. Never invent executable node types."
            ),
            prompt=(
                f"OBJECTIVE: {payload['objective']}\n\nINITIAL RESEARCH:\n"
                f"{_source_context(bundle)}\n\nReturn a best-case mission plan. "
                "Search queries must be "
                "under 400 characters and units must collectively cover the objective."
            ),
        )
    elif kind == "research":
        unit = ResearchUnitSpec.model_validate(payload["unit"])
        bundle = await _tavily_search(
            unit.search_query, unit_id=unit.unit_id, limit=payload["max_sources"]
        )
        analysis = await _structured_agent(
            model=request.model,
            name=f"research_{unit.unit_id}",
            response_format=UnitAnalysis,
            system_prompt=(
                "You are one member of a source-grounded research team. Emit atomic claims only. "
                "Every claim must name one supplied source_id and copy a short, exact, contiguous "
                "source_quote. Declare every numeric token appearing in the claim. Do not use "
                "outside "
                "knowledge and label inference explicitly."
            ),
            prompt=(
                f"QUESTION: {unit.question}\nRATIONALE: {unit.rationale}\n\n"
                f"SOURCES:\n{_source_context(bundle)}\n\nAnswer the question and return at most "
                "six atomic claims. source_id must exactly match a supplied SOURCE label."
            ),
        )
        output = ResearchUnitResult(unit=unit, sources=bundle.sources, analysis=analysis)
    elif kind == "synthesize":
        claims = tuple(AcceptedClaim.model_validate(item) for item in payload["claims"])
        claim_text = "\n".join(
            f"{claim.claim_id} | {claim.claim_text} | {claim.source_url}" for claim in claims
        )
        output = await _structured_agent(
            model=request.model,
            name="swarm_synthesis",
            response_format=FinalSynthesis,
            system_prompt=(
                "Synthesize only the accepted claims supplied. Cite claim IDs in the answer, "
                "preserve "
                "uncertainty, and never introduce a new number or factual proposition."
            ),
            prompt=(
                f"OBJECTIVE: {payload['objective']}\n\nACCEPTED CLAIMS:\n{claim_text}\n\n"
                "Produce a concise answer with inline claim IDs and list every claim ID used."
            ),
        )
    else:
        raise ValueError(f"unsupported swarm stage kind {kind!r}")
    output_text = output.model_dump_json()
    activity.heartbeat("swarm-stage-completed")
    return TemporalStageResult(
        attempt_id=request.attempt_id,
        stage_id=request.stage_id,
        output_text=output_text,
        output_digest=digest_text(output_text),
    )


@activity.defn
async def record_swarm_completion(completion: CompletionRecord) -> None:
    repository = await get_worker_repository()
    await repository.record_completion_and_wake(completion)
