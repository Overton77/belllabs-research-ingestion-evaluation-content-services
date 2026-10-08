"""The chain-link Context Packet (SPEC-04 "State transfer", SPEC-02 chain link source).

Pure: :func:`chain_pack_request` turns the released links of one consumer, the supplier run
facts and the supplied artifacts' custody metadata into a ``PackRequest`` for the B1 packer
(``purpose: chain_link``). Per supplied output it carries the artifact (tier per binding, default
``auto``) as a ``chain_supply`` item bound ``<from_mission_key>.<output_name>`` so a
``materialize`` item lands at ``/inputs/<from>.<output>/<file>``; per supplier the acceptance
disposition (inline), the final checkpoint and journal-digest references with a
``missionctl run transcript`` retrieval instruction; and the chain identity (inline). Nothing
from a supplier's workspace is restored: files travel only as registered artifacts.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from uuid import UUID, uuid5

from mission_control.application.chains.reducer import MemberRun
from mission_control.application.context.pack_service import DEFAULT_MODEL_BUDGET_PROFILE
from mission_control.domain.authoring.manifest import ExpansionTier
from mission_control.domain.composition.chain import (
    ChainBinding,
    ChainLink,
    ChainLinkKind,
    MissionChain,
)
from mission_control.domain.context.packet import (
    ContextBinding,
    ContextPurpose,
    ContextSourceKind,
    ContextTrust,
    ExpandMode,
    ItemProvenance,
    LaneFileSupport,
    ModelBudgetProfile,
    PackCandidate,
    PacketScope,
    PacketTarget,
    PackRequest,
    RetrievalInstruction,
    RetrievalKind,
)

_PACKET_NAMESPACE = UUID("8d6f2a14-5c3b-5e7a-b1f0-3a9c7e2d4b16")
CHAIN_PACKET_NODE_KEY = "chain"


@dataclass(frozen=True, slots=True)
class SuppliedArtifact:
    """Custody metadata of one accepted supplier output (no bytes are read)."""

    source_ref: str
    content_digest: str
    size_bytes: int
    media_type: str
    durable_ref: str
    """``<object_ref>#<sha256>:<size>``: what the workspace materializer fetches."""
    logical_path: str | None = None

    @property
    def file_name(self) -> str | None:
        if self.logical_path is None:
            return None
        return PurePosixPath(self.logical_path).name or None

    def names(self) -> set[str]:
        """Names an output may be matched by: the file name and its stem, the path parts."""

        found: set[str] = set()
        for text in (self.logical_path, self.source_ref.rsplit("/", 1)[-1]):
            if not text:
                continue
            path = PurePosixPath(text)
            found.update({path.name, path.stem})
            found.update(part for part in path.parts if part not in {"/", ""})
        return found


def match_supplied(
    binding: ChainBinding, artifacts: Sequence[SuppliedArtifact], *, single_binding: bool
) -> tuple[SuppliedArtifact, ...]:
    """The accepted outputs that realize ``binding``: matched by name (file name, stem or a
    path segment equal to the output name); a link with one bound output takes every accepted
    output when none carries the name."""

    named = tuple(item for item in artifacts if binding.output_name in item.names())
    if named:
        return named
    return tuple(artifacts) if single_binding else ()


def _digest(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def _expand(tier: ExpansionTier) -> ExpandMode:
    # Bytes are never read inside the ledger transaction, so an inline request is decided by
    # the packer's budget rule from the custody metadata (it falls back to materialize/reference).
    if tier is ExpansionTier.MATERIALIZE:
        return ExpandMode.MATERIALIZE
    if tier is ExpansionTier.REFERENCE:
        return ExpandMode.REFERENCE
    return ExpandMode.AUTO


def _text(
    kind: ContextSourceKind,
    ref: str,
    text: str,
    *,
    provenance: ItemProvenance,
    trust: ContextTrust = ContextTrust.AUTHORITATIVE,
    mandatory: bool = True,
) -> PackCandidate:
    return PackCandidate(
        source_kind=kind,
        source_ref=ref,
        content_digest=_digest(text),
        bytes=len(text.encode("utf-8")),
        media_type="text/markdown",
        trust=trust,
        mandatory=mandatory,
        expand=ExpandMode.INLINE,
        text=text,
        provenance=provenance,
    )


def _acceptance_ref(link: ChainLink, run: MemberRun) -> str:
    if link.on.kind == "goal_accepted":
        return f"run:{run.run_key}:obligation:{link.on.goal_key}"
    return f"run:{run.run_key}:terminal"


def _disposition(chain: MissionChain, link: ChainLink, run: MemberRun) -> str:
    accepted = ", ".join(sorted(run.accepted_obligations)) or "none"
    outcome = run.terminal_outcome.value if run.terminal_outcome else run.phase.value
    condition = link.on.kind + (f"{{{link.on.goal_key}}}" if link.on.goal_key else "")
    return (
        f"Supplier mission `{link.from_mission_key}` (run `{run.run_key}`) satisfied "
        f"`{condition}` for link `{link.link_key}` of chain `{chain.chain_key}`. "
        f"Accepted goals/obligations: {accepted}. Run outcome: {outcome}. "
        f"Evidence frontier: {run.evidence_frontier_digest or 'none'}."
    )


def chain_pack_request(
    *,
    chain: MissionChain,
    consumer_mission_id: UUID,
    consumer_revision_id: UUID,
    consumer_run_key: str,
    links: Sequence[ChainLink],
    supplier_runs: Mapping[UUID, MemberRun],
    supplied: Mapping[UUID, Sequence[SuppliedArtifact]],
    sealed_at: datetime,
    request_scope: str,
    profile: ModelBudgetProfile = DEFAULT_MODEL_BUDGET_PROFILE,
) -> PackRequest:
    """The packet request of the consumer released by ``links`` (all its incoming links)."""

    consumer_key = next(
        member.mission_key for member in chain.members if member.mission_id == consumer_mission_id
    )
    candidates: list[PackCandidate] = []
    bindings: list[ContextBinding] = []
    producer_refs: list[str] = []
    seen_suppliers: set[UUID] = set()
    for link in sorted(links, key=lambda item: item.link_key):
        supplier_id = link.from_mission_id
        assert supplier_id is not None
        run = supplier_runs[supplier_id]
        accepted_ref = _acceptance_ref(link, run)
        provisional = not (
            run.mission_accepted
            or (link.on.kind == "goal_accepted" and run.goal_accepted(link.on.goal_key or ""))
        )
        provenance = ItemProvenance(
            producer_activation_id=run.run_key,
            accepted_decision_ref=None if provisional else accepted_ref,
            chain_link_id=str(link.link_id),
            provisional=provisional,
        )
        if link.kind is ChainLinkKind.SUPPLIES:
            single = len(link.bindings) == 1
            for binding in link.bindings:
                name = f"{link.from_mission_key}.{binding.output_name}"
                bindings.append(
                    ContextBinding(
                        binding_name=name,
                        expand=_expand(binding.expand),
                        mandatory=not provisional,
                    )
                )
                for artifact in match_supplied(
                    binding, supplied.get(supplier_id, ()), single_binding=single
                ):
                    candidates.append(
                        PackCandidate(
                            source_kind=ContextSourceKind.CHAIN_SUPPLY,
                            source_ref=artifact.source_ref,
                            binding_name=name,
                            content_digest=artifact.content_digest,
                            bytes=artifact.size_bytes,
                            media_type=artifact.media_type,
                            schema_ref=binding.schema_ref,
                            # Model-produced artifacts are data, never instructions.
                            trust=ContextTrust.UNTRUSTED_CONTENT,
                            mandatory=not provisional,
                            summary=(
                                f"{name} supplied by run {run.run_key} "
                                f"({artifact.media_type}, {artifact.size_bytes} bytes)"
                            ),
                            file_name=artifact.file_name,
                            durable_ref=artifact.durable_ref,
                            provenance=provenance,
                        )
                    )
        if supplier_id in seen_suppliers:
            continue
        seen_suppliers.add(supplier_id)
        producer_refs.append(run.run_key)
        candidates.append(
            _text(
                ContextSourceKind.CHAIN_SUPPLY,
                f"chain://{chain.chain_id}/suppliers/{link.from_mission_key}/disposition",
                _disposition(chain, link, run),
                provenance=provenance.model_copy(
                    update={"accepted_decision_ref": accepted_ref, "provisional": False}
                ),
            )
        )
        frontier = run.evidence_frontier_digest or _digest(run.run_key)
        transcript = f"missionctl run transcript {run.run_key}"
        candidates.append(
            PackCandidate(
                source_kind=ContextSourceKind.CONTINUATION_CHECKPOINT,
                source_ref=f"checkpoint://{run.run_key}/final",
                content_digest=frontier,
                bytes=0,
                media_type="application/json",
                trust=ContextTrust.AUTHORITATIVE,
                mandatory=True,
                expand=ExpandMode.REFERENCE,
                summary=(
                    f"final continuation checkpoint of supplier `{link.from_mission_key}` "
                    f"(run {run.run_key}); read the supplier transcript from the start"
                ),
                retrieval=RetrievalInstruction(
                    kind=RetrievalKind.MISSIONCTL, command=f"{transcript} --since 0"
                ),
            )
        )
        candidates.append(
            PackCandidate(
                source_kind=ContextSourceKind.JOURNAL_DIGEST,
                source_ref=f"journal://{run.run_key}/final",
                content_digest=frontier,
                bytes=0,
                media_type="application/json",
                trust=ContextTrust.AUTHORITATIVE,
                mandatory=True,
                expand=ExpandMode.REFERENCE,
                summary=(
                    f"journal digest of supplier `{link.from_mission_key}` (run {run.run_key})"
                ),
                retrieval=RetrievalInstruction(
                    kind=RetrievalKind.MISSIONCTL, command=f"{transcript} --format markdown"
                ),
            )
        )
    identity = json.dumps(
        {
            "chain_id": str(chain.chain_id),
            "chain_key": chain.chain_key,
            "consumer": consumer_key,
            "links": [
                {
                    "link_id": str(link.link_id),
                    "link_key": link.link_key,
                    "kind": link.kind.value,
                    "from": link.from_mission_key,
                    "on": link.on.model_dump(mode="json", exclude_none=True),
                }
                for link in sorted(links, key=lambda item: item.link_key)
            ],
        },
        sort_keys=True,
        indent=2,
    )
    candidates.append(
        _text(
            ContextSourceKind.CHAIN_SUPPLY,
            f"chain://{chain.chain_id}/links/{consumer_key}",
            "Mission Chain identity (you are the consumer; the suppliers are read-only):\n"
            + identity,
            provenance=ItemProvenance(accepted_decision_ref=f"chain:{chain.chain_id}"),
        )
    )
    parts = request_scope.split("/")
    scope = (
        PacketScope(installation_id=parts[1], application_id=parts[2], tenant_id=parts[3])
        if len(parts) == 4 and parts[0] == "mc"
        else PacketScope(installation_id=request_scope, application_id="-", tenant_id="-")
    )
    seed = f"{chain.chain_id}:{consumer_mission_id}:{consumer_run_key}"
    return PackRequest(
        packet_id=str(uuid5(_PACKET_NAMESPACE, f"packet:{seed}")),
        sealed_at=sealed_at,
        context_selection_ref=f"context_selection:{uuid5(_PACKET_NAMESPACE, f'selection:{seed}')}",
        scope=scope,
        target=PacketTarget(
            mission_id=str(consumer_mission_id),
            run_id=consumer_run_key,
            revision_id=str(consumer_revision_id),
            node_key=CHAIN_PACKET_NODE_KEY,
            activation_id=f"chain:{chain.chain_id}",
            attempt_no=1,
            generation=1,
            purpose=ContextPurpose.CHAIN_LINK,
        ),
        producer_refs=tuple(producer_refs),
        profile=profile,
        bindings=tuple(bindings),
        candidates=tuple(candidates),
        lane=LaneFileSupport(writable_workspace=True, text_only_files=False),
    )
