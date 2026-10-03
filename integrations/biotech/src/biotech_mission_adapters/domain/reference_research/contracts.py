"""Strict, provider-neutral contracts for the immutable Q/D reference families."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, HttpUrl, model_validator

QUALIA_FAMILY_ID = "reference.qualia-life.current-supplement-products"
DAVE_FAMILY_ID = "reference.dave-asprey.current-company-ownership"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SourceAuthority(StrEnum):
    OFFICIAL_COMMERCE = "official_commerce"
    FIRST_PARTY_CORPORATE = "first_party_corporate"
    REGULATORY_OR_LEGAL = "regulatory_or_legal"
    HIGH_QUALITY_SECONDARY = "high_quality_secondary"
    THIRD_PARTY_LISTING = "third_party_listing"


class SourceFact(Contract):
    source_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    url: HttpUrl
    authority: SourceAuthority
    observed_at: AwareDatetime
    excerpt: str = Field(min_length=1, max_length=500)


class QualiaInclusionPolicy(Contract):
    supplements: Literal[True] = True
    bundles: Literal[False] = False
    subscriptions: Literal[True] = True
    unavailable_or_out_of_stock: Literal[False] = False
    third_party_listings: Literal[False] = False
    historical_products: Literal[False] = False


class QualiaCandidateFact(Contract):
    record_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    name: str = Field(min_length=1)
    canonical_product_url: HttpUrl | None = None
    item_kind: Literal["supplement", "bundle", "digital_good", "unknown"]
    availability: Literal["available", "out_of_stock", "unavailable", "unknown"]
    seller: str | None = None
    source_refs: tuple[str, ...] = Field(min_length=1)
    historical: bool = False


class QualiaFixtureInput(Contract):
    family_id: Literal["reference.qualia-life.current-supplement-products"] = (
        "reference.qualia-life.current-supplement-products"
    )
    as_of: AwareDatetime
    source_authority_policy: tuple[SourceAuthority, ...] = (
        SourceAuthority.OFFICIAL_COMMERCE,
        SourceAuthority.FIRST_PARTY_CORPORATE,
    )
    inclusion_policy: QualiaInclusionPolicy = Field(default_factory=QualiaInclusionPolicy)
    sources: tuple[SourceFact, ...] = Field(min_length=1)
    candidates: tuple[QualiaCandidateFact, ...] = Field(min_length=3)

    @model_validator(mode="after")
    def refs_resolve(self) -> QualiaFixtureInput:
        known = {source.source_id for source in self.sources}
        if any(set(candidate.source_refs) - known for candidate in self.candidates):
            raise ValueError("Qualia candidate references an unknown source")
        return self


class QualiaProductClaim(Contract):
    record_id: str
    name: str
    canonical_product_url: HttpUrl | None
    classification: Literal["included", "excluded", "unknown_requires_review"]
    reason_code: str = Field(min_length=1)
    availability: Literal["available", "out_of_stock", "unavailable", "unknown"]
    observed_at: AwareDatetime
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    confidence: float = Field(ge=0, le=1)


class QualiaCatalogResult(Contract):
    family_id: Literal["reference.qualia-life.current-supplement-products"] = (
        "reference.qualia-life.current-supplement-products"
    )
    as_of: AwareDatetime
    products: tuple[QualiaProductClaim, ...]
    review_required_record_ids: tuple[str, ...] = ()


class CompanyRelationshipClass(StrEnum):
    CURRENTLY_OWNS_OR_CONTROLS = "currently_owns_or_controls"
    FOUNDER_CURRENT_OWNERSHIP_UNVERIFIED = "founder_but_current_ownership_unverified"
    INVESTOR_SHAREHOLDER_EXTENT_UNKNOWN = "investor_or_shareholder_extent_unknown"
    ADVISOR_OR_BOARD_ROLE = "advisor_or_board_role"
    ENDORSEMENT_OR_AFFILIATION = "brand_endorsement_or_affiliation"
    FORMER_OR_HISTORICAL_ASSOCIATION = "former_or_historical_association"
    CONFLICTING_OR_INSUFFICIENT_EVIDENCE = "conflicting_or_insufficient_evidence"


class DaveCompanyFact(Contract):
    record_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    company: str = Field(min_length=1)
    asserted_relationship: CompanyRelationshipClass
    explicit_current_control_evidence: bool = False
    explicit_former_evidence: bool = False
    source_refs: tuple[str, ...] = Field(min_length=1)
    contrary_source_refs: tuple[str, ...] = ()
    jurisdiction_or_context: str | None = None


class DaveFixtureInput(Contract):
    family_id: Literal["reference.dave-asprey.current-company-ownership"] = (
        "reference.dave-asprey.current-company-ownership"
    )
    as_of: AwareDatetime
    evidence_rule: Literal[
        "current ownership requires affirmative accepted evidence; "
        "founding, association, or absence never proves ownership or non-ownership"
    ] = (
        "current ownership requires affirmative accepted evidence; "
        "founding, association, or absence never proves ownership or non-ownership"
    )
    sources: tuple[SourceFact, ...] = Field(min_length=1)
    companies: tuple[DaveCompanyFact, ...] = Field(min_length=4)

    @model_validator(mode="after")
    def refs_resolve(self) -> DaveFixtureInput:
        known = {source.source_id for source in self.sources}
        for company in self.companies:
            if (set(company.source_refs) | set(company.contrary_source_refs)) - known:
                raise ValueError("Dave company fact references an unknown source")
        return self


class DaveCompanyClaim(Contract):
    record_id: str
    company: str
    relationship_class: CompanyRelationshipClass
    current_status: Literal["affirmed", "not_current", "unknown_requires_review"]
    observed_at: AwareDatetime
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    contrary_evidence_refs: tuple[str, ...] = ()
    confidence: float = Field(ge=0, le=1)
    unresolved_limitations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def ownership_requires_affirmative_class(self) -> DaveCompanyClaim:
        if self.current_status == "affirmed" and self.relationship_class != (
            CompanyRelationshipClass.CURRENTLY_OWNS_OR_CONTROLS
        ):
            raise ValueError("only affirmative current-control evidence can affirm ownership")
        return self


class DaveOwnershipResult(Contract):
    family_id: Literal["reference.dave-asprey.current-company-ownership"] = (
        "reference.dave-asprey.current-company-ownership"
    )
    as_of: AwareDatetime
    companies: tuple[DaveCompanyClaim, ...]
    review_required_record_ids: tuple[str, ...] = ()


ReferenceFixture = Annotated[
    QualiaFixtureInput | DaveFixtureInput,
    Field(discriminator="family_id"),
]
