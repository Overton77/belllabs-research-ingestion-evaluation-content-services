from __future__ import annotations

import asyncio
import json
from typing import Any

from neo4j import AsyncManagedTransaction
from openai import AsyncOpenAI

from app.config import get_settings
from app.integrations.neo4j import create_neo4j

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536
CAPTURED_AT = "2026-03-30T00:00:00Z"
RESEARCH_RUN_ID = "trudiagnostic-20260330-203619-research-mission"

ORGANIZATION = {
    "id": "64720458-3328-5439-b6de-1624bd5b60ae",
    "name": "TruDiagnostic",
    "description": (
        "Private biotechnology and health-services company focused on epigenetic testing, "
        "DNA-methylation diagnostics, longevity science, and research collaborations."
    ),
    "searchText": (
        "TruDiagnostic | The Epigenetics Company | private biotechnology company in Lexington, "
        "Kentucky offering TruAge and TruHealth at-home finger-prick DNA methylation tests, "
        "biological age clocks, organ-system age scores, and epigenetic biomarker proxies."
    ),
    "legalName": "TruDiagnostic",
    "displayName": "TruDiagnostic",
    "organizationType": "PRIVATE_COMPANY",
    "active": True,
    "foundedYear": 2019,
    "employeeCountBand": "SMALL_MID_51_200",
    "employeeCountRaw": "51-200 employees",
    "revenueModel": (
        "Direct-to-consumer testing kits, clinical-trial partnerships, research collaborations, "
        "and algorithm licensing"
    ),
    "valueChainStages": ["diagnostics", "research", "health data"],
    "sector": "Biotechnology Research / Health Services",
    "websiteUrl": "https://www.trudiagnostic.com",
    "headquartersSummary": "881 Corporate Dr, Lexington, Kentucky 40503, United States",
    "operatingSummary": (
        "CLIA-certified laboratory using methylation arrays and dried blood spot samples for "
        "epigenetic age and biomarker-proxy testing."
    ),
    "competitivePositionSummary": (
        "Offers second- and third-generation aging algorithms and broad methylation-derived "
        "biomarker outputs."
    ),
    "isPublicCompany": False,
    "isProviderOrganization": True,
    "isResearchOrganization": True,
    "isManufacturer": False,
}

PRODUCTS = [
    {
        "id": "6d669d2c-e6d0-5738-aee2-8fc0271b5c83",
        "name": "TruAge",
        "description": (
            "At-home epigenetic aging assessment derived from DNA methylation analysis of "
            "850,000+ CpG sites using a finger-prick dried blood spot."
        ),
        "searchText": (
            "TruAge | TruDiagnostic epigenetic aging test | biological age with OMICmAge, "
            "11 SYMPHONYAge organ-system ages, DunedinPACE pace of aging, telomere-length "
            "estimate, inflammation proxies, mortality and disease-risk outputs."
        ),
        "productType": "Epigenetic Aging Test",
        "applicationContext": "Biological aging and longevity assessment",
        "categories": ["epigenetic testing", "biological age", "longevity"],
        "launchYear": 2021,
        "targetCustomer": "Consumers and clinicians",
        "deliveryModel": "At-home finger-prick blood spot collection",
        "productClass": "Laboratory testing product",
        "modalitySummary": "DNA methylation analysis using an Illumina EPIC array",
    },
    {
        "id": "2bd58d3f-c751-57d3-8227-9bf8fbc51765",
        "name": "TruHealth",
        "description": (
            "At-home nutritional and systems-health assessment reporting 105+ named biomarkers "
            "as DNA-methylation-derived proxies from a finger-prick dried blood spot."
        ),
        "searchText": (
            "TruHealth | TruDiagnostic nutritional and systems health test | epigenetic "
            "biomarker proxies for vitamins, lipids, metabolism, immune function, inflammation, "
            "toxins, mitochondrial function, NAD+ metabolism, ketones, and supplements."
        ),
        "productType": "Nutritional / Systems Health Test",
        "applicationContext": "Nutritional, metabolic, immune, and systems-health assessment",
        "categories": ["epigenetic testing", "nutrition", "systems health"],
        "launchYear": 2024,
        "targetCustomer": "Consumers and clinicians",
        "deliveryModel": "At-home finger-prick blood spot collection",
        "productClass": "Laboratory testing product",
        "modalitySummary": "DNA methylation proxy analysis using an Illumina EPIC array",
    },
    {
        "id": "bf427af6-b9d1-5307-9cf3-ac09715e7d8e",
        "name": "TruAge + TruHealth",
        "description": (
            "Combined at-home panel bundling TruAge biological-aging outputs with TruHealth "
            "nutritional and systems-health biomarker proxies."
        ),
        "searchText": (
            "TruAge + TruHealth | combined TruDiagnostic epigenetic test | biological age, "
            "pace of aging, organ-system ages, and nutritional, metabolic, immune, "
            "inflammation, toxin, and systems-health biomarker proxies."
        ),
        "productType": "Combined Epigenetic Testing Panel",
        "applicationContext": "Combined aging and systems-health assessment",
        "categories": ["epigenetic testing", "biological age", "systems health"],
        "launchYear": 2024,
        "targetCustomer": "Consumers and clinicians",
        "deliveryModel": "At-home finger-prick blood spot collection",
        "productClass": "Combined laboratory testing product",
        "modalitySummary": "DNA methylation analysis using an Illumina EPIC array",
    },
]

LAB_TESTS = [
    {
        "id": "36fda8bc-e8fb-5e56-a6b3-d856ea0ae061",
        "name": "TruAge Epigenetic Biological Age Test",
        "description": (
            "DNA methylation test producing biological-age, organ-system-age, pace-of-aging, "
            "inflammation, telomere, and risk outputs."
        ),
        "searchText": (
            "TruAge epigenetic biological age lab test measuring OMICmAge, SYMPHONYAge organ "
            "ages, DunedinPACE, telomere-length estimate, CRP and IL-6 proxies."
        ),
        "testType": "DNA methylation biological age assessment",
        "products": ["TruAge", "TruAge + TruHealth"],
    },
    {
        "id": "d8966f18-e3d7-5a94-992b-7afa8a24361f",
        "name": "TruHealth Epigenetic Biomarker Proxy Test",
        "description": (
            "DNA methylation proxy test reporting nutritional, metabolic, cardiovascular, "
            "immune, inflammatory, toxin-exposure, and other systems-health biomarkers."
        ),
        "searchText": (
            "TruHealth epigenetic biomarker proxy lab test measuring Vitamin D, ApoB, HbA1c, "
            "glucose, CRP, IL-6, IGF-1, PFAS exposure, beta hydroxybutyrate and other proxies."
        ),
        "testType": "DNA methylation biomarker proxy assessment",
        "products": ["TruHealth", "TruAge + TruHealth"],
    },
]

BIOMARKERS = [
    {
        "id": "4660df29-be0b-5b0b-b853-0045bbb42569",
        "name": "OMICmAge",
        "description": "Multi-omic DNA-methylation-derived biological age score.",
        "biomarkerType": "Biological age score",
        "clinicalSignificance": "Overall biological aging status",
        "tests": ["TruAge Epigenetic Biological Age Test"],
    },
    {
        "id": "fab8bec5-fdc3-5464-ac64-94f8c4f6903b",
        "name": "SymphonyAge organ ages",
        "description": "Eleven system-specific epigenetic age scores.",
        "biomarkerType": "Organ-system biological age",
        "clinicalSignificance": "Aging status across eleven physiological systems",
        "tests": ["TruAge Epigenetic Biological Age Test"],
    },
    {
        "id": "72083449-35f7-5f1b-bdbb-2b6d5814de18",
        "name": "DunedinPACE",
        "description": "DNA-methylation-derived pace-of-aging score.",
        "biomarkerType": "Pace of aging",
        "clinicalSignificance": "Estimated biological aging rate per calendar year",
        "tests": ["TruAge Epigenetic Biological Age Test"],
    },
    {
        "id": "052f4d78-f4af-5a51-b0f0-b7946fe22fad",
        "name": "Telomere length estimate",
        "description": (
            "Epigenetic proxy estimate of telomere length, not a direct qPCR measurement."
        ),
        "biomarkerType": "Epigenetic proxy",
        "clinicalSignificance": "Estimated telomere length relative to age-matched populations",
        "tests": ["TruAge Epigenetic Biological Age Test"],
    },
    {
        "id": "5ecbd9ef-9049-5b46-8f61-d68b3f29e085",
        "name": "CRP proxy",
        "description": "DNA-methylation surrogate for C-reactive protein.",
        "biomarkerType": "Inflammation biomarker proxy",
        "moleculeClass": "Protein proxy",
        "clinicalSignificance": "Systemic inflammation",
        "tests": [
            "TruAge Epigenetic Biological Age Test",
            "TruHealth Epigenetic Biomarker Proxy Test",
        ],
    },
    {
        "id": "e24887f2-5c8d-59f7-9ed2-e8cdf015147a",
        "name": "IL-6 proxy",
        "description": "DNA-methylation surrogate for interleukin-6.",
        "biomarkerType": "Inflammation biomarker proxy",
        "moleculeClass": "Cytokine proxy",
        "clinicalSignificance": "Pro-inflammatory signaling",
        "tests": [
            "TruAge Epigenetic Biological Age Test",
            "TruHealth Epigenetic Biomarker Proxy Test",
        ],
    },
    {
        "id": "63077ef0-e295-5cf8-9465-bc2567e2e30f",
        "name": "Vitamin D epigenetic proxy",
        "description": "DNA-methylation-derived proxy for Vitamin D status.",
        "biomarkerType": "Nutrient biomarker proxy",
        "moleculeClass": "Vitamin proxy",
        "clinicalSignificance": "Fat-soluble vitamin status",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
    {
        "id": "f6203531-c960-5820-8132-b49b13424e24",
        "name": "ApoB epigenetic proxy",
        "description": "DNA-methylation-derived proxy for apolipoprotein B.",
        "biomarkerType": "Lipid biomarker proxy",
        "moleculeClass": "Lipoprotein proxy",
        "clinicalSignificance": "Atherogenic particle burden",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
    {
        "id": "e0788fbb-3ce8-53f6-92da-01d645f8b823",
        "name": "HbA1c proxy",
        "description": "DNA-methylation-derived proxy for glycated hemoglobin.",
        "biomarkerType": "Metabolic biomarker proxy",
        "moleculeClass": "Clinical chemistry proxy",
        "clinicalSignificance": "Longer-term glycemic status",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
    {
        "id": "e140d636-6cf9-57e0-9440-f84962fcd299",
        "name": "Fasting glucose proxy",
        "description": "DNA-methylation-derived proxy for blood glucose.",
        "biomarkerType": "Metabolic biomarker proxy",
        "moleculeClass": "Metabolite proxy",
        "clinicalSignificance": "Glycemic status",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
    {
        "id": "2b5bb991-7b7a-5c5c-951b-b5930f75b522",
        "name": "IGF-1 proxy",
        "description": "DNA-methylation-derived proxy for insulin-like growth factor 1.",
        "biomarkerType": "Growth and aging biomarker proxy",
        "moleculeClass": "Hormone proxy",
        "clinicalSignificance": "Growth and aging axis",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
    {
        "id": "8e095cf4-cd88-5fef-bed1-bf04bd8c55b3",
        "name": "PFAS exposure proxy",
        "description": "DNA-methylation-derived proxy associated with PFAS exposure.",
        "biomarkerType": "Environmental exposure proxy",
        "moleculeClass": "Toxin exposure proxy",
        "clinicalSignificance": "Environmental toxin burden",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
    {
        "id": "0a8e34b4-16dc-58e1-9d9c-f2b3169abd3e",
        "name": "Beta hydroxybutyrate epigenetic proxy",
        "description": "DNA-methylation-derived proxy for beta hydroxybutyrate.",
        "biomarkerType": "Metabolic biomarker proxy",
        "moleculeClass": "Ketone proxy",
        "clinicalSignificance": "Ketosis and metabolic flexibility",
        "tests": ["TruHealth Epigenetic Biomarker Proxy Test"],
    },
]

INDEX_QUERIES = [
    """
    CREATE FULLTEXT INDEX OrganizationName IF NOT EXISTS
    FOR (n:Organization)
    ON EACH [n.name, n.searchText, n.legalName, n.displayName, n.canonicalTicker]
    """,
    """
    CREATE VECTOR INDEX OrganizationSearchEmbedding IF NOT EXISTS
    FOR (n:Organization) ON (n.searchEmbedding)
    OPTIONS {indexConfig: {
      `vector.dimensions`: 1536,
      `vector.similarity_function`: 'cosine'
    }}
    """,
    """
    CREATE FULLTEXT INDEX ProductSearch IF NOT EXISTS
    FOR (n:Product) ON EACH [
      n.name, n.description, n.searchText, n.productType, n.productClass,
      n.primaryRegulatoryIdentifier, n.regulatoryAuthorizationId
    ]
    """,
    """
    CREATE VECTOR INDEX ProductSearchEmbedding IF NOT EXISTS
    FOR (n:Product) ON (n.searchEmbedding)
    OPTIONS {indexConfig: {
      `vector.dimensions`: 1536,
      `vector.similarity_function`: 'cosine'
    }}
    """,
]


def _with_common_fields(item: dict[str, Any]) -> dict[str, Any]:
    return {
        **item,
        "mongoResearchRunId": RESEARCH_RUN_ID,
        "currentAsOf": CAPTURED_AT,
        "searchFields": ["name", "description", "searchText"],
    }


async def _embed(client: AsyncOpenAI, texts: list[str]) -> list[list[float]]:
    response = await client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
        dimensions=EMBEDDING_DIMENSIONS,
    )
    return [item.embedding for item in sorted(response.data, key=lambda item: item.index)]


async def _run_write(
    transaction: AsyncManagedTransaction,
    query: str,
    **parameters: Any,
) -> None:
    result = await transaction.run(query, **parameters)
    await result.consume()


async def _write_graph(
    transaction: AsyncManagedTransaction,
    organization: dict[str, Any],
    products: list[dict[str, Any]],
    lab_tests: list[dict[str, Any]],
    biomarkers: list[dict[str, Any]],
) -> None:
    await _run_write(
        transaction,
        """
        MERGE (o:Organization {id: $organization.id})
        ON CREATE SET o.createdAt = datetime()
        SET o += $organization,
            o.currentAsOf = datetime($organization.currentAsOf),
            o.updatedAt = datetime(),
            o.embeddingModel = $embeddingModel,
            o.embeddingDimensions = $embeddingDimensions
        """,
        organization=organization,
        embeddingModel=EMBEDDING_MODEL,
        embeddingDimensions=EMBEDDING_DIMENSIONS,
    )
    await _run_write(
        transaction,
        """
        UNWIND $products AS row
        MERGE (p:Product {id: row.id})
        ON CREATE SET p.createdAt = datetime()
        SET p += row,
            p.currentAsOf = datetime(row.currentAsOf),
            p.updatedAt = datetime(),
            p.embeddingModel = $embeddingModel,
            p.embeddingDimensions = $embeddingDimensions
        WITH p
        MATCH (o:Organization {id: $organizationId})
        MERGE (o)-[r:OFFERS]->(p)
        SET r.role = 'product provider',
            r.roleType = 'DISTRIBUTOR',
            r.confidence = 1.0,
            r.recordedFrom = datetime($capturedAt),
            r.mongoResearchRunId = $researchRunId
        """,
        products=products,
        organizationId=organization["id"],
        embeddingModel=EMBEDDING_MODEL,
        embeddingDimensions=EMBEDDING_DIMENSIONS,
        capturedAt=CAPTURED_AT,
        researchRunId=RESEARCH_RUN_ID,
    )
    await _run_write(
        transaction,
        """
        UNWIND $labTests AS row
        MERGE (t:LabTest {id: row.id})
        ON CREATE SET t.createdAt = datetime()
        SET t.name = row.name,
            t.description = row.description,
            t.searchText = row.searchText,
            t.searchFields = ['name', 'description', 'searchText'],
            t.testType = row.testType,
            t.mongoResearchRunId = $researchRunId,
            t.updatedAt = datetime()
        WITH t, row
        UNWIND row.productIds AS productId
        MATCH (p:Product {id: productId})
        MERGE (p)-[r:DELIVERS_LABTEST]->(t)
        SET r.role = 'delivered test',
            r.roleType = 'DISTRIBUTOR',
            r.confidence = 1.0,
            r.recordedFrom = datetime($capturedAt),
            r.mongoResearchRunId = $researchRunId
        """,
        labTests=lab_tests,
        capturedAt=CAPTURED_AT,
        researchRunId=RESEARCH_RUN_ID,
    )
    await _run_write(
        transaction,
        """
        UNWIND $biomarkers AS row
        MERGE (b:Biomarker {id: row.id})
        ON CREATE SET b.createdAt = datetime()
        SET b.name = row.name,
            b.description = row.description,
            b.searchText = row.name + ' | ' + row.description,
            b.searchFields = ['name', 'description', 'searchText'],
            b.biomarkerType = row.biomarkerType,
            b.specimenMatrix = 'dried blood spot',
            b.moleculeClass = row.moleculeClass,
            b.measurementDirectness = 'DNA methylation-derived proxy',
            b.clinicalSignificance = row.clinicalSignificance,
            b.mongoResearchRunId = $researchRunId,
            b.updatedAt = datetime()
        WITH b, row
        UNWIND row.testIds AS testId
        MATCH (t:LabTest {id: testId})
        MERGE (t)-[r:MEASURES]->(b)
        SET r.method = 'DNA methylation-derived proxy',
            r.methodRole = 'estimation',
            r.sampleType = 'finger-prick dried blood spot',
            r.role = 'reported output',
            r.mongoResearchRunId = $researchRunId
        """,
        biomarkers=biomarkers,
        researchRunId=RESEARCH_RUN_ID,
    )


async def main() -> None:
    settings = get_settings()
    driver = await create_neo4j(settings)

    try:
        openai_client = AsyncOpenAI(api_key=settings.openai_api_key.get_secret_value())
        try:
            searchable = [ORGANIZATION, *PRODUCTS]
            embeddings = await _embed(
                openai_client,
                [str(item["searchText"]) for item in searchable],
            )
            organization = {
                **_with_common_fields(ORGANIZATION),
                "searchEmbedding": embeddings[0],
            }
            products = [
                {
                    **_with_common_fields(product),
                    "searchEmbedding": embedding,
                }
                for product, embedding in zip(PRODUCTS, embeddings[1:], strict=True)
            ]
            product_ids = {product["name"]: product["id"] for product in PRODUCTS}
            lab_tests = [
                {
                    **lab_test,
                    "productIds": [product_ids[name] for name in lab_test["products"]],
                }
                for lab_test in LAB_TESTS
            ]
            test_ids = {lab_test["name"]: lab_test["id"] for lab_test in LAB_TESTS}
            biomarkers = [
                {
                    **biomarker,
                    "testIds": [test_ids[name] for name in biomarker["tests"]],
                }
                for biomarker in BIOMARKERS
            ]

            for query in INDEX_QUERIES:
                await driver.execute_query(query)
            await driver.execute_query("CALL db.awaitIndexes(300)")

            async with driver.session() as session:
                await session.execute_write(
                    _write_graph,
                    organization,
                    products,
                    lab_tests,
                    biomarkers,
                )

            query_text = "at-home DNA methylation test for biological age and pace of aging"
            query_embedding = (await _embed(openai_client, [query_text]))[0]
            records, _, _ = await driver.execute_query(
                """
                CALL db.index.vector.queryNodes('ProductSearchEmbedding', 3, $embedding)
                YIELD node, score
                RETURN node.name AS name, score, node.searchText AS searchText
                ORDER BY score DESC
                """,
                embedding=query_embedding,
            )
            counts, _, _ = await driver.execute_query(
                """
                MATCH (o:Organization {id: $organizationId})-[:OFFERS]->(p:Product)
                OPTIONAL MATCH (p)-[:DELIVERS_LABTEST]->(t:LabTest)-[:MEASURES]->(b:Biomarker)
                RETURN count(DISTINCT p) AS products,
                       count(DISTINCT t) AS labTests,
                       count(DISTINCT b) AS biomarkers,
                       size(o.searchEmbedding) AS organizationEmbeddingDimensions,
                       collect(DISTINCT size(p.searchEmbedding)) AS productEmbeddingDimensions
                """,
                organizationId=ORGANIZATION["id"],
            )
            print(
                json.dumps(
                    {
                        "loaded": dict(counts[0]),
                        "vectorQuery": query_text,
                        "vectorResults": [dict(record) for record in records],
                    },
                    indent=2,
                )
            )
        finally:
            await openai_client.close()
    finally:
        await driver.close()


if __name__ == "__main__":
    asyncio.run(main())
