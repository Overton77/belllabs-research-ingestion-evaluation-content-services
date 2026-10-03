# Dynamic research swarm experiment

Generated: 2026-08-08T04:47:58.082223+00:00

- Run ID: `swarm-a369645f6564`
- Objective: Assess the peer-reviewed human evidence for fisetin as a senolytic intervention, including study designs, sample sizes, reported outcomes, and major limitations. This is research discussion, not medical advice.
- Mission units: 3
- Immutable source snapshots: 9
- Atomic claims: 17
- Accepted claims: 4
- Rejected claims: 13

## Dynamically planned StageGraph

Search for and extract all peer-reviewed human fisetin senolytic/intervention studies (including trials where fisetin is the active senolytic), then summarize by study design, sample size, dosing/regimen, outcomes (senescence biomarkers and clinical endpoints), and key limitations. Use two parallel research units: (1) comprehensive trial/clinical evidence search; (2) outcome/limitations extraction from the identified primary peer-reviewed papers.

| Unit | Mode | Decomposed question | Search query |
|---|---|---|---|
| `u1` | search | What peer-reviewed human studies (trials/cohorts/case series) evaluate fisetin specifically as a senolytic (or senotherapy intended to clear senescent cells) and what are their basic study characteristics (design, N, dosing/regimen, population, endpoints)? | `(fisetin OR “fisetin treatment” OR “fisetin senolytic”) AND (trial OR randomized OR placebo OR cohort OR “human” OR “postmenopausal”) AND (senolytic OR senotherapy OR senescent OR SASP)` |
| `u2` | decompose_search | From the identified primary peer-reviewed human fisetin senolytic papers, what specific outcomes were reported (senescence/SASP biomarkers, functional or disease endpoints), and what major limitations or threats to validity are described (e.g., small N, short duration, inadequate senescent-cell burden stratification, assay limitations, attrition, missing biomarker data)? | `fisetin senolytic randomized trial biomarkers CTx P1NP “p16” T cell p16 OR “senescent cell burden”` |
| `u3` | search | Are there additional peer-reviewed human studies where fisetin is part of senolytic regimens or compared against other senolytics/senotherapeutics, and do they provide usable evidence for senolytic activity (even if not definitive clearance)? | `(fisetin AND (dasatinib OR quercetin OR “senescent cells” OR “senolytic combination”)) AND (phase OR randomized OR “clinical trial” OR “human study”)` |

## Durable stage lifecycle

| Stage | Status | Launched UTC | Completed UTC | Temporal workflow ID |
|---|---|---:|---:|---|
| bootstrap_search | ADMITTED | 2026-08-08T04:41:32.229110+00:00 | 2026-08-08T04:41:32.945990+00:00 | `stagegraph-experiment:attempt:swarm-a369645f6564:bootstrap_search:1` |
| mission_planner | ADMITTED | 2026-08-08T04:41:33.151201+00:00 | 2026-08-08T04:41:39.765659+00:00 | `stagegraph-experiment:attempt:swarm-a369645f6564:mission_planner:1` |
| research__u1 | ADMITTED | 2026-08-08T04:41:40.029401+00:00 | 2026-08-08T04:41:48.450082+00:00 | `stagegraph-experiment:attempt:swarm-a369645f6564:research__u1:1` |
| research__u2 | ADMITTED | 2026-08-08T04:41:40.024392+00:00 | 2026-08-08T04:41:50.479147+00:00 | `stagegraph-experiment:attempt:swarm-a369645f6564:research__u2:1` |
| research__u3 | ADMITTED | 2026-08-08T04:41:40.023441+00:00 | 2026-08-08T04:41:49.770003+00:00 | `stagegraph-experiment:attempt:swarm-a369645f6564:research__u3:1` |
| synthesize | ADMITTED | 2026-08-08T04:47:34.217390+00:00 | 2026-08-08T04:47:39.613512+00:00 | `stagegraph-experiment:attempt:swarm-a369645f6564:synthesize:1` |

## Claim fidelity evaluations

Admission required exact source attribution, snapshot hash integrity, an exact evidence span,
numeric fidelity, complete numeric declarations, polarity/modality agreement, and deterministic
lexical-support cosine >= 0.20.

| Claim | Disposition | Support score | Text |
|---|---|---:|---|
| `claim:swarm-a369645f6564:u1:1` | REJECT | 0.596 | The TROFFi study is described as a multicenter, phase II, randomized, double-blind, placebo-controlled trial evaluating an oral senolytic agent fisetin intended to target senescence. |
| `claim:swarm-a369645f6564:u1:2` | REJECT | 0.800 | The TROFFi study plans to enroll 88 postmenopausal women with early-stage, high-risk breast cancer who completed neo/adjuvant chemotherapy within the past 12 months and have a 6-minute walk distance (6MWD) <400 m. |
| `claim:swarm-a369645f6564:u1:3` | REJECT | 0.759 | The TROFFi dosing regimen is placebo or fisetin 20 mg/kg/day on days 1-3 of a 14-day cycle for four cycles. |
| `claim:swarm-a369645f6564:u1:4` | REJECT | 0.844 | The TROFFi primary endpoint is change in 6MWD from baseline to end of treatment. |
| `claim:swarm-a369645f6564:u1:5` | ACCEPT | 0.899 | The TROFFi abstract states its objective is to evaluate effects of targeting senescence with the oral senolytic agent fisetin on physical function in chemotherapy-treated postmenopausal breast cancer survivors. |
| `claim:swarm-a369645f6564:u1:6` | REJECT | 0.312 | The provided COVFIS-HOME material is a “PHASE 2 PLACEBO-CONTROLLED PILOT STUDY” protocol document for fisetin, but the excerpt shown does not provide senolytic/senescent-cell clearance-specific trial objectives or endpoints within the provided text. |
| `claim:swarm-a369645f6564:u2:1` | REJECT | 0.716 | In SOURCE u2:S2, the trial tested intermittent administration of “dasatinib plus quercetin (D + Q)” in postmenopausal women, with “(_n_ = 60 participants).” |
| `claim:swarm-a369645f6564:u2:2` | REJECT | 0.979 | In SOURCE u2:S2, the primary endpoint “percentage changes at 20 weeks” in the bone resorption marker “C-terminal telopeptide of type 1 collagen (CTx)” did not differ between groups (P = 0.611). |
| `claim:swarm-a369645f6564:u2:3` | REJECT | 0.757 | In SOURCE u2:S2, the secondary endpoint “percentage changes” in the bone formation marker “procollagen type 1 N-terminal propeptide (P1NP)” increased significantly “at both 2 weeks” (+16%, P = 0.020) and “4 weeks” (+16%, P = 0.024), but was not different from control at “20 weeks” (−9%, P = 0.149). |
| `claim:swarm-a369645f6564:u2:4` | REJECT | 0.783 | In SOURCE u2:S2, exploratory analyses state that skeletal response was “driven principally by women with a high senescent cell burden (highest tertile for T cell p16 … mRNA levels)” where D+Q “increased P1NP (+34%, _P_ = 0.035) and reduced CTx (−11%, _P_ = 0.049) at 2 weeks.” |
| `claim:swarm-a369645f6564:u2:5` | REJECT | 0.900 | In SOURCE u2:S2, the authors conclude “intermittent D + Q treatment did not reduce bone resorption in the overall group of postmenopausal women” and that “further studies are needed testing the hypothesis” that “underlying senescent cell burden may dictate the clinical response.” |
| `claim:swarm-a369645f6564:u2:6` | REJECT | 0.668 | In SOURCE u2:S1, limitations/threats to validity are described as trials having “a small number of participants,” with “feasibility and safety being the primary focus,” and that efficacy conclusions are cautioned because some studies “didn’t have control groups to compare against.” |
| `claim:swarm-a369645f6564:u3:1` | REJECT | 0.536 | SOURCE u3:S1 reports a “subsequent investigation” where DQ was combined with “Fisetin (DQF)” in participants receiving “DQF for 6 months” with DNA methylation assessed at “baseline and 6 months.” |
| `claim:swarm-a369645f6564:u3:2` | ACCEPT | 0.902 | SOURCE u3:S1 states the addition of fisetin resulted in “non-significant increases in epigenetic age acceleration,” described as “suggesting a potential mitigating effect of Fisetin on the impact of DQ on epigenetic aging.” |
| `claim:swarm-a369645f6564:u3:3` | REJECT | 0.683 | SOURCE u3:S1 describes that the initial DQ study used “DQ for 6 months,” with measurements at “baseline, 3 months, and 6 months.” |
| `claim:swarm-a369645f6564:u3:4` | ACCEPT | 0.403 | SOURCE u3:S1 reports that in the DQ (dasatinib+quercetin) study there were “Significant increases in epigenetic age acceleration” at “3 and 6 months,” indicating senolytic regimen-related biomarker effects. |
| `claim:swarm-a369645f6564:u3:5` | ACCEPT | 0.296 | SOURCE u3:S1 explicitly characterizes the regimen context as “senolytic interventions” and identifies DQF as “senolytic.” |

## Final bubbled-up synthesis

- **Overall human evidence (clinical objective):** The TROFFi abstract states the objective is to evaluate the effects of targeting senescence with the oral senolytic agent **fisetin** on **physical function** in **chemotherapy-treated postmenopausal breast cancer survivors** (claim:swarm-a369645f6564:u1:5).

- **Reported human biomarker outcomes (epigenetic aging, TROFFi-related sources):** SOURCE u3:S1 reports that adding fisetin led to **“non-significant increases in epigenetic age acceleration,”** which it interprets as **suggesting a potential mitigating effect** of fisetin on the impact of DQ on epigenetic aging (claim:swarm-a369645f6564:u3:2).

- **Comparison/regimen context (DQ biomarker direction at early timepoints):** SOURCE u3:S1 reports that in the **DQ (dasatinib+quercetin)** study there were **“Significant increases in epigenetic age acceleration”** at **3 and 6 months**, consistent with regimen-related biomarker effects in that context (claim:swarm-a369645f6564:u3:4).

- **Mechanistic/regimen framing in the sources:** SOURCE u3:S1 explicitly characterizes the regimen context as **“senolytic interventions”** and identifies **DQF** as **“senolytic”** (claim:swarm-a369645f6564:u3:5).

- **Major limitations (as far as can be inferred from the accepted claims provided):** The accepted claims only specify direction and statistical description for epigenetic-age acceleration (non-significant vs significant) and do not provide sufficient detail here to assess study design, sample sizes, primary/secondary endpoints beyond the objective statement, adverse events, adherence, dosing duration, blinding/randomization, or the full limitations discussion. Therefore, the limitations summary is constrained to what the claims explicitly state (claims:swarm-a369645f6564:u3:2, claim:swarm-a369645f6564:u3:4, claim:swarm-a369645f6564:u1:5).

Claims used: `claim:swarm-a369645f6564:u1:5`, `claim:swarm-a369645f6564:u3:2`, `claim:swarm-a369645f6564:u3:4`, `claim:swarm-a369645f6564:u3:5`

Limitations: The accepted claims provided do not include sample sizes, full study design details (e.g., randomized vs non-randomized, control arms, blinding), specific outcome measures beyond physical function objective and epigenetic-age-acceleration direction, or detailed limitations text; therefore these cannot be fully assessed from the allowed evidence.; No dosing, follow-up duration, statistical effect sizes, multiple-comparisons handling, or adverse event reporting are included in the accepted claims provided.

## Immutable source ledger

| Source | Title | URL | Text digest |
|---|---|---|---|
| `u1:S1` | A phase II randomized placebo-controlled study of fisetin to improve physical function in breast cancer survivors: the TROFFi study rationale and trial design - PubMed | https://pubmed.ncbi.nlm.nih.gov/41835341 | `d50557d0efae` |
| `u1:S2` | [PDF] NCT04771611 4/18/2022 - ClinicalTrials.gov | https://cdn.clinicaltrials.gov/large-docs/11/NCT04771611/Prot_SAP_000.pdf | `68e88670529e` |
| `u1:S3` | A phase II randomized double-blind placebo-controlled study of fisetin to improve physical function in frail older breast cancer survivors (TROFFi). | https://ascopubs.org/doi/10.1200/JCO.2024.42.16_suppl.TPS1645 | `f9a8a394fbdd` |
| `u2:S1` | Personalized Medicine Approach to Senolytics Clinical Trials | https://lifespan.io/personalized-medicine-approach-to-senolytics-clinical-trials | `92dfe887325d` |
| `u2:S2` | Effects of intermittent senolytic therapy on bone metabolism in postmenopausal women: a phase 2 randomized controlled trial | https://pmc.ncbi.nlm.nih.gov/articles/PMC11705617 | `787019a91d43` |
| `u2:S3` | Effects of intermittent senolytic therapy on bone metabolism in postmenopausal women: a phase 2 randomized controlled trial - PubMed | https://pubmed.ncbi.nlm.nih.gov/38956196 | `90208462cff5` |
| `u3:S1` | Exploring the effects of Dasatinib, Quercetin, and Fisetin on ... | https://pmc.ncbi.nlm.nih.gov/articles/PMC10929829 | `c8a37936a8ca` |
| `u3:S2` | Intermittent Senolytic Therapy with Dasatinib and Quercetin: Phase 2 Trial Shows Potential Benefits for Bone Health in Postmenopausal Women with High Senescent Cell Burden - Gilmore Health News | https://www.gilmorehealth.com/intermittent-senolytic-therapy-with-dasatinib-and-quercetin-phase-2-trial-shows-potential-benefits-for-bone-health-in-postmenopausal-women-with-high-senescent-cell-burden | `77f7e3087dcc` |
| `u3:S3` | Dasatinib and Quercetin: The First Senolytic Combination Studied in Humans - Semaglutide Guide | https://semaglutideguide.net/dasatinib-quercetin-senolytic-combination-humans-2 | `e700e464b18f` |

## Acceptance

- PASS: an LLM planned multiple bounded research units.
- PASS: each mission produced source evidence.
- PASS: deterministic gates admitted at least one atomic claim.
- PASS: synthesis used only admitted claim IDs.
- PASS: each generic dynamic stage executed as a separate durable Temporal workflow.
- PASS: LangGraph checkpoints and outbox wakes resumed the same thread.

## Recovery and trace evidence

The driver was stopped after a reconciliation persistence error. `--resume swarm-a369645f6564`
loaded the same PostgreSQL checkpoint, retried only the failed `reconcile` task, and did not relaunch
bootstrap search, mission planning, or any of the three completed research workflows.

LangSmith root traces:

- [mission planner](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdfad-2725-7383-96c5-593621175f4b?poll=true)
- [research unit u1](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdfad-465f-7250-8875-93cc2f21027f?poll=true)
- [research unit u2](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdfad-46e4-7461-bf40-480c36f45494?poll=true)
- [research unit u3](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdfad-4ecb-7f02-831c-df8ca5a87893?poll=true)
- [final synthesis](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdfb2-a393-72e0-be19-61495163f5ef?poll=true)

## Important limitations

- This proves fidelity to captured source text, not that a source is true, current, unbiased, or
  scientifically high quality.
- The deterministic semantic-support gate is a lexical cosine proxy. Production needs a pinned,
  calibrated biomedical NLI/embedding evaluator plus entity, temporal, and unit ontologies.
- Search uses Tavily advanced search with bounded raw text. Production should use the workspace's
  reviewed MCP adapters and immutable external artifact storage.
- This captured run preserves provider mojibake in several source snapshots. Future retrievals repair
  the known UTF-8/CP1252 pattern before hashing; immutable records from this run were not rewritten.
- The planner summary says "two" parallel units while its structured plan contains three. Production
  needs a deterministic plan-coherence evaluator before admitting a mission revision.
- Search relevance did not guarantee source quality: two secondary commercial pages entered the
  retrieval ledger. A separate source-quality and publication-type admission policy is required.
- The proof dynamically creates mission data executed by fixed generic node types. Runtime code/node
  injection remains forbidden; safe graph injection means a new validated mission-plan revision.
