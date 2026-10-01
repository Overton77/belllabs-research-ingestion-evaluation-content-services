# TruDiagnostic — Products, Lab Tests & Biomarkers

> Research report generated 2026-03-30 for LLM ingestion pipeline.
> Entity: Organization:TruDiagnostic

---

## 0. Temporal Context for Ingestion

This report was captured on **2026-03-30**. All ProductState snapshots describe the product as of this date.

### ProductState temporal fields

| Product | validFrom | validTo | recordedFrom | Notes |
|---------|-----------|---------|--------------|-------|
| TruAge™ | `2021-01-01T00:00:00Z` | `null` | `2026-03-30T00:00:00Z` | In operation since at least 2021 (15K+ patients on EPIC array); current state |
| TruHealth™ | `2024-01-01T00:00:00Z` | `null` | `2026-03-30T00:00:00Z` | Explicitly launched 2024; current state |
| TruAge + TruHealth™ Combo | `2024-01-01T00:00:00Z` | `null` | `2026-03-30T00:00:00Z` | Available since TruHealth launched |

Pricing (~$299, ~$299, ~$499) is as of the 2026-03-30 report date.

### Algorithm / TechnologyPlatform validFrom estimates

| Platform | validFrom | Notes |
|----------|-----------|-------|
| OMICmAge™ | `2023-01-01T00:00:00Z` | Released 2023 (bioRxiv preprint Oct 2023) |
| SYMPHONYAge™ | `2023-01-01T00:00:00Z` | Released 2023 (bioRxiv preprint Jul 2023) |
| DunedinPACE™ | `2021-01-01T00:00:00Z` | Licensed by TruDiagnostic 2021 |
| DeepStrataAge | `2026-01-01T00:00:00Z` | Published NPJ Aging 2026 |
| Imprintome Array | `2023-01-01T00:00:00Z` | Collaboration initiated 2023; bioRxiv Jan 2024 |
| Epigenetic Biomarkers™ | `2024-01-01T00:00:00Z` | Underlies TruHealth™ launched 2024 |

---

## 1. Product Catalog


| Product                       | Type                              | Estimated Price                       | Collection Method                    | Shop URL                                                                                                                                                       |
| ----------------------------- | --------------------------------- | ------------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **TruAge™**                   | Epigenetic Aging Test             | ~$299 (with 20% off promotional code) | At-home finger-prick blood spot card | [https://shop.trudiagnostic.com/products/truage-complete-epigenetic-collection](https://shop.trudiagnostic.com/products/truage-complete-epigenetic-collection) |
| **TruHealth™**                | Nutritional / Systems Health Test | ~$299                                 | At-home finger-prick blood spot card | [https://shop.trudiagnostic.com/products/truhealth](https://shop.trudiagnostic.com/products/truhealth)                                                         |
| **TruAge + TruHealth™ Combo** | Combined panel                    | ~$499                                 | At-home finger-prick blood spot card | [https://shop.trudiagnostic.com/products/truage-truhealth](https://shop.trudiagnostic.com/products/truage-truhealth)                                           |


- **Payment:** HSA/FSA accepted (qualifying medical expense)
- **Turnaround:** 2–3 weeks from lab receipt of sample
- **Sample reports available:**
  - TruAge: [https://cdn.prod.website-files.com/6690e254f4f07d1a469bb330/684b0280d60621d1be91641c_ExampleAdvancedTruAge.pdf](https://cdn.prod.website-files.com/6690e254f4f07d1a469bb330/684b0280d60621d1be91641c_ExampleAdvancedTruAge.pdf)
  - TruHealth: [https://cdn.prod.website-files.com/6690e254f4f07d1a469bb330/684b02801b74450c5587b2c5_ExampleTruHealth.pdf](https://cdn.prod.website-files.com/6690e254f4f07d1a469bb330/684b02801b74450c5587b2c5_ExampleTruHealth.pdf)

---

## 2. TruAge™ Lab Test — Complete Specification

TruAge is TruDiagnostic's flagship product: a comprehensive epigenetic aging assessment derived from DNA methylation analysis of 850,000+ CpG sites (marketed as "1 million+") using the Illumina Infinium MethylationEPIC v2 BeadChip array.

### 2.1 Report Components

#### 2.1.1 Biological Age Score (OMICmAge™)

- **Developed with:** Harvard University (Jessica A. Lasky-Su, Brigham and Women's Hospital)
- **Method:** Multi-omic approach that integrates epigenetic data with dozens of proteins, metabolites, and clinical biomarkers. Unlike first-generation clocks (Horvath, Hannum) that use only CpG sites, OMICmAge captures multi-omic aging signatures through DNA methylation proxies.
- **Generation:** 2nd/3rd generation algorithm
- **Publication:** bioRxiv 2023.10.16.562114v2; Nature Aging PMID 41741793 (2026)
- **Output:** Single biological age score representing overall aging status

#### 2.1.2 11 Organ Systems Age Scores (SYMPHONYAge™)

- **Developed with:** Yale University (Albert T. Higgins-Chen, Morgan Levine)
- **Method:** System-specific epigenetic aging clocks trained on organ-specific biomarker data
- **Organ systems measured:**
  1. Brain
  2. Heart
  3. Liver
  4. Lung
  5. Kidney
  6. Hormone (endocrine system)
  7. Immune System
  8. Musculoskeletal
  9. Inflammation
  10. Blood (hematological)
  11. Metabolic
- **Generation:** 2nd generation
- **Publication:** bioRxiv 2023.07.13.548904v1
- **Output:** 11 independent biological age scores, each reflecting a different physiological system

#### 2.1.3 Pace of Aging (DunedinPACE™)

- **Developed by:** Duke University / Columbia University
- **Licensed by:** TruDiagnostic (exclusive commercial license)
- **Method:** Measures the rate of biological aging per calendar year based on longitudinal methylation changes in the Dunedin birth cohort
- **Generation:** 2nd generation
- **Output:** A pace score (e.g., 0.80 = aging at 80% of typical rate; 1.20 = aging 20% faster than typical)
- **Interpretation:** Values below 1.0 indicate slower-than-average aging; values above 1.0 indicate accelerated aging

#### 2.1.4 OMICm FitAge

- **Method:** Biological age calculation based on physical fitness and functionality epigenetic biomarkers
- **Output:** Age score reflecting physical/functional aging status

#### 2.1.5 Telomere Length Report

- **Method:** DNA methylation-based estimation of telomere length (epigenetic proxy, not qPCR measurement)
- **Output:** Estimated telomere length relative to age-matched population

#### 2.1.6 12-Cell Immunity Report

- **Method:** Deconvolution of immune cell proportions from bulk methylation data
- **Cell types measured:** 12 distinct immune cell populations including major lymphocyte and myeloid subsets
- **Output:** Immune cell concentrations, ratios, and associations with death/disease risk
- **Publication basis:** Genome Medicine immune cell fractions meta-analysis (23K samples)

#### 2.1.7 Inflammation Score (CRP & IL-6)

- **Biomarkers:** Epigenetic proxies for C-reactive protein (CRP) and Interleukin-6 (IL-6)
- **Method:** DNA methylation surrogates trained on measured CRP/IL-6 values
- **Output:** Inflammation status without requiring a separate blood draw for serum protein measurement

#### 2.1.8 Smoking Impact Score

- **Method:** Epigenetic assessment of smoking's cumulative impact
- **Output:** Impact on skin aging, blood aging, and liver aging from tobacco exposure

#### 2.1.9 Alcohol Impact Score

- **Method:** Epigenetic assessment of alcohol's cumulative impact on aging processes
- **Output:** Lifetime alcohol consumption effects on biological aging rate

#### 2.1.10 Weight Loss Response

- **Method:** Epigenetic assessment of biological programming for dietary intervention response
- **Output:** Predicted responsiveness to weight loss interventions based on methylation patterns

#### 2.1.11 Type 2 Diabetes Risk Score

- **Method:** Epigenetic risk assessment for Type 2 Diabetes
- **Output:** Risk score derived from diabetes-associated methylation patterns

#### 2.1.12 Cancer Risk

- **Method:** Methylation-based cancer risk assessment (replaces mitotic clock in current version)
- **Output:** Cancer risk score

#### 2.1.13 Mortality Risk

- **Method:** Epigenetic mortality risk assessment
- **Output:** All-cause mortality risk derived from methylation patterns

#### 2.1.14 Grip Strength & Gait Speed

- **Method:** Epigenetic proxies for physical fitness indicators
- **Output:** Estimated grip strength and gait speed based on methylation biomarkers — indicators of functional aging and frailty risk

---

## 3. TruHealth™ Lab Test — Complete Biomarker Catalog

TruHealth is TruDiagnostic's nutritional and systems health assessment, measuring 105+ biomarkers entirely through DNA methylation proxies (Epigenetic Biomarkers™). No separate blood panel is required — all values are derived from the same finger-prick blood spot used for TruAge.

### 3.1 Vitamins (11 biomarkers)


| Biomarker                     | Category              |
| ----------------------------- | --------------------- |
| Vitamin A                     | Fat-soluble vitamin   |
| Vitamin B2 (Riboflavin)       | B-complex vitamin     |
| Vitamin B3 (Niacin)           | B-complex vitamin     |
| Vitamin B5 (Pantothenic Acid) | B-complex vitamin     |
| Vitamin B6 (Pyridoxine)       | B-complex vitamin     |
| Vitamin B8 (Inositol)         | B-complex vitamin     |
| Vitamin C (Ascorbic Acid)     | Water-soluble vitamin |
| Vitamin D                     | Fat-soluble vitamin   |
| Vitamin E                     | Fat-soluble vitamin   |
| Choline                       | Essential nutrient    |
| Betaine                       | Methyl donor nutrient |


### 3.2 Amino Acids (18 biomarkers)


| Biomarker                        | Relevance                                      |
| -------------------------------- | ---------------------------------------------- |
| Methionine                       | Methyl donor, one-carbon metabolism            |
| Cysteine                         | Glutathione precursor                          |
| S-Methylmethionine               | Methylation intermediate                       |
| Taurine                          | Cell membrane stabilizer, longevity-associated |
| Ergothioneine                    | Cellular antioxidant, longevity biomarker      |
| Glutamine                        | Immune function, gut health                    |
| Arginine                         | Nitric oxide precursor                         |
| L-Aspartic Acid                  | Amino acid metabolism                          |
| Valine                           | Branched-chain amino acid                      |
| Asparagine                       | Amino acid metabolism                          |
| Threonine                        | Essential amino acid                           |
| Glycine                          | Collagen synthesis, longevity-associated       |
| Carnosine                        | Anti-glycation, anti-aging                     |
| Cystathionine                    | Transsulfuration pathway                       |
| Histidine                        | Essential amino acid                           |
| Citrulline                       | Urea cycle, vascular health                    |
| Dimethylarginine (ADMA and SDMA) | Cardiovascular risk markers                    |
| Homocitrulline                   | Urea cycle marker                              |
| Dimethyllysine                   | Protein methylation marker                     |


### 3.3 Antioxidants (2 biomarkers)


| Biomarker          | Relevance                               |
| ------------------ | --------------------------------------- |
| Acetyl-L-Carnitine | Mitochondrial function, neuroprotection |
| Carotenoids        | Dietary antioxidant status              |


### 3.4 Fats and Cellular Membranes (11 biomarkers)


| Biomarker                                                | Relevance                                 |
| -------------------------------------------------------- | ----------------------------------------- |
| Omega-3 (total)                                          | Anti-inflammatory fatty acid              |
| DHA (Docosahexaenoic Acid)                               | Brain health, omega-3                     |
| DPA (Docosapentaenoic Acid)                              | Omega-3 intermediate                      |
| EPA (Eicosapentaenoic Acid)                              | Anti-inflammatory omega-3                 |
| Omega-6 (total)                                          | Pro-inflammatory fatty acid balance       |
| LA (Linoleic Acid)                                       | Essential omega-6                         |
| PUFA (Polyunsaturated Fatty Acids)                       | Total polyunsaturated fats                |
| MUFA (Monounsaturated Fatty Acids)                       | Oleic acid family                         |
| SFA (Saturated Fatty Acids)                              | Saturated fat status                      |
| Pentadecanoate                                           | Odd-chain fatty acid, dairy intake marker |
| Phosphoglycerides / Phosphatidylcholine / Sphingomyelins | Cell membrane lipid composition           |


### 3.5 Lipid Peroxidation (3 biomarkers)


| Biomarker                      | Relevance                               |
| ------------------------------ | --------------------------------------- |
| Phospholipase A2               | Membrane lipid metabolism, inflammation |
| Glutathione Peroxidase         | Antioxidant enzyme activity             |
| Octadecadienedioate (C18:2-DC) | Lipid peroxidation product              |


### 3.6 Serum Lipids (12 biomarkers)


| Biomarker                 | Relevance                           |
| ------------------------- | ----------------------------------- |
| ApoB (Apolipoprotein B)   | Atherogenic particle count          |
| LDL-C (LDL Cholesterol)   | "Bad" cholesterol                   |
| LDL Particle Size         | Small dense LDL = higher risk       |
| VLDL-C (VLDL Cholesterol) | Triglyceride-rich lipoprotein       |
| VLDL Particle Size        | Metabolic health indicator          |
| ApoA1 (Apolipoprotein A1) | Cardioprotective marker             |
| HDL-C (HDL Cholesterol)   | "Good" cholesterol                  |
| HDL Particle Size         | Larger = more protective            |
| Total Triglycerides       | Metabolic health                    |
| Remnant Cholesterol       | Emerging cardiovascular risk marker |
| ApoC-II                   | Triglyceride metabolism regulator   |
| APOE / PCSK9              | Lipid metabolism regulators         |


### 3.7 Blood Pressure (2 biomarkers)


| Biomarker                                   | Relevance                                           |
| ------------------------------------------- | --------------------------------------------------- |
| Vanilla Acetic Acid (Vanillylmandelic Acid) | Catecholamine metabolism, blood pressure regulation |
| Phenylacetylglutamine                       | Gut microbiome-derived cardiovascular risk marker   |


### 3.8 Metabolic Markers (7 biomarkers)


| Biomarker                      | Relevance                                     |
| ------------------------------ | --------------------------------------------- |
| HgbA1c (Glycated Hemoglobin)   | 3-month blood sugar average                   |
| Glucose                        | Blood sugar level                             |
| Fat Burning Marker             | Metabolic flexibility indicator               |
| Satiety Hormone                | Appetite regulation (likely leptin proxy)     |
| Phenylalanine Dysbiosis Marker | Gut microbiome health indicator               |
| Adiponectin                    | Insulin sensitivity, metabolic health         |
| Erythritol                     | Sugar alcohol metabolism, cardiovascular risk |


### 3.9 Immune Markers (7 biomarkers)


| Biomarker                          | Relevance                            |
| ---------------------------------- | ------------------------------------ |
| White Blood Cell Count             | Total immune cell count              |
| Neutrophil Count                   | Innate immune cells                  |
| Lymphocyte Count                   | Adaptive immune cells                |
| CRP (C-Reactive Protein)           | Systemic inflammation                |
| Neutrophil to Lymphocyte Ratio     | Inflammation/immune balance          |
| Systemic Immune-Inflammation Index | Composite immune marker              |
| CD4/CD8 Ratio                      | T-cell balance, immunosenescence     |
| Cystatin-C                         | Kidney function, cardiovascular risk |
| HSP-90 alpha                       | Heat shock protein, cellular stress  |


### 3.10 Neurocognitive Markers (5 biomarkers)


| Biomarker                           | Relevance                             |
| ----------------------------------- | ------------------------------------- |
| Memory Health Protein               | Neurocognitive function indicator     |
| Brain Inflammation Marker           | Neuroinflammation status              |
| Cell Repair Marker                  | Neuronal repair capacity              |
| Brain Anti-Inflammatory Protein     | Neuroprotective factor                |
| VGF (Nerve Growth Factor Inducible) | Neurotrophic factor, cognitive health |


### 3.11 Inflammation Markers (7 biomarkers)


| Biomarker                            | Relevance                         |
| ------------------------------------ | --------------------------------- |
| IL-6 (Interleukin-6)                 | Pro-inflammatory cytokine         |
| Oxidative Stress Marker              | Systemic oxidative burden         |
| Serum Amyloid A-1 Protein            | Acute phase reactant              |
| Glycoprotein Acetyls (GlycA)         | Chronic inflammation composite    |
| IGF-1 (Insulin-like Growth Factor 1) | Growth/aging axis                 |
| DHEA Sulfate                         | Adrenal function, aging biomarker |


### 3.12 Stress Markers (2 biomarkers)


| Biomarker               | Relevance                 |
| ----------------------- | ------------------------- |
| Adrenal Activity Marker | HPA axis function         |
| Chronic Stress Marker   | Allostatic load indicator |


### 3.13 Toxins (4 biomarkers)


| Biomarker                                  | Relevance                         |
| ------------------------------------------ | --------------------------------- |
| PFAS (Per- and Polyfluoroalkyl Substances) | Environmental toxin exposure      |
| Acrolein                                   | Combustion/smoking byproduct      |
| Polycyclic Aromatic Hydrocarbons           | Environmental carcinogen exposure |
| Pesticides / Lead Exposure                 | Heavy metal and pesticide burden  |


### 3.14 Uric Acid Pathway (3 biomarkers)


| Biomarker | Relevance                                     |
| --------- | --------------------------------------------- |
| Uric Acid | Purine metabolism, gout risk, cardiovascular  |
| Xanthine  | Purine metabolism intermediate                |
| Allantoin | Uric acid oxidation product, oxidative stress |


### 3.15 Mitochondrial Function (3 biomarkers)


| Biomarker                | Relevance                         |
| ------------------------ | --------------------------------- |
| Energy Balance Marker    | Mitochondrial output indicator    |
| ATP Synthase             | Cellular energy production enzyme |
| Energy Transport Protein | Mitochondrial substrate transport |


### 3.16 Oxidative Defense (3 biomarkers)


| Biomarker                  | Relevance                             |
| -------------------------- | ------------------------------------- |
| Myeloperoxidase            | Oxidative enzyme, cardiovascular risk |
| Oxidative Stress Indicator | Systemic oxidative status             |
| Oxidative Damage Marker    | DNA/protein oxidative damage          |


### 3.17 NAD+ Metabolism (2 biomarkers)


| Biomarker                    | Relevance                                  |
| ---------------------------- | ------------------------------------------ |
| Byproduct Marker             | NAD+ metabolism intermediate               |
| 1-MNA (1-Methylnicotinamide) | NAD+ metabolism endpoint, longevity marker |


### 3.18 Ketones (1 biomarker)


| Biomarker            | Relevance                                |
| -------------------- | ---------------------------------------- |
| Beta Hydroxybutyrate | Ketosis indicator, metabolic flexibility |


### 3.19 Supplements (3 biomarkers)


| Biomarker                   | Relevance                                    |
| --------------------------- | -------------------------------------------- |
| Alpha-Ketoglutarate         | TCA cycle intermediate, longevity supplement |
| Spermidine                  | Autophagy inducer, longevity supplement      |
| Palmitoylethanolamide (PEA) | Anti-inflammatory lipid, supplement          |


**Total named biomarkers: ~105**

---

## 4. Proprietary Algorithms & Technologies

### 4.1 OMICmAge™

- **Developer:** TruDiagnostic + Harvard University
- **Type:** Multi-omic biological age clock
- **Generation:** 2nd/3rd generation
- **Method:** Integrates DNA methylation proxies for proteins, metabolites, and clinical biomarkers into a composite aging score
- **Publication:** bioRxiv 2023.10.16.562114v2 → Nature Aging PMID 41741793 (2026)
- **Differentiator:** First clock to use epigenetic surrogates for multi-omic data, combining the precision of methylation measurement with the biological depth of proteomics and metabolomics

### 4.2 SYMPHONYAge™ (Systems Age)

- **Developer:** TruDiagnostic + Yale University (Higgins-Chen, Levine)
- **Type:** Organ-specific aging clock system
- **Generation:** 2nd generation
- **Method:** 11 independent clocks, each trained on organ-specific biomarkers
- **Publication:** bioRxiv 2023.07.13.548904v1
- **Differentiator:** Provides granular organ-level aging data instead of a single composite score

### 4.3 DunedinPACE™

- **Developer:** Duke University / Columbia University
- **Licensing:** Exclusively licensed to TruDiagnostic for commercial use
- **Type:** Pace of aging clock
- **Generation:** 2nd generation
- **Method:** Derived from longitudinal data in the Dunedin Multidisciplinary Health and Development Study birth cohort
- **Differentiator:** Measures the *rate* of aging rather than a static biological age — captures dynamic change

### 4.4 DeepStrataAge

- **Developer:** TruDiagnostic (internal, Dwaraka et al.)
- **Type:** Deep learning-based aging clock
- **Method:** Stage-divergent and sex-divergent deep learning model for biological age
- **Publication:** NPJ Aging, PMID 41826374 (2026)
- **Differentiator:** Uses deep learning (neural networks) rather than elastic net regression; accounts for non-linear aging dynamics and sex-specific aging patterns

### 4.5 Imprintome Array

- **Developer:** TruDiagnostic + NC State University + Duke University
- **Type:** Custom methylation array (22,819 probes)
- **Method:** Targeted array focusing on genomic imprinting regions — a novel approach to measuring epigenetic regulation of imprinted genes
- **Publication:** bioRxiv 2024.01.15.575646v1
- **Differentiator:** First custom array designed specifically for imprinting analysis at scale

### 4.6 Epigenetic Biomarkers™

- **Developer:** TruDiagnostic + Harvard University
- **Type:** Platform technology for TruHealth™
- **Method:** 1,670+ biomarkers derived from DNA methylation surrogates for proteins, metabolites, vitamins, toxins, and clinical markers
- **Differentiator:** Enables a comprehensive nutritional and health panel from a single methylation assay — no additional blood draws needed

### 4.7 OMICm FitAge

- **Developer:** TruDiagnostic
- **Type:** Physical fitness biological age
- **Method:** Epigenetic proxies for physical fitness markers (grip strength, gait speed, functional capacity)

### 4.8 Retroelement-Age Clocks

- **Developer:** TruDiagnostic (Dwaraka) + Weill Cornell (Ndhlovu)
- **Type:** HERV/LINE-1 methylation-based aging clocks
- **Method:** Measures methylation of human endogenous retroviruses and LINE-1 transposable elements as aging biomarkers
- **Differentiator:** Novel target class (retroelements) for aging measurement; connects aging to transposon derepression

---

## 5. Technology Platform


| Component                 | Specification                                              |
| ------------------------- | ---------------------------------------------------------- |
| **Array**                 | Illumina Infinium MethylationEPIC v2 BeadChip              |
| **CpG Sites**             | 850,000+ (marketed as "1 million+")                        |
| **Sample Type**           | Dried blood spot (finger-prick blood spot card)            |
| **Lab Certification**     | CLIA-certified, HIPAA-compliant                            |
| **Processing**            | In-house at Lexington, KY facility                         |
| **Custom Arrays**         | Imprintome (22,819 probes) for targeted imprinting studies |
| **Database**              | 200,000+ patient methylation profiles                      |
| **Turnaround**            | 2–3 weeks from sample receipt                              |
| **ICC (Reproducibility)** | >0.98 for all primary outputs                              |


