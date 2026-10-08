# Pattern: Mechanism and pathway orientation

Use this when the question asks how a drug, gene, or molecular alteration causes an effect through a pathway.

```bash
biomcp search drug imatinib --limit 5
biomcp get drug imatinib targets regulatory
biomcp get gene ABL1 pathways protein
biomcp search article --drug imatinib -g ABL1 -d "chronic myeloid leukemia" --type review --limit 5
```

Interpretation:
- Start with the drug record to identify targets before explaining mechanism.
- Use the gene card and pathways to connect the target to biological process.
- Keep regulatory context separate from mechanistic evidence.
- Use a review search to tie drug, target, and disease into one supported explanation.
