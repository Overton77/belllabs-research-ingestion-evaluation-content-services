# Pattern: Variant pathogenicity evidence

Use this when the question asks whether a named variant is pathogenic, actionable, or clinically relevant in a disease context.

```bash
biomcp get variant "BRAF V600E" clinvar predictions population
biomcp get variant "BRAF V600E" civic cgi
biomcp variant trials "BRAF V600E" --limit 5
biomcp variant articles "BRAF V600E" --limit 5
```

Interpretation:
- Start with ClinVar, computational predictions, and population frequency before making any pathogenicity claim.
- Compare cancer knowledge bases separately from germline clinical assertions.
- Use trials and articles to anchor disease-specific relevance or therapy context.
- Say when evidence is variant-level but not disease-specific enough to answer the exact question.
