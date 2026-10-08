# Pattern: Syndrome name disambiguation

Use this when similar syndrome names may refer to different diseases, genes, phenotypes, or inheritance patterns.

```bash
biomcp search disease "Goldberg-Shprintzen syndrome" --limit 5
biomcp get disease MONDO:0012280 phenotypes
biomcp search disease "Shprintzen-Goldberg syndrome" --limit 5
biomcp search article -k "\"Goldberg-Shprintzen\" \"Shprintzen-Goldberg\"" --type review --limit 5
```

Interpretation:
- Search each similar name separately before merging evidence.
- Use disease IDs and phenotype sections to keep entities distinct.
- Quote both names in article search to find papers that discuss the distinction.
- State which syndrome the evidence supports and do not treat disputed names as synonyms.
