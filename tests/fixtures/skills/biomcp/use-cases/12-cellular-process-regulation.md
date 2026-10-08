# Pattern: Cellular process regulation

Use this when the question asks which cellular processes a gene regulates or participates in.

```bash
biomcp get gene NANOG
biomcp get gene NANOG ontology
biomcp gene pathways NANOG --limit 5
biomcp search article -g NANOG -k "cell cycle G1 S transition" --limit 5
```

Interpretation:
- Use the gene card first to confirm the symbol and primary biology.
- Use ontology and pathway sections to collect normalized process terms.
- Search articles only after structured terms show the likely process family.
- Report uncertainty when articles discuss a process but structured annotations are weak.
