# Pattern: Gene function and localization

Use this when the question asks what a protein does, where it localizes, or which cellular compartment supports its role.

```bash
biomcp get gene OPA1 protein hpa
biomcp get gene OPA1 ontology
biomcp gene pathways OPA1 --limit 5
biomcp search article -g OPA1 -k "mitochondrial intermembrane space localization" --type review --limit 5
```

Interpretation:
- Pull protein, HPA, and ontology sections before relying on free-text literature.
- Treat HPA localization and UniProt protein summaries as structured evidence when present.
- Use pathway results to connect molecular function to biological process.
- Add a targeted review search when the question needs finer localization than the gene card provides.
