# Pattern: Symptom / phenotype lookup

Use this when the question is "what symptoms or phenotypes are linked to X?"
or "which disease matches these symptoms?"

```bash
biomcp get disease "Marfan syndrome" phenotypes
biomcp discover "developmental delay"
biomcp search phenotype "HP:0001263 HP:0001250"
biomcp search phenotype "seizure, developmental delay" --limit 5
biomcp search article -d "Marfan syndrome" --type review --limit 5
```

Interpretation:
- Start with the phenotype section for normalized HPO-backed findings.
- Use `discover` first when you want BioMCP to surface candidate `HP:` terms
  before ranking diseases.
- Use `search phenotype` directly when you already have HPO IDs or
  comma-separated symptom phrases.
- Supplement with review literature when the phenotype list is short or the question needs fuller clinical presentation.
