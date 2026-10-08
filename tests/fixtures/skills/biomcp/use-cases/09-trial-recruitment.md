# Pattern: Trial recruitment check

Use this when the question asks whether studies are recruiting for a disease or intervention area.

```bash
biomcp search disease "tick-borne encephalitis" --limit 5
biomcp get disease MONDO:0017572
biomcp search trial -c "tick-borne encephalitis" --status recruiting --limit 5
biomcp search article -d "tick-borne encephalitis" --type review --limit 5
```

Interpretation:
- Resolve the disease name first so trial searches use the intended condition.
- Use `--status recruiting` for current recruitment, not for historical trial existence.
- Read trial titles and interventions before claiming a study matches the user question.
- Use reviews for disease and prevention context when trial hits are sparse or indirect.
