# Pattern: Treatment / approved-drug lookup

Use this when the question is "what drugs treat X?" and you need approved-drug signal before broader literature.

```bash
biomcp search drug --indication "myasthenia gravis" --limit 5
biomcp get drug pyridostigmine
biomcp search article -d "myasthenia gravis" --type review --limit 5
```

Interpretation:
- A structured indication hit is the fastest approved-drug answer.
- A structured miss is still informative for approved-drug questions.
- Use review literature next when you need investigational or off-label context.
