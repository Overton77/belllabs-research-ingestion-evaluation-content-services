# Pattern: Article follow-up via citations and recommendations

Use this when one relevant article answers part of the question and you need to expand or verify the evidence chain.

If a keyword-only article search returns an exact entity suggestion in
`_meta.suggestions[]`, check that structured `get gene`, `get drug`, or
`get disease` command before expanding through citations.

```bash
biomcp get article 22663011 annotations
biomcp article citations 22663011 --limit 5
biomcp article recommendations 22663011 --limit 5
```

Interpretation:
- Use `annotations` to extract standardized entities from the starting paper.
- Use citations to move forward in time and recommendations to broaden laterally.
