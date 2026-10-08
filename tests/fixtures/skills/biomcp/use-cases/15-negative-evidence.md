# Pattern: Negative evidence and no-association checks

Use this when the question asks whether an association exists and repeated targeted searches may support a cautious no-evidence answer.

```bash
biomcp search article -k "\"Borna disease virus\" \"brain tumor\"" --type review --limit 5
biomcp search disease "Borna disease" --limit 5
biomcp search article -k "\"Borna disease virus\" glioma association" --limit 5
biomcp search article -k "\"Notch\" CADASIL Pick prion neurodegenerative" --type review --limit 5
```

Interpretation:
- Start with the exact co-occurrence claim before broadening terms.
- Check disease context so a name match is not mistaken for an association.
- Reformulate once with synonyms or disease classes, then stop if evidence remains absent.
- Answer as "no supporting evidence found" rather than proving the association impossible.
