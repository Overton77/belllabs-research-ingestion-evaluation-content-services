# Organization evidence

Versioned companion maps for the 2026-10-03 clean-break cleanup. These documents
preserve path evidence in a fresh checkout; recovery source archives remain local.

- [Initial file map](file-map.json): physical app-to-src relocation destinations.
- [Initial module map](module-map.json): import namespace rewrites.
- [Experiment relocation](experiment-relocation.json): prototype source moved out of the wheel.
- [Biotech module map](biotech-module-map.json): optional application integration extraction.
- [Adapter module map](adapter-module-map.json): concrete ownership replacing infrastructure catchall.
- [Persistence retirements](mongo-removals.json): exact 54 source/test/script paths retired by that slice.

Initial destinations are historical intermediate paths. Apply the later extraction
and adapter supplements, then consult the current source and the
[removal guide](../REMOVAL_GUIDE.md) for deliberate removals and additional changes.
The guide also records capability, Agent Server, script and composition changes
outside the persistence retirement list. This is path provenance, not a claim that
a retired module is supported or that a migration was applied to a live database.
