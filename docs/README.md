# Mission Control documentation

- [Glossary](../GLOSSARY.md): the shared language; use its terms and honour its Avoid lists.
- [Decision records](adr/): why the architecture is the way it is; numbered, short, with status.
- [Knowledge bundle](knowledge/index.md): application logic and source/test navigation (Open Knowledge Format).
- [Agent process config](agents/): issue tracker (Linear), triage labels and domain-doc rules used by the engineering skills.
- [Operator guide](MISSION_CONTROL_LOCAL_API.md): configured API, worker, identity and roles.
- [Implementation evidence](MISSION_CONTROL_IMPLEMENTATION_STATUS.md): supported and blocked behavior.
- [Removal guide](REMOVAL_GUIDE.md): clean-break changes, replacements and recovery.
- Tools (`tools/`): `okf_search.py` searches the whole corpus, `validate_okf.py` validates the bundle,
  `agents_docs_index.py` regenerates the compressed index in the root `AGENTS.md`, `okf_frontmatter.py`
  adds frontmatter to new documents, `check_links.py` checks links.

The accepted general architecture is maintained in the sibling
`mission-control-general/general-mission-control` package. This repository records
implementation and qualification evidence; the ADRs restate the accepted decisions in
the shared language and add owner decisions taken since.

The former historical trees `interview_and_research_result_documentation/` (20 files)
and `migrations_instructions/` (65 files) were deleted by the owner (pending,
uncommitted on 2026-10-03) and are not restored. Their content remains recoverable
from commit `f6521c1` (`git show f6521c1:docs/<path>`); the exact path list is in the
[removal guide](REMOVAL_GUIDE.md#pending-owner-documentation-deletions). Their old app
paths, Mongo assignments and process instructions were never current authority.

- [Two-project rollout plan](plans/mission-control-two-project-rollout/IMPLEMENTATION_PLAN.md):
  common-component handoff, G1 contract freeze and docs sync plan.
