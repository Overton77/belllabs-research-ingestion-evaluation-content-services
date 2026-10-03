---
okf_version: "0.1"
---

# Mission Control knowledge

An Open Knowledge Format explanation bundle using the adjacent Biotech catalog's
minimal v0.1 profile. These documents describe code and evidence; accepted general
specifications remain authoritative. Upstream OKF now also publishes v0.2; this
bundle does not claim optional trust or attestation machinery.

| Task | Concept |
| --- | --- |
| Find ownership or change a module | [Architecture](architecture.md) |
| Admit or control a run | [Lifecycle](lifecycle.md) |
| Understand StageGraph and GoalDirected | [Execution](execution.md) |
| Snapshot, fork or repair a run | [Recovery](recovery.md) |
| Diagnose roles, schema or replayed records | [Persistence](persistence.md) |
| Pin and materialize a skill directory | [Capabilities](capabilities.md) |
| Configure identity and start processes | [Operations](operations.md) |
| Assess what tests prove | [Qualification](qualification.md) |

[Update log](log.md). Search: `rg -n -i "admission|fork|binding" docs/knowledge`.

# Citations

- [Open Knowledge Format specification](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md).
- Local convention: `../biotech-knowledge-catalog/README.md` and its
  `tools/validate_okf.py`; one concept per file with YAML type, reserved index/log.
