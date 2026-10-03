# Mission Control agent guide

Read the accepted [general specification](../mission-control-general/general-mission-control/SPECIFICATION.md)
and relevant runtime/database/workflow annex, then the closest scoped AGENTS.md.
[Knowledge navigation](docs/knowledge/index.md) explains application logic.
[Removal guide](docs/REMOVAL_GUIDE.md) explains changed paths and recovery.

## Work by concern

| Concern | Start |
| --- | --- |
| Public request, identity or permissions | src/mission_control/interfaces/http and bootstrap/api.py |
| Admission, commands, lifecycle | src/mission_control/application/missions and execution |
| StageGraph or GoalDirected decisions | src/mission_control/domain/programs and application/programs |
| Durable workflow mechanics | src/mission_control/adapters/temporal |
| Fork, checkpoint incident or reconciliation | src/mission_control/application/recovery |
| Persistent records or migrations | src/mission_control/adapters/postgres |
| Directory skills, tools or providers | src/mission_control/adapters/capabilities and deep_agents |
| Application-specific knowledge | integrations/biotech; never import it from the kernel |
| Startup, restricted pools or installation | src/mission_control/bootstrap |

## Invariants

Interfaces call application handlers; application uses domain rules and ports;
adapters implement ports; bootstrap wires them. Domain imports no database,
Temporal, FastAPI or provider SDK. One distribution imports mission_control from
src/mission_control; do not recreate app/ as a compatibility kernel.

Temporal alone schedules missions. Deep Agents is bounded operation cognition.
Reducers own lifecycle and settlement. Accepted command, delivered signal and
applied effect are distinct; execution completion is not mission acceptance.

Application PostgreSQL and Temporal persistence are separate. Common SQL belongs
to its independent owner. Local transitional readiness is not production readiness.
Do not invent missing provider bindings, schemas, grants or model routes.

Preserve dirty work and user files, including ignored app/personal_code. Use
recoverable checkpoints before broad cleanup. No live destructive migrations,
volume deletion, commit, push, deployment or paid experiments without authorization.
Historical BellLabs WPs/specs are provenance, not competing current authority.

## Verify

Run `uv run ruff check src tests`, `uv run mypy src/mission_control` and relevant
`uv run --group biotech pytest` selections (the optional group is needed for
Biotech-domain tests; plain runtime installation remains independent).
Use real local PostgreSQL/Temporal for persistence and
runtime claims. Report passed, failed, blocked and unrun checks separately.
Operational setup: [operator guide](docs/MISSION_CONTROL_LOCAL_API.md).
