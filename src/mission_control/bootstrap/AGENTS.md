# Trusted composition and startup

api.py is the configured FastAPI factory; worker.py selects the app-bound Temporal
worker; preflight.py performs read-only readiness; installation.py explicitly
registers installation identity; composition.py wires actual repositories and
application services. technical_api.py retains optional lower-level
authoring/control/realtime proof surfaces; api.py remains the configured public
entrypoint. Both use the same handlers and engine.

Validate binding digests, actual persisted installation identity, restricted roles,
migration receipts and configured queues before readiness. Startup never invents
catalog definitions or policy grants, migrates schema or fetches token-supplied keys.
MISSION_CONTROL_HOME defaults to the current working directory for operator assets;
do not resolve deployment secrets from the installed wheel location.

Production common-schema mode fails closed until its released component and
adapters are qualified. Test real startup with restricted PostgreSQL pools and
signed identity. Operator guide: docs/MISSION_CONTROL_LOCAL_API.md.
