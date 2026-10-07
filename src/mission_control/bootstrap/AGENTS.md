# Trusted composition and startup

api.py is the configured FastAPI factory; worker.py selects the app-bound Temporal
worker; preflight.py performs read-only readiness; common_installation.py verifies
the installed common release; composition.py wires actual repositories and
application services. installation.py targets the retired transitional schema and
is pending removal. technical_api.py retains optional lower-level
authoring/control/realtime proof surfaces; api.py remains the configured public
entrypoint. Both use the same handlers and engine.

Validate binding digests, actual persisted installation identity, the attested
release and fingerprint, restricted roles and configured queues before readiness. Startup never invents
catalog definitions or policy grants, migrates schema or fetches token-supplied keys.
MISSION_CONTROL_HOME defaults to the current working directory for operator assets;
do not resolve deployment secrets from the installed wheel location.

storage_mode is production_common only; missing or mismatched evidence fails
closed with no transitional fallback. Test real startup with restricted PostgreSQL
pools and signed identity. Operator guide: docs/MISSION_CONTROL_LOCAL_API.md.
