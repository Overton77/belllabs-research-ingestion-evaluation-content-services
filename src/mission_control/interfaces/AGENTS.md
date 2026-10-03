# Public transport

http exposes typed application-scoped operations; cli uses the public client;
mcp exposes bounded transport capabilities. Interfaces call application handlers
rather than implementing a second lifecycle reducer or scheduler.

Authenticate before resolving scoped services. Verify service scope against the
trusted installation/application/tenant. Request fields and user-editable token
metadata cannot select pools, grants, bootstrap factories or provider credentials.
Keep replay response semantics and structured rejection codes explicit.

Start with tests/unit/mission_control/test_public_interfaces.py and
test_authentication.py; authenticated runtime proof is
tests/acceptance/mission_control/test_authenticated_scoped_runtime.py.
