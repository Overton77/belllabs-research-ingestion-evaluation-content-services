# Temporal execution mechanics

workflows/mission_run.py implements mc.mission_run.v1 using the governed root;
workflows/stagegraph.py and goal_directed.py reconcile family decisions;
workflows/operation.py implements mc.operation.v1 bounded operation execution.
Registration, activities and task queues must remain mutually consistent.

Workflow code must be deterministic. Activities call application services.
Validate family scope/run/ERC/type/epoch against the root before dispatch.
Scoped child IDs cannot collide across installations, applications or tenants.
Linked independent roots preserve admitted parent lineage.

Submission replay verifies actual first-history input before issuing a receipt.
Check tests/unit/mission_control/test_temporal_identities.py and real replay in
tests/acceptance/mission_control. Do not certify historic captures from new-run tests.
