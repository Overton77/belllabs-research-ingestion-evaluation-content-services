"""MP-09 / OVE-72: Cursor local/cloud parity and qualification gaps (FIXTURE suites).

Every provider here is a labelled fixture (`tests/fixtures/cursor_cloud.FakeCloudApi` behind
`httpx.MockTransport`, `tests/fixtures/cursor_local.ReplayBridgeLauncher`); nothing proves live
Cursor behaviour and nothing flips `qualified`.
"""
