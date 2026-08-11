"""Stable graph registry. Importing this module performs no external I/O.

StageGraph and GoalDirected Agent Server macro graphs were deleted after Temporal
parity ownership moved to `app/temporal/workflows/{stagegraph,goal_directed}.py`.
"""

GRAPH_REGISTRY: dict[str, object] = {}

__all__ = ["GRAPH_REGISTRY"]
