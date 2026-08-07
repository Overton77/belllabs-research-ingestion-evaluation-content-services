"""Compatibility version markers for Block C N / N+1 graphs and assemblies."""

from __future__ import annotations

from typing import Literal

COMPAT_VERSION_N = "block-c-qual-n"
COMPAT_VERSION_N1 = "block-c-qual-n1"

GRAPH_ID_N = "block_c_qualification"
GRAPH_ID_N1 = "block_c_qualification_n1"
GRAPH_ID_WAIT = "block_c_wait"

# Distinct qualification deployments (shared disposable Postgres, separate Redis).
AssemblyRole = Literal["n", "n1"]
ASSEMBLY_ROLE_N: AssemblyRole = "n"
ASSEMBLY_ROLE_N1: AssemblyRole = "n1"

ASSEMBLY_N = "block-c-qual-assembly-n"
ASSEMBLY_N1 = "block-c-qual-assembly-n1"

DEFAULT_ENDPOINT_N = "http://127.0.0.1:8133"
DEFAULT_ENDPOINT_N1 = "http://127.0.0.1:8134"

COMPOSE_PROJECT_N = "belllabs-block-c-qualification"
COMPOSE_PROJECT_N1 = "belllabs-block-c-qualification-n1"
