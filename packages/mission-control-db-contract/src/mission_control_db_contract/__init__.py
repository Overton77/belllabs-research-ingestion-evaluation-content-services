"""Common Mission Control database component tooling (``mission-db``).

Administrative deployment tooling only. The Mission Control runtime never imports
this package; it owns no application/domain schema and contains no app branching.
"""

from .errors import ContractError

COMPONENT = "mission_control"
SOURCE_IDENTITY = "mission-control-db-contract"
INSTALLER_PROTOCOL = "mission-control-sql/v2"
OWNED_SCHEMAS = ("mission_control", "mission_control_search")
RUNTIME_SCHEMA = "mission_control_runtime"
SUPPORTED_APPS = ("biotech", "ai-engineer")

__all__ = [
    "COMPONENT",
    "ContractError",
    "INSTALLER_PROTOCOL",
    "OWNED_SCHEMAS",
    "RUNTIME_SCHEMA",
    "SOURCE_IDENTITY",
    "SUPPORTED_APPS",
]
