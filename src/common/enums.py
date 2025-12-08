"""
Common enums used throughout the application
"""
from enum import Enum


class PipelineStatus(str, Enum):
    """Pipeline execution status"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
    NO_DATA = "NO_DATA"


class ResourceType(str, Enum):
    """Available CEIPAL API resources"""
    EMPLOYEES = "employees"
    PROJECTS = "projects"
    PLACEMENTS = "placements"
    CLIENTS = "clients"
    VENDORS = "vendors"
    EXPENSES = "expenses"
    INVOICES = "invoices"

    @classmethod
    def list_all(cls) -> list[str]:
        """Get list of all resource names"""
        return [r.value for r in cls]

    @classmethod
    def from_string(cls, value: str) -> "ResourceType":
        """Convert string to ResourceType, case-insensitive"""
        value_lower = value.lower()
        for resource in cls:
            if resource.value == value_lower:
                return resource
        available = ", ".join(cls.list_all())
        raise ValueError(f"Unknown resource '{value}'. Available: {available}")


class OutputMode(str, Enum):
    """Data output modes"""
    JSON = "json"
    SNOWFLAKE = "snowflake"


class LoadMode(str, Enum):
    """Data load modes for Snowflake"""
    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"
