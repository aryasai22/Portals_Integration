"""
CEIPAL API resource configuration with type safety using Pydantic
"""
from typing import Dict, List, Optional
from pydantic import BaseModel, Field

from src.common.config import settings
from src.common.enums import ResourceType


class PaginationConfig(BaseModel):
    """Pagination configuration for API endpoints"""
    page_size: int = Field(ge=1, le=1000, description="Number of records per page")
    max_pages: Optional[int] = Field(default=None, ge=1, description="Maximum pages to fetch")

    class Config:
        frozen = True  # Immutable config


class ResourceConfig(BaseModel):
    """
    Configuration for a CEIPAL API resource.

    Attributes:
        list_endpoint: API endpoint for listing records
        detail_endpoint: API endpoint for fetching individual record details
        fetch_details: Whether to fetch detail records (can be overridden at runtime)
        primary_key: Field name for primary key (typically 'id')
        merge_keys: Fields to use when merging list and detail data
        pagination: Pagination configuration
        required_params: Required query parameters for this resource
    """
    list_endpoint: str
    detail_endpoint: str
    fetch_details: bool = Field(default=False, description="Fetch detail endpoint by default")
    primary_key: str = Field(default="id")
    merge_keys: List[str] = Field(default_factory=lambda: ["id"])
    pagination: PaginationConfig
    required_params: Dict[str, str] = Field(default_factory=dict)

    class Config:
        frozen = True  # Immutable config


# Resource configurations using type-safe Pydantic models
RESOURCE_CONFIGS: Dict[ResourceType, ResourceConfig] = {
    ResourceType.EMPLOYEES: ResourceConfig(
        list_endpoint="/wf/v1/getEmployees",
        detail_endpoint="/wf/v1/getEmployeesDetail/{id}",
        fetch_details=False,  # Detail endpoint provides NO new fields
        pagination=PaginationConfig(page_size=100)
    ),
    ResourceType.PROJECTS: ResourceConfig(
        list_endpoint="/wf/v1/getProjects",
        detail_endpoint="/wf/v1/getProjectDetails",
        fetch_details=False,  # Detail endpoint returns 404
        pagination=PaginationConfig(page_size=100)
    ),
    ResourceType.PLACEMENTS: ResourceConfig(
        list_endpoint="/wf/v1/getPlacements",
        detail_endpoint="/wf/v1/getPlacementsDetails",
        fetch_details=False,  # Detail endpoint provides NO new fields
        pagination=PaginationConfig(page_size=100)
    ),
    ResourceType.CLIENTS: ResourceConfig(
        list_endpoint="/wf/v1/getClients",
        detail_endpoint="/wf/v1/getClientDetails",
        fetch_details=False,  # Detail endpoint returns 404
        pagination=PaginationConfig(page_size=100)
    ),
    ResourceType.VENDORS: ResourceConfig(
        list_endpoint="/wf/v1/getVendors",
        detail_endpoint="/wf/v1/getVendorDetails",
        fetch_details=False,  # Fetch list only (faster)
        pagination=PaginationConfig(page_size=100)
    ),
    ResourceType.EXPENSES: ResourceConfig(
        list_endpoint="/wf/v1/getExpenses",
        detail_endpoint="/wf/v1/getExpenseDetails",
        fetch_details=False,  # Detail endpoint returns 404
        pagination=PaginationConfig(page_size=50),
        required_params={
            "fromdate": settings.ceipal_from_date,
            "todate": settings.ceipal_to_date
        }
    ),
    ResourceType.INVOICES: ResourceConfig(
        list_endpoint="/wf/v1/getInvoices",
        detail_endpoint="/wf/v1/getInvoiceDetails",
        fetch_details=False,  # Detail endpoint returns 404
        pagination=PaginationConfig(page_size=50),
        required_params={
            "fromdate": settings.ceipal_from_date,
            "todate": settings.ceipal_to_date
        }
    ),
}


# Legacy dictionary format for backward compatibility
RESOURCE_CATALOG = {
    resource.value: {
        "list_endpoint": config.list_endpoint,
        "detail_endpoint": config.detail_endpoint,
        "fetch_details": config.fetch_details,
        "primary_key": config.primary_key,
        "merge_keys": config.merge_keys,
        "pagination": {
            "page_size": config.pagination.page_size,
            "max_pages": config.pagination.max_pages
        },
        "required_params": config.required_params
    }
    for resource, config in RESOURCE_CONFIGS.items()
}


def get_resource_config(resource_name: str) -> Dict:
    """
    Get configuration for a resource (backward compatible).

    Args:
        resource_name: Resource name as string

    Returns:
        Resource configuration dictionary

    Raises:
        ValueError: If resource name is unknown
    """
    # Try to convert to ResourceType enum
    try:
        resource = ResourceType(resource_name.lower())
        # Return Pydantic model for new code
        return RESOURCE_CONFIGS[resource]
    except ValueError:
        # For backward compatibility, also check legacy catalog
        if resource_name in RESOURCE_CATALOG:
            return RESOURCE_CATALOG[resource_name]

        available = ", ".join(ResourceType.list_all())
        raise ValueError(f"Unknown resource '{resource_name}'. Available: {available}")


def get_typed_config(resource: ResourceType) -> ResourceConfig:
    """
    Get typed Pydantic configuration for a resource.

    Args:
        resource: Resource type enum

    Returns:
        ResourceConfig Pydantic model

    Raises:
        KeyError: If resource not found in configs
    """
    return RESOURCE_CONFIGS[resource]


def list_resources() -> List[str]:
    """
    List all available resource names.

    Returns:
        List of resource name strings
    """
    return ResourceType.list_all()
