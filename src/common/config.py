"""Config management with Pydantic"""
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
from typing import Optional
from enum import Enum


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # CEIPAL API
    ceipal_base_url: str = Field(default="https://api.ceipal.com")
    ceipal_email: str
    ceipal_password: str
    ceipal_api_key: str
    ceipal_retries: int = Field(default=6, ge=1, le=20)
    ceipal_backoff_factor: float = Field(default=1.2, ge=0.1, le=5.0)
    ceipal_detail_rps: float = Field(default=0.8, gt=0, le=100)
    ceipal_detail_workers: int = Field(default=1, ge=1, le=20)

    # Date range for time-based APIs (expenses, invoices)
    ceipal_from_date: str
    ceipal_to_date: str

    # Snowflake (optional)
    snowflake_account: Optional[str] = None
    snowflake_user: Optional[str] = None
    snowflake_password: Optional[str] = None
    snowflake_warehouse: str = Field(default="COMPUTE_WH")
    snowflake_database: str = Field(default="PORTALS_DW")
    snowflake_role: str = Field(default="SYSADMIN")

    # Pipeline settings
    bronze_batch_size: int = Field(default=500, ge=1, le=10000)
    silver_lookback_days: int = Field(default=14, ge=1, le=365)
    silver_merge_enabled: bool = Field(default=True)
    gold_rebuild_dims: bool = Field(default=False)
    gold_rebuild_facts: bool = Field(default=True)

    # Spark
    spark_driver_memory: str = Field(default="4g")
    spark_executor_memory: str = Field(default="4g")
    spark_shuffle_partitions: int = Field(default=200, ge=1, le=2000)

    # Logging
    log_level: LogLevel = Field(default=LogLevel.INFO)
    log_to_file: bool = Field(default=True)
    log_dir: Path = Field(default=Path("output/logs"))

    # Checkpointing
    checkpoint_enabled: bool = Field(default=True)
    checkpoint_dir: Path = Field(default=Path("output/checkpoints"))

    @field_validator('ceipal_from_date', 'ceipal_to_date')
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """Validate date format YYYY-MM-DD"""
        from datetime import datetime
        try:
            datetime.strptime(v, '%Y-%m-%d')
            return v
        except ValueError:
            raise ValueError(f"Date must be in YYYY-MM-DD format, got: {v}")

    @field_validator('spark_driver_memory', 'spark_executor_memory')
    @classmethod
    def validate_memory_string(cls, v: str) -> str:
        """Validate memory string format (e.g., 4g, 512m)"""
        import re
        if not re.match(r'^\d+[gmGM]$', v):
            raise ValueError(f"Memory must be in format like '4g' or '512m', got: {v}")
        return v

    @property
    def is_snowflake_configured(self) -> bool:
        """Check if Snowflake credentials are configured"""
        return all([
            self.snowflake_account,
            self.snowflake_user,
            self.snowflake_password
        ])


# Initialize settings with validation
try:
    settings = Settings()
except Exception as e:
    print(f"[ERROR] Configuration validation failed: {e}")
    print("[INFO] Please check your .env file and ensure all required variables are set")
    raise


# ============================================================================
# Backward Compatibility Layer
# These module-level variables maintain compatibility with existing code
# ============================================================================

# CEIPAL API
CEIPAL_BASE_URL = settings.ceipal_base_url
CEIPAL_EMAIL = settings.ceipal_email
CEIPAL_PASSWORD = settings.ceipal_password
CEIPAL_API_KEY = settings.ceipal_api_key
CEIPAL_RETRIES = settings.ceipal_retries
CEIPAL_BACKOFF_FACTOR = settings.ceipal_backoff_factor
CEIPAL_DETAIL_RPS = settings.ceipal_detail_rps
CEIPAL_DETAIL_WORKERS = settings.ceipal_detail_workers

# Date range
CEIPAL_FROM_DATE = settings.ceipal_from_date
CEIPAL_TO_DATE = settings.ceipal_to_date

# Snowflake
SNOWFLAKE_ACCOUNT = settings.snowflake_account
SNOWFLAKE_USER = settings.snowflake_user
SNOWFLAKE_PASSWORD = settings.snowflake_password
SNOWFLAKE_WAREHOUSE = settings.snowflake_warehouse
SNOWFLAKE_DATABASE = settings.snowflake_database
SNOWFLAKE_ROLE = settings.snowflake_role

# Pipeline settings
BRONZE_BATCH_SIZE = settings.bronze_batch_size
SILVER_LOOKBACK_DAYS = settings.silver_lookback_days
SILVER_MERGE_ENABLED = settings.silver_merge_enabled
GOLD_REBUILD_DIMS = settings.gold_rebuild_dims
GOLD_REBUILD_FACTS = settings.gold_rebuild_facts

# Spark
SPARK_DRIVER_MEMORY = settings.spark_driver_memory
SPARK_EXECUTOR_MEMORY = settings.spark_executor_memory
SPARK_SHUFFLE_PARTITIONS = settings.spark_shuffle_partitions

# Logging
LOG_LEVEL = settings.log_level.value  # Convert enum to string
LOG_TO_FILE = settings.log_to_file
LOG_DIR = settings.log_dir

# Checkpointing
CHECKPOINT_ENABLED = settings.checkpoint_enabled
CHECKPOINT_DIR = settings.checkpoint_dir


# ============================================================================
# Validation Functions (backward compatible)
# ============================================================================

def validate_config() -> None:
    """
    Validate required CEIPAL configuration.
    Raises ValueError if required settings are missing.
    """
    # Pydantic already validates at initialization, but keep this for explicit validation calls
    required_fields = [
        'ceipal_email',
        'ceipal_password',
        'ceipal_api_key',
        'ceipal_from_date',
        'ceipal_to_date'
    ]

    missing = []
    for field in required_fields:
        value = getattr(settings, field, None)
        if not value:
            missing.append(field.upper())

    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")


def validate_snowflake_config() -> None:
    """
    Validate Snowflake configuration.
    Raises ValueError if required Snowflake settings are missing.
    """
    required_fields = [
        ('snowflake_account', 'SNOWFLAKE_ACCOUNT'),
        ('snowflake_user', 'SNOWFLAKE_USER'),
        ('snowflake_password', 'SNOWFLAKE_PASSWORD')
    ]

    missing = []
    for field, display_name in required_fields:
        value = getattr(settings, field, None)
        if not value:
            missing.append(display_name)

    if missing:
        raise ValueError(f"Missing Snowflake credentials: {', '.join(missing)}")


def is_snowflake_configured() -> bool:
    """Check if Snowflake credentials are configured"""
    return settings.is_snowflake_configured
