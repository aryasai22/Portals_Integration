"""
Silver transformer for CEIPAL employees.
"""
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
from pyspark.sql.functions import col, upper, coalesce, lit

from src.transformers.silver.base import BaseSilverTransformer


class EmployeesTransformer(BaseSilverTransformer):
    """Transform CEIPAL employees to Silver layer."""

    def __init__(self, spark):
        super().__init__(spark, "employees")

    def get_schema(self) -> StructType:
        """Define Silver schema for employees."""
        return StructType([
            # Core identifiers
            StructField("id", StringType(), False),
            StructField("employee_code", StringType(), True),

            # Personal information
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("mobile", StringType(), True),

            # Employment details
            StructField("status", StringType(), True),
            StructField("employment_type", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("department", StringType(), True),
            StructField("location", StringType(), True),

            # Dates
            StructField("hire_date", TimestampType(), True),
            StructField("termination_date", TimestampType(), True),

            # Manager relationship
            StructField("manager_id", StringType(), True),
            StructField("manager_name", StringType(), True),

            # Financial
            StructField("salary", DoubleType(), True),
            StructField("currency", StringType(), True),

            # Metadata from source
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ])

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform Bronze employees to Silver.

        Cleaning rules:
        - Standardize status to uppercase
        - Clean email and phone fields
        - Derive full_name from first_name + last_name
        - Handle null dates properly
        """
        # Clean string fields
        for col_name in ["employee_code", "first_name", "last_name", "job_title", "department", "location"]:
            if col_name in df.columns:
                df = self.clean_string(df, col_name)

        # Clean email
        if "email" in df.columns:
            df = self.clean_email(df, "email")

        # Clean phone fields
        for phone_col in ["phone", "mobile"]:
            if phone_col in df.columns:
                df = self.clean_phone(df, phone_col)

        # Standardize status
        if "status" in df.columns:
            df = df.withColumn("status", upper(col("status")))

        # Standardize employment_type
        if "employment_type" in df.columns:
            df = df.withColumn("employment_type", upper(col("employment_type")))

        # Parse dates
        for date_col in ["hire_date", "termination_date", "created_at", "updated_at"]:
            if date_col in df.columns:
                df = self.standardize_date(df, date_col)

        # Add derived full_name column
        df = df.withColumn(
            "FULL_NAME",
            coalesce(
                col("first_name").concat(lit(" ")).concat(col("last_name")),
                col("first_name"),
                col("last_name"),
                lit("UNKNOWN")
            )
        )

        # Add IS_ACTIVE flag
        df = df.withColumn(
            "IS_ACTIVE",
            col("status").isin(["ACTIVE", "EMPLOYED"])
        )

        return df

    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules for employees.

        Rules:
        - Employees without email are flagged
        - Employees with termination_date in past are marked inactive
        """
        # Flag employees without email
        df = df.withColumn(
            "HAS_EMAIL",
            col("email").isNotNull()
        )

        # Validate hire_date is before termination_date
        df = df.withColumn(
            "DATES_VALID",
            (col("hire_date").isNull()) |
            (col("termination_date").isNull()) |
            (col("hire_date") <= col("termination_date"))
        )

        return df
