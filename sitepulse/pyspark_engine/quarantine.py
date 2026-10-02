"""Data Quality Quarantine and Validation rules for PySpark clickstream events."""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, current_timestamp, to_timestamp
)
from sitepulse.generator.event_schema import VALID_EVENT_NAMES


def validate_and_quarantine(events_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Validates incoming raw clickstream events against the contract.
    Returns (clean_events_df, quarantined_events_df).
    """
    # 1. Check required fields
    has_event_id = col("event_id").isNotNull()
    has_visitor_id = col("visitor_id").isNotNull()
    has_timestamp = col("event_timestamp").isNotNull()
    has_device = col("device").isNotNull() & col("device.device_type").isNotNull()
    has_page = col("page").isNotNull() & col("page.url").isNotNull()
    
    # 2. Check event taxonomy
    is_valid_event = col("event_name").isin(list(VALID_EVENT_NAMES))
    
    # 3. Check ecommerce constraints
    valid_revenue = (
        col("ecommerce.revenue").isNull() | 
        ((col("ecommerce.revenue") >= 0.0) & (col("ecommerce.revenue") < 50000.0))
    )

    # Compile quarantine reason
    quarantine_cond = (
        when(~has_event_id, lit("ERR_MISSING_EVENT_ID"))
        .when(~has_visitor_id, lit("ERR_MISSING_VISITOR_ID"))
        .when(~has_device, lit("ERR_MISSING_DEVICE_PAYLOAD"))
        .when(~has_page, lit("ERR_MISSING_PAGE_PAYLOAD"))
        .when(~is_valid_event, lit("ERR_SCHEMA_DRIFT_UNKNOWN_EVENT"))
        .when(~valid_revenue, lit("ERR_INVALID_REVENUE_RANGE"))
        .otherwise(lit(None))
    )

    flagged_df = events_df.withColumn("quarantine_reason", quarantine_cond)

    quarantined_df = (
        flagged_df
        .filter(col("quarantine_reason").isNotNull())
        .withColumn("quarantined_at", current_timestamp())
    )

    clean_df = (
        flagged_df
        .filter(col("quarantine_reason").isNull())
        .drop("quarantine_reason")
    )

    return clean_df, quarantined_df
