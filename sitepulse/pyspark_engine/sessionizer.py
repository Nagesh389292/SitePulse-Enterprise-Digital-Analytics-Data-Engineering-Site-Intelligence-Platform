"""PySpark Event Sessionization, Window Deduplication, and Silver Mart Builder."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, lit, when, lag, sum as spark_sum, min as spark_min,
    max as spark_max, count as spark_count, concat, row_number,
    to_timestamp, unix_timestamp, coalesce
)

from sitepulse.config import SESSION_TIMEOUT_SECONDS
from sitepulse.generator.event_schema import CLICKSTREAM_SPARK_SCHEMA
from sitepulse.pyspark_engine.spark_session import get_spark_session
from sitepulse.pyspark_engine.quarantine import validate_and_quarantine


def _clean_path(p: Path) -> str:
    """Ensure path is free of spaces on Windows for Spark/Hadoop compatibility."""
    resolved = str(p.resolve())
    if "NAGESH REDDY" in resolved:
        return resolved.replace("NAGESH REDDY", "NAGESH~1")
    return resolved


@dataclass
class ProcessingResult:
    total_raw_records: int
    clean_records: int
    quarantined_records: int
    deduplicated_records: int
    total_sessions: int
    total_conversions: int
    quarantine_breakdown: Dict[str, int]


class EventSessionizer:
    """Processes raw clickstream streams, deduplicates, quarantines, and computes sessions."""

    def __init__(self, spark: Optional[SparkSession] = None):
        self.spark = spark or get_spark_session("SitePulse-Sessionizer")

    def process_raw_events(
        self,
        raw_events_path: Path,
        bronze_output_path: Path,
        silver_sessions_path: Path,
        silver_conversions_path: Path,
        quarantine_output_path: Path
    ) -> ProcessingResult:
        """End-to-end PySpark processing pipeline."""
        clean_raw_path = _clean_path(raw_events_path)
        clean_bronze_path = _clean_path(bronze_output_path)
        clean_silver_sessions_path = _clean_path(silver_sessions_path)
        clean_silver_conversions_path = _clean_path(silver_conversions_path)
        clean_quarantine_path = _clean_path(quarantine_output_path)

        # 1. Read raw JSON/JSONL using explicit schema (production best practice)
        raw_df = (
            self.spark.read
            .schema(CLICKSTREAM_SPARK_SCHEMA)
            .json(clean_raw_path)
        )
        total_raw = raw_df.count()

        if total_raw == 0:
            return ProcessingResult(0, 0, 0, 0, 0, 0, {})

        # Ensure event_timestamp is timestamp type
        raw_df = raw_df.withColumn("ts", to_timestamp(col("event_timestamp")))

        # 2. Validate & Quarantine
        clean_df, quarantined_df = validate_and_quarantine(raw_df)
        clean_count = clean_df.count()
        quarantined_count = quarantined_df.count()

        quarantine_breakdown = {}
        if quarantined_count > 0:
            reasons = (
                quarantined_df
                .groupBy("quarantine_reason")
                .count()
                .collect()
            )
            quarantine_breakdown = {row["quarantine_reason"]: row["count"] for row in reasons}
            # Write quarantined events
            quarantined_df.write.mode("append").parquet(clean_quarantine_path)

        if clean_count == 0:
            return ProcessingResult(total_raw, 0, quarantined_count, 0, 0, 0, quarantine_breakdown)

        # 3. Windowed Deduplication (Partition by event_id, order by ts asc)
        dedup_window = Window.partitionBy("event_id").orderBy(col("ts").asc())
        deduped_df = (
            clean_df
            .withColumn("row_num", row_number().over(dedup_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
        )
        deduped_count = deduped_df.count()

        # 4. Sessionization (Inactivity timeout window per visitor_id)
        visitor_window = Window.partitionBy("visitor_id").orderBy("ts")
        
        events_with_lag = deduped_df.withColumn(
            "prev_ts_epoch", 
            lag(unix_timestamp("ts")).over(visitor_window)
        )
        
        # New session if first event or gap > SESSION_TIMEOUT_SECONDS (1800s)
        events_with_diff = events_with_lag.withColumn(
            "is_new_session",
            when(
                col("prev_ts_epoch").isNull() | 
                (unix_timestamp("ts") - col("prev_ts_epoch") > SESSION_TIMEOUT_SECONDS),
                1
            ).otherwise(0)
        )

        # Running cumulative sum gives session index per visitor
        session_index_window = Window.partitionBy("visitor_id").orderBy("ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        events_with_session = (
            events_with_diff
            .withColumn("session_idx", spark_sum("is_new_session").over(session_index_window))
            .withColumn("session_id", concat(col("visitor_id"), lit("_s"), col("session_idx")))
            .drop("prev_ts_epoch", "is_new_session", "session_idx")
        )

        # 5. Write Bronze Lake (enriched deduplicated clean events)
        bronze_output_path.parent.mkdir(parents=True, exist_ok=True)
        events_with_session.write.mode("overwrite").parquet(clean_bronze_path)

        # 6. Aggregate to Silver Sessions Mart
        sessions_df = (
            events_with_session
            .groupBy("session_id", "visitor_id")
            .agg(
                spark_min("ts").alias("session_start"),
                spark_max("ts").alias("session_end"),
                (unix_timestamp(spark_max("ts")) - unix_timestamp(spark_min("ts"))).alias("duration_seconds"),
                spark_count(when(col("event_name") == "page_view", 1)).alias("page_view_count"),
                spark_max(when(col("event_name") == "add_to_cart", 1).otherwise(0)).alias("has_cart"),
                spark_max(when(col("event_name") == "checkout_start", 1).otherwise(0)).alias("has_checkout"),
                spark_max(when(col("event_name") == "purchase_completed", 1).otherwise(0)).alias("has_purchase"),
                spark_sum(coalesce(col("ecommerce.revenue"), lit(0.0))).alias("total_revenue"),
                spark_min("device.device_type").alias("device_type"),
                spark_min("device.os").alias("device_os"),
                spark_min("device.browser").alias("browser"),
                spark_min("campaign.utm_source").alias("utm_source"),
                spark_min("campaign.utm_medium").alias("utm_medium"),
                spark_min("campaign.utm_campaign").alias("utm_campaign")
            )
        )
        total_sessions = sessions_df.count()
        silver_sessions_path.parent.mkdir(parents=True, exist_ok=True)
        sessions_df.write.mode("overwrite").parquet(clean_silver_sessions_path)

        # 7. Aggregate to Silver Conversions Mart
        conversions_df = (
            events_with_session
            .filter(col("event_name") == "purchase_completed")
            .select(
                col("event_id").alias("conversion_event_id"),
                col("session_id"),
                col("visitor_id"),
                col("ts").alias("conversion_time"),
                col("ecommerce.order_id").alias("order_id"),
                coalesce(col("ecommerce.revenue"), lit(0.0)).alias("revenue"),
                coalesce(col("ecommerce.items_count"), lit(1)).alias("items_count"),
                coalesce(col("ecommerce.currency"), lit("USD")).alias("currency"),
                col("device.device_type").alias("device_type"),
                col("device.os").alias("device_os"),
                col("campaign.utm_campaign").alias("campaign")
            )
        )
        total_conversions = conversions_df.count()
        silver_conversions_path.parent.mkdir(parents=True, exist_ok=True)
        conversions_df.write.mode("overwrite").parquet(clean_silver_conversions_path)

        return ProcessingResult(
            total_raw_records=total_raw,
            clean_records=clean_count,
            quarantined_records=quarantined_count,
            deduplicated_records=deduped_count,
            total_sessions=total_sessions,
            total_conversions=total_conversions,
            quarantine_breakdown=quarantine_breakdown
        )
