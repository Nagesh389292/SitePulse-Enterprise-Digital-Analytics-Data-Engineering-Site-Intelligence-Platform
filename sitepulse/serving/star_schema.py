"""Analytical Serving Layer using DuckDB with Star Schema Dimensional Marts."""
from pathlib import Path
from typing import Optional, Dict, Any
import duckdb

from sitepulse.config import SERVING_DB_PATH


def get_serving_connection(db_path: Optional[Path] = None) -> duckdb.DuckDBPyConnection:
    """Connect to or create the DuckDB serving database."""
    target_path = str(db_path or SERVING_DB_PATH)
    conn = duckdb.connect(database=target_path)
    return conn


def init_serving_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize star-schema DDL and Quality Tables."""
    conn.execute("""
    -- 1. Dimensions
    CREATE TABLE IF NOT EXISTS dim_page (
        page_sk VARCHAR PRIMARY KEY,
        url VARCHAR NOT NULL,
        path VARCHAR NOT NULL,
        title VARCHAR,
        content_category VARCHAR
    );

    CREATE TABLE IF NOT EXISTS dim_device (
        device_sk VARCHAR PRIMARY KEY,
        device_type VARCHAR NOT NULL,
        os VARCHAR NOT NULL,
        browser VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_campaign (
        campaign_sk VARCHAR PRIMARY KEY,
        utm_source VARCHAR,
        utm_medium VARCHAR,
        utm_campaign VARCHAR
    );

    -- 2. Fact Tables
    CREATE TABLE IF NOT EXISTS fact_sessions (
        session_sk VARCHAR PRIMARY KEY,
        session_id VARCHAR NOT NULL UNIQUE,
        visitor_id VARCHAR NOT NULL,
        session_start TIMESTAMP NOT NULL,
        session_end TIMESTAMP NOT NULL,
        duration_seconds BIGINT,
        page_view_count BIGINT,
        has_cart INTEGER,
        has_checkout INTEGER,
        has_purchase INTEGER,
        total_revenue DOUBLE,
        device_sk VARCHAR,
        campaign_sk VARCHAR,
        session_date DATE
    );

    CREATE TABLE IF NOT EXISTS fact_conversions (
        conversion_sk VARCHAR PRIMARY KEY,
        conversion_event_id VARCHAR NOT NULL UNIQUE,
        session_sk VARCHAR NOT NULL,
        session_id VARCHAR NOT NULL,
        visitor_id VARCHAR NOT NULL,
        conversion_time TIMESTAMP NOT NULL,
        order_id VARCHAR,
        revenue DOUBLE,
        items_count INTEGER,
        currency VARCHAR,
        device_sk VARCHAR,
        campaign_sk VARCHAR,
        conversion_date DATE
    );

    -- 3. Operational Quality & Incident Tables
    CREATE TABLE IF NOT EXISTS quality_incidents (
        incident_id VARCHAR PRIMARY KEY,
        detected_at TIMESTAMP NOT NULL,
        severity VARCHAR NOT NULL, -- CRITICAL, DEGRADED, HEALTHY
        failure_type VARCHAR NOT NULL,
        metric_affected VARCHAR,
        description VARCHAR NOT NULL,
        details_json VARCHAR
    );

    CREATE TABLE IF NOT EXISTS quality_scores_daily (
        evaluation_date DATE PRIMARY KEY,
        overall_score DOUBLE NOT NULL,
        freshness_score DOUBLE NOT NULL,
        completeness_score DOUBLE NOT NULL,
        schema_score DOUBLE NOT NULL,
        volume_score DOUBLE NOT NULL,
        referential_score DOUBLE NOT NULL,
        status VARCHAR NOT NULL
    );

    CREATE TABLE IF NOT EXISTS business_metric_anomalies (
        anomaly_id VARCHAR PRIMARY KEY,
        detected_at TIMESTAMP NOT NULL,
        anomaly_type VARCHAR NOT NULL,
        segment VARCHAR NOT NULL,
        severity VARCHAR NOT NULL,
        baseline_value DOUBLE,
        observed_value DOUBLE,
        relative_change DOUBLE,
        root_cause_hint VARCHAR NOT NULL,
        details_json VARCHAR
    );
    """)


class ServingLayerManager:
    """Manages loading from Lakehouse Parquet outputs into DuckDB Serving Marts."""

    def __init__(self, db_path: Optional[Path] = None):
        self.conn = get_serving_connection(db_path)
        init_serving_schema(self.conn)

    def load_from_lake(
        self,
        bronze_events_path: Path,
        silver_sessions_path: Path,
        silver_conversions_path: Path
    ) -> Dict[str, int]:
        """
        Reproducible vectorized load from Iceberg/PySpark Parquet files into DuckDB star schema.
        Populates surrogate keys and dimensional relationships.
        """
        clean_bronze = str(bronze_events_path).replace("\\", "/") + "/*.parquet"
        clean_sessions = str(silver_sessions_path).replace("\\", "/") + "/*.parquet"
        clean_conversions = str(silver_conversions_path).replace("\\", "/") + "/*.parquet"

        # 1. Populate dim_page from Bronze events
        self.conn.execute(f"""
        INSERT INTO dim_page (page_sk, url, path, title, content_category)
        SELECT DISTINCT
            md5(coalesce(page.url, '') || coalesce(page.path, '')) AS page_sk,
            page.url,
            page.path,
            page.title,
            page.content_category
        FROM read_parquet('{clean_bronze}')
        WHERE page.url IS NOT NULL
        ON CONFLICT (page_sk) DO NOTHING;
        """)

        # 2. Populate dim_device from Bronze events
        self.conn.execute(f"""
        INSERT INTO dim_device (device_sk, device_type, os, browser)
        SELECT DISTINCT
            md5(coalesce(device.device_type, 'unknown') || coalesce(device.os, 'unknown') || coalesce(device.browser, 'unknown')) AS device_sk,
            coalesce(device.device_type, 'unknown') AS device_type,
            coalesce(device.os, 'unknown') AS os,
            coalesce(device.browser, 'unknown') AS browser
        FROM read_parquet('{clean_bronze}')
        ON CONFLICT (device_sk) DO NOTHING;
        """)

        # 3. Populate dim_campaign from Bronze events
        self.conn.execute(f"""
        INSERT INTO dim_campaign (campaign_sk, utm_source, utm_medium, utm_campaign)
        SELECT DISTINCT
            md5(coalesce(campaign.utm_source, 'direct') || coalesce(campaign.utm_medium, 'none') || coalesce(campaign.utm_campaign, 'none')) AS campaign_sk,
            coalesce(campaign.utm_source, 'direct') AS utm_source,
            coalesce(campaign.utm_medium, 'none') AS utm_medium,
            coalesce(campaign.utm_campaign, 'none') AS utm_campaign
        FROM read_parquet('{clean_bronze}')
        ON CONFLICT (campaign_sk) DO NOTHING;
        """)

        # 4. Populate fact_sessions from Silver Sessions
        self.conn.execute(f"""
        INSERT INTO fact_sessions (
            session_sk, session_id, visitor_id, session_start, session_end,
            duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
            total_revenue, device_sk, campaign_sk, session_date
        )
        SELECT
            md5(s.session_id) AS session_sk,
            s.session_id,
            s.visitor_id,
            s.session_start,
            s.session_end,
            s.duration_seconds,
            s.page_view_count,
            s.has_cart,
            s.has_checkout,
            s.has_purchase,
            s.total_revenue,
            md5(coalesce(s.device_type, 'unknown') || coalesce(s.device_os, 'unknown') || coalesce(s.browser, 'unknown')) AS device_sk,
            md5(coalesce(s.utm_source, 'direct') || coalesce(s.utm_medium, 'none') || coalesce(s.utm_campaign, 'none')) AS campaign_sk,
            s.session_start::DATE AS session_date
        FROM read_parquet('{clean_sessions}') s
        ON CONFLICT (session_id) DO UPDATE SET
            has_purchase = EXCLUDED.has_purchase,
            total_revenue = EXCLUDED.total_revenue,
            page_view_count = EXCLUDED.page_view_count;
        """)

        # 5. Populate fact_conversions from Silver Conversions
        # Check if conversions exist before attempting insert
        conversions_count = self.conn.execute(f"""
        SELECT count(*) FROM glob('{clean_conversions}')
        """).fetchone()[0]

        if conversions_count > 0:
            self.conn.execute(f"""
            INSERT INTO fact_conversions (
                conversion_sk, conversion_event_id, session_sk, session_id,
                visitor_id, conversion_time, order_id, revenue, items_count,
                currency, device_sk, campaign_sk, conversion_date
            )
            SELECT
                md5(c.conversion_event_id) AS conversion_sk,
                c.conversion_event_id,
                md5(c.session_id) AS session_sk,
                c.session_id,
                c.visitor_id,
                c.conversion_time,
                c.order_id,
                c.revenue,
                c.items_count,
                c.currency,
                coalesce(s.device_sk, md5(coalesce(c.device_type, 'unknown') || coalesce(c.device_os, 'unknown') || 'Safari')) AS device_sk,
                coalesce(s.campaign_sk, md5('google' || 'cpc' || coalesce(c.campaign, 'none'))) AS campaign_sk,
                c.conversion_time::DATE AS conversion_date
            FROM read_parquet('{clean_conversions}') c
            LEFT JOIN fact_sessions s ON c.session_id = s.session_id
            ON CONFLICT (conversion_event_id) DO NOTHING;
            """)

        # Return table counts
        counts = {
            "dim_page": self.conn.execute("SELECT count(*) FROM dim_page").fetchone()[0],
            "dim_device": self.conn.execute("SELECT count(*) FROM dim_device").fetchone()[0],
            "dim_campaign": self.conn.execute("SELECT count(*) FROM dim_campaign").fetchone()[0],
            "fact_sessions": self.conn.execute("SELECT count(*) FROM fact_sessions").fetchone()[0],
            "fact_conversions": self.conn.execute("SELECT count(*) FROM fact_conversions").fetchone()[0],
        }
        return counts

    def close(self):
        self.conn.close()
