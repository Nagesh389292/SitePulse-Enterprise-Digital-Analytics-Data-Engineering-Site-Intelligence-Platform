"""Semantic Metric Catalog.

Provides a unified business semantic layer defining analytical metrics,
source tables, dimensional grains, SQL expressions, and business documentation.
Enforces consistent analytical governance across dashboards, SQL generators, and AI agents.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any


@dataclass
class DimensionDefinition:
    key: str
    name: str
    table: str
    column: str
    description: str


@dataclass
class MetricDefinition:
    key: str
    name: str
    category: str  # conversion, revenue, engagement, funnel, quality
    formula: str
    sql_expression: str
    required_tables: List[str]
    supported_dimensions: List[str]
    business_definition: str
    unit: str  # percentage, currency_usd, count, ratio, score


@dataclass
class TableMetadata:
    name: str
    table_type: str  # fact, dimension, operational
    primary_key: str
    foreign_keys: Dict[str, str]  # fk_column -> target_table.pk
    columns: Dict[str, str]       # column_name -> data_type
    description: str


class SemanticCatalog:
    """Enterprise semantic catalog defining metrics and dimensional relationships."""

    def __init__(self):
        self._tables: Dict[str, TableMetadata] = {}
        self._dimensions: Dict[str, DimensionDefinition] = {}
        self._metrics: Dict[str, MetricDefinition] = {}
        self._bootstrap_catalog()

    def _bootstrap_catalog(self) -> None:
        # 1. Register Tables
        self._tables["fact_sessions"] = TableMetadata(
            name="fact_sessions",
            table_type="fact",
            primary_key="session_sk",
            foreign_keys={"device_sk": "dim_device.device_sk", "campaign_sk": "dim_campaign.campaign_sk"},
            columns={
                "session_sk": "VARCHAR",
                "session_id": "VARCHAR",
                "visitor_id": "VARCHAR",
                "session_start": "TIMESTAMP",
                "session_end": "TIMESTAMP",
                "duration_seconds": "BIGINT",
                "page_view_count": "BIGINT",
                "has_cart": "INTEGER",
                "has_checkout": "INTEGER",
                "has_purchase": "INTEGER",
                "total_revenue": "DOUBLE",
                "device_sk": "VARCHAR",
                "campaign_sk": "VARCHAR",
                "session_date": "DATE"
            },
            description="Aggregated user session fact table at the 30-minute inactivity boundary."
        )

        self._tables["fact_conversions"] = TableMetadata(
            name="fact_conversions",
            table_type="fact",
            primary_key="conversion_sk",
            foreign_keys={
                "session_sk": "fact_sessions.session_sk",
                "device_sk": "dim_device.device_sk",
                "campaign_sk": "dim_campaign.campaign_sk"
            },
            columns={
                "conversion_sk": "VARCHAR",
                "conversion_event_id": "VARCHAR",
                "session_sk": "VARCHAR",
                "session_id": "VARCHAR",
                "visitor_id": "VARCHAR",
                "conversion_time": "TIMESTAMP",
                "order_id": "VARCHAR",
                "revenue": "DOUBLE",
                "items_count": "INTEGER",
                "currency": "VARCHAR",
                "device_sk": "VARCHAR",
                "campaign_sk": "VARCHAR",
                "conversion_date": "DATE"
            },
            description="Individual verified e-commerce purchase transactions."
        )

        self._tables["dim_device"] = TableMetadata(
            name="dim_device",
            table_type="dimension",
            primary_key="device_sk",
            foreign_keys={},
            columns={
                "device_sk": "VARCHAR",
                "device_type": "VARCHAR",
                "os": "VARCHAR",
                "browser": "VARCHAR"
            },
            description="Device environment attributes (e.g., mobile/desktop, iOS/Android, Chrome/Safari)."
        )

        self._tables["dim_campaign"] = TableMetadata(
            name="dim_campaign",
            table_type="dimension",
            primary_key="campaign_sk",
            foreign_keys={},
            columns={
                "campaign_sk": "VARCHAR",
                "utm_source": "VARCHAR",
                "utm_medium": "VARCHAR",
                "utm_campaign": "VARCHAR"
            },
            description="Marketing attribution dimensions (UTM source, medium, campaign)."
        )

        self._tables["dim_page"] = TableMetadata(
            name="dim_page",
            table_type="dimension",
            primary_key="page_sk",
            foreign_keys={},
            columns={
                "page_sk": "VARCHAR",
                "url": "VARCHAR",
                "path": "VARCHAR",
                "title": "VARCHAR",
                "content_category": "VARCHAR"
            },
            description="Site pages and content taxonomy."
        )

        self._tables["quality_incidents"] = TableMetadata(
            name="quality_incidents",
            table_type="operational",
            primary_key="incident_id",
            foreign_keys={},
            columns={
                "incident_id": "VARCHAR",
                "detected_at": "TIMESTAMP",
                "severity": "VARCHAR",
                "failure_type": "VARCHAR",
                "metric_affected": "VARCHAR",
                "description": "VARCHAR",
                "details_json": "VARCHAR"
            },
            description="Operational telemetry and schema contract incidents logged by PySpark quarantine and Quality Engine."
        )

        self._tables["business_metric_anomalies"] = TableMetadata(
            name="business_metric_anomalies",
            table_type="operational",
            primary_key="anomaly_id",
            foreign_keys={},
            columns={
                "anomaly_id": "VARCHAR",
                "detected_at": "TIMESTAMP",
                "anomaly_type": "VARCHAR",
                "segment": "VARCHAR",
                "severity": "VARCHAR",
                "baseline_value": "DOUBLE",
                "observed_value": "DOUBLE",
                "relative_change": "DOUBLE",
                "root_cause_hint": "VARCHAR",
                "details_json": "VARCHAR"
            },
            description="Behavioral and segment-level metric anomalies (mobile drop, revenue collapse, funnel break)."
        )

        # 2. Register Dimensions
        self._dimensions["device_type"] = DimensionDefinition(
            key="device_type",
            name="Device Category",
            table="dim_device",
            column="device_type",
            description="Device form factor (mobile, desktop, tablet)."
        )
        self._dimensions["os"] = DimensionDefinition(
            key="os",
            name="Operating System",
            table="dim_device",
            column="os",
            description="Client OS (iOS, Android, Windows, macOS)."
        )
        self._dimensions["browser"] = DimensionDefinition(
            key="browser",
            name="Web Browser",
            table="dim_device",
            column="browser",
            description="Client browser engine (Safari, Chrome, Firefox, Edge)."
        )
        self._dimensions["utm_source"] = DimensionDefinition(
            key="utm_source",
            name="Campaign Source",
            table="dim_campaign",
            column="utm_source",
            description="Marketing referral source (google, newsletter, direct, meta)."
        )
        self._dimensions["session_date"] = DimensionDefinition(
            key="session_date",
            name="Session Date",
            table="fact_sessions",
            column="session_date",
            description="Calendar date of the session start."
        )

        # 3. Register Core Metrics
        self._metrics["conversion_rate"] = MetricDefinition(
            key="conversion_rate",
            name="Conversion Rate",
            category="conversion",
            formula="count(distinct order_id) / count(distinct session_id)",
            sql_expression="round(count(distinct c.order_id)::DOUBLE / nullif(count(distinct s.session_id), 0) * 100.0, 2)",
            required_tables=["fact_sessions", "fact_conversions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Percentage of site user sessions that resulted in a completed e-commerce purchase transaction.",
            unit="percentage"
        )

        self._metrics["total_sessions"] = MetricDefinition(
            key="total_sessions",
            name="Total Sessions",
            category="engagement",
            formula="count(distinct session_id)",
            sql_expression="count(distinct s.session_id)",
            required_tables=["fact_sessions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Total count of visitor sessions defined by a 30-minute inactivity window.",
            unit="count"
        )

        self._metrics["total_revenue"] = MetricDefinition(
            key="total_revenue",
            name="Total Revenue",
            category="revenue",
            formula="sum(revenue)",
            sql_expression="round(coalesce(sum(c.revenue), 0.0), 2)",
            required_tables=["fact_conversions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Sum of all transaction revenue in USD from verified orders.",
            unit="currency_usd"
        )

        self._metrics["revenue_per_session"] = MetricDefinition(
            key="revenue_per_session",
            name="Revenue per Session (RPS)",
            category="revenue",
            formula="sum(revenue) / count(distinct session_id)",
            sql_expression="round(coalesce(sum(c.revenue), 0.0) / nullif(count(distinct s.session_id), 0), 2)",
            required_tables=["fact_sessions", "fact_conversions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Monetary value generated per visit across the entire traffic base.",
            unit="currency_usd"
        )

        self._metrics["average_order_value"] = MetricDefinition(
            key="average_order_value",
            name="Average Order Value (AOV)",
            category="revenue",
            formula="sum(revenue) / count(distinct order_id)",
            sql_expression="round(coalesce(sum(c.revenue), 0.0) / nullif(count(distinct c.order_id), 0), 2)",
            required_tables=["fact_conversions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Average revenue generated per completed order transaction.",
            unit="currency_usd"
        )

        self._metrics["bounce_rate"] = MetricDefinition(
            key="bounce_rate",
            name="Bounce Rate",
            category="engagement",
            formula="count(session_id where page_view_count <= 1 and duration_seconds < 10) / count(session_id)",
            sql_expression="round(count(case when s.page_view_count <= 1 and s.duration_seconds < 10 then 1 end)::DOUBLE / nullif(count(s.session_id), 0) * 100.0, 2)",
            required_tables=["fact_sessions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Percentage of single-page sessions lasting under 10 seconds without downstream engagement.",
            unit="percentage"
        )

        self._metrics["cart_abandonment_rate"] = MetricDefinition(
            key="cart_abandonment_rate",
            name="Cart Abandonment Rate",
            category="funnel",
            formula="(sum(has_cart) - sum(has_purchase)) / sum(has_cart)",
            sql_expression="round((sum(s.has_cart) - sum(s.has_purchase))::DOUBLE / nullif(sum(s.has_cart), 0) * 100.0, 2)",
            required_tables=["fact_sessions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Percentage of shopping cart sessions that abandoned before completing order payment.",
            unit="percentage"
        )

        self._metrics["checkout_completion_rate"] = MetricDefinition(
            key="checkout_completion_rate",
            name="Checkout Completion Rate",
            category="funnel",
            formula="sum(has_purchase) / sum(has_checkout)",
            sql_expression="round(sum(s.has_purchase)::DOUBLE / nullif(sum(s.has_checkout), 0) * 100.0, 2)",
            required_tables=["fact_sessions"],
            supported_dimensions=["device_type", "os", "browser", "utm_source", "session_date"],
            business_definition="Conversion velocity from entering checkout step to final transaction confirmation.",
            unit="percentage"
        )

    def get_metric(self, key: str) -> Optional[MetricDefinition]:
        return self._metrics.get(key)

    def list_metrics(self) -> List[MetricDefinition]:
        return list(self._metrics.values())

    def get_table(self, name: str) -> Optional[TableMetadata]:
        return self._tables.get(name)

    def list_tables(self) -> List[TableMetadata]:
        return list(self._tables.values())

    def get_dimension(self, key: str) -> Optional[DimensionDefinition]:
        return self._dimensions.get(key)

    def get_schema_context_markdown(self) -> str:
        """Generates structured documentation for AI agents and LLM prompts."""
        lines = [
            "# SitePulse Semantic Data Catalog & Governing Rules",
            "\n## Analytical Tables",
        ]
        for tbl in self._tables.values():
            cols = ", ".join([f"`{c}` ({t})" for c, t in tbl.columns.items()])
            fks = "; ".join([f"`{k}` -> `{v}`" for k, v in tbl.foreign_keys.items()]) if tbl.foreign_keys else "None"
            lines.append(f"- **{tbl.name}** ({tbl.table_type}): {tbl.description}")
            lines.append(f"  - Primary Key: `{tbl.primary_key}`")
            lines.append(f"  - Foreign Keys: {fks}")
            lines.append(f"  - Columns: {cols}")

        lines.append("\n## Governed Metrics")
        for m in self._metrics.values():
            lines.append(f"- **{m.name}** (`{m.key}`): {m.business_definition}")
            lines.append(f"  - Category: {m.category} | Unit: {m.unit}")
            lines.append(f"  - SQL Logic: `{m.sql_expression}`")
            lines.append(f"  - Tables: {', '.join(m.required_tables)}")
            lines.append(f"  - Valid Dimensions: {', '.join(m.supported_dimensions)}")

        lines.append("\n## Dimensional Joins Convention")
        lines.append("- Join `fact_sessions s` to `dim_device d` ON `s.device_sk = d.device_sk`")
        lines.append("- Join `fact_sessions s` to `dim_campaign c` ON `s.campaign_sk = c.campaign_sk`")
        lines.append("- Join `fact_sessions s` to `fact_conversions conv` ON `s.session_id = conv.session_id`")
        return "\n".join(lines)
