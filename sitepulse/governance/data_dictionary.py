"""Enterprise Data Governance Catalog & Data Dictionary.

Defines operational ownership, data stewardship, sensitivity classification (PII vs Non-PII),
refresh SLAs, quality guarantees, and metric lineage across analytical assets.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ColumnGovernance:
    name: str
    data_type: str
    business_description: str
    sensitivity: str  # PUBLIC, INTERNAL, CONFIDENTIAL_PII, RESTRICTED_FINANCIAL
    is_nullable: bool
    is_surrogate_key: bool
    quality_rules: List[str]


@dataclass
class TableGovernance:
    table_name: str
    domain: str
    table_type: str  # Star Schema Fact, Dimension, Operational Mart
    data_owner: str  # Executive accountable
    data_steward: str  # Operational data custodian
    source_system: str
    refresh_frequency: str
    refresh_sla_minutes: int
    quality_sla_completeness_pct: float
    retention_period: str
    documentation_url: str
    columns: List[ColumnGovernance] = field(default_factory=list)


@dataclass
class MetricGovernance:
    metric_key: str
    metric_name: str
    business_owner: str
    technical_custodian: str
    source_tables: List[str]
    refresh_sla_minutes: int
    minimum_health_threshold: float
    business_definition: str
    calculation_logic: str


class DataGovernanceCatalog:
    """Enterprise Data Governance and Dictionary registry."""

    def __init__(self):
        self._tables: Dict[str, TableGovernance] = {}
        self._metrics: Dict[str, MetricGovernance] = {}
        self._bootstrap_governance()

    def _bootstrap_governance(self) -> None:
        # 1. Fact Sessions Governance
        self._tables["fact_sessions"] = TableGovernance(
            table_name="fact_sessions",
            domain="Digital Analytics",
            table_type="Star Schema Fact",
            data_owner="Head of Content Ops & Site Quality (Dentsu)",
            data_steward="Lead Analytics Engineer",
            source_system="PySpark Sessionizer / Silver Parquet Lakehouse",
            refresh_frequency="Hourly Micro-Batch",
            refresh_sla_minutes=30,
            quality_sla_completeness_pct=99.0,
            retention_period="730 Days (2 Years)",
            documentation_url="https://wiki.dentsu.internal/analytics/fact_sessions",
            columns=[
                ColumnGovernance("session_sk", "VARCHAR", "Deterministic MD5 surrogate hash of session_id", "INTERNAL", False, True, ["Must be non-null", "Unique key"]),
                ColumnGovernance("session_id", "VARCHAR", "Logical session identifier bounded by 30-min inactivity", "INTERNAL", False, False, ["Regex UUID/Hex format"]),
                ColumnGovernance("visitor_id", "VARCHAR", "First-party pseudonymous client identity token", "CONFIDENTIAL_PII", False, False, ["Must never contain raw email or IP"]),
                ColumnGovernance("duration_seconds", "BIGINT", "Total elapsed session duration in seconds", "INTERNAL", True, False, ["Must be >= 0", "Flag if > 86400"]),
                ColumnGovernance("page_view_count", "BIGINT", "Total pageviews recorded in session", "INTERNAL", False, False, ["Must be >= 1"]),
                ColumnGovernance("total_revenue", "DOUBLE", "Aggregated session transaction value in USD", "RESTRICTED_FINANCIAL", False, False, ["Must be >= 0.0", "Quarantine if > 50,000.0"])
            ]
        )

        # 2. Fact Conversions Governance
        self._tables["fact_conversions"] = TableGovernance(
            table_name="fact_conversions",
            domain="E-Commerce & Commercial Analytics",
            table_type="Star Schema Fact",
            data_owner="VP of Commercial Operations",
            data_steward="Senior Data Engineer (Content Ops)",
            source_system="PySpark E-Commerce Pipeline / Lakehouse Silver",
            refresh_frequency="Near Real-Time (15 mins)",
            refresh_sla_minutes=15,
            quality_sla_completeness_pct=99.9,
            retention_period="2555 Days (7 Years Financial)",
            documentation_url="https://wiki.dentsu.internal/analytics/fact_conversions",
            columns=[
                ColumnGovernance("conversion_sk", "VARCHAR", "Surrogate hash of conversion event", "INTERNAL", False, True, ["Unique constraint"]),
                ColumnGovernance("order_id", "VARCHAR", "Unique commercial transaction receipt number", "RESTRICTED_FINANCIAL", False, False, ["Alphanumeric order contract"]),
                ColumnGovernance("revenue", "DOUBLE", "Gross transaction monetary amount in USD", "RESTRICTED_FINANCIAL", False, False, ["Range 0.01 to 50,000.00 USD"]),
                ColumnGovernance("currency", "VARCHAR", "ISO 4217 Currency Code (default USD)", "INTERNAL", False, False, ["ISO 4217 standard"])
            ]
        )

        # 3. Dim Device Governance
        self._tables["dim_device"] = TableGovernance(
            table_name="dim_device",
            domain="Telemetry & Client Infrastructure",
            table_type="Dimension",
            data_owner="Lead Web Performance Architect",
            data_steward="Site Quality Engineer",
            source_system="Client User-Agent Parser / Client Hints",
            refresh_frequency="Daily SCD Type 1",
            refresh_sla_minutes=60,
            quality_sla_completeness_pct=95.0,
            retention_period="Permanent",
            documentation_url="https://wiki.dentsu.internal/analytics/dim_device",
            columns=[
                ColumnGovernance("device_sk", "VARCHAR", "MD5 surrogate key (type + os + browser)", "INTERNAL", False, True, ["Unique key"]),
                ColumnGovernance("device_type", "VARCHAR", "Form factor classification (mobile, desktop, tablet)", "INTERNAL", False, False, ["Enum: mobile, desktop, tablet, unknown"]),
                ColumnGovernance("os", "VARCHAR", "Operating system family (iOS, Android, Windows, macOS)", "INTERNAL", False, False, ["Parsed client OS"]),
                ColumnGovernance("browser", "VARCHAR", "Browser rendering engine (Safari, Chrome, Firefox, Edge)", "INTERNAL", False, False, ["Parsed user browser"])
            ]
        )

        # 4. Metric Governance
        self._metrics["conversion_rate"] = MetricGovernance(
            metric_key="conversion_rate",
            metric_name="Site Conversion Rate",
            business_owner="Marketing Analytics Director",
            technical_custodian="Senior Data Engineer",
            source_tables=["fact_sessions", "fact_conversions"],
            refresh_sla_minutes=30,
            minimum_health_threshold=90.0,
            business_definition="Percentage of total visitor sessions that completed a purchase transaction.",
            calculation_logic="count(distinct fact_conversions.order_id) / count(distinct fact_sessions.session_id)"
        )

        self._metrics["mobile_conversion_rate"] = MetricGovernance(
            metric_key="mobile_conversion_rate",
            metric_name="Mobile Segment Conversion Rate",
            business_owner="Mobile Experience & Growth Lead",
            technical_custodian="Content Ops & Site Quality Lead",
            source_tables=["fact_sessions", "fact_conversions", "dim_device"],
            refresh_sla_minutes=30,
            minimum_health_threshold=90.0,
            business_definition="Conversion velocity specifically on mobile responsive viewports.",
            calculation_logic="Conversions where dim_device.device_type='mobile' / Sessions where dim_device.device_type='mobile'"
        )

    def get_table(self, table_name: str) -> Optional[TableGovernance]:
        return self._tables.get(table_name)

    def list_tables(self) -> List[TableGovernance]:
        return list(self._tables.values())

    def get_metric(self, metric_key: str) -> Optional[MetricGovernance]:
        return self._metrics.get(metric_key)

    def list_metrics(self) -> List[MetricGovernance]:
        return list(self._metrics.values())
