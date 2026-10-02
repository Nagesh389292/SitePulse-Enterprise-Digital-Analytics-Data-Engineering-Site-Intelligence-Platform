"""SitePulse Unified Enterprise Orchestrator.

Ties together the entire 14-step enterprise pipeline:
1. Ingestion / Clickstream Generation (Google Merchandise Store schema)
2. PySpark Sessionization, Deduplication & DLQ Quarantine
3. Apache Iceberg Lakehouse ACID Snapshot Commits
4. Star-Schema Dimensional Serving (DuckDB / SQL Server)
5. Automated Data Quality Health Evaluation (0-100 Score)
6. Business Metric Anomaly & Divergence Detection
7. Statistical Significance Engine (Two-Sample Proportion Z-Test)
8. Business & Financial Revenue Exposure Modeling
9. Executive KPI Variance Engine (Actual vs Baseline)
10. ITIL Incident Lifecycle Automation (INC-XXXX Triaged & Logged)
11. AST-Guarded NL-to-SQL Query Execution
12. Autonomous AI Root Cause Analysis (RCA) & Remediation Strategy
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List
import uuid
import shutil

from sitepulse.generator.event_stream import ClickstreamGenerator, FailureScenario
from sitepulse.pyspark_engine.spark_session import get_spark_session
from sitepulse.pyspark_engine.sessionizer import EventSessionizer
from sitepulse.lakehouse.iceberg_manager import IcebergLakehouseManager
from sitepulse.serving.star_schema import ServingLayerManager
from sitepulse.quality_engine.evaluator import SiteQualityEngine
from sitepulse.quality_engine.business_anomaly import BusinessMetricAnomalyEngine
from sitepulse.analytics.statistical_engine import StatisticalEngine
from sitepulse.analytics.impact_modeler import BusinessImpactModeler
from sitepulse.analytics.variance_engine import VarianceEngine
from sitepulse.incident_mgmt.lifecycle import IncidentLifecycleManager
from sitepulse.semantic.catalog import SemanticCatalog
from sitepulse.ai_agent.nl_sql import NLToSQLAgent
from sitepulse.ai_agent.rca_investigator import RCAInvestigator


@dataclass
class OrchestratorExecutionResult:
    execution_id: str
    scenario: Optional[FailureScenario]
    timestamp: datetime
    events_generated: int
    clean_events: int
    quarantined_events: int
    deduplicated_events: int
    sessions_built: int
    conversions_built: int
    iceberg_snapshots: Dict[str, str]
    structural_dq_score: float
    business_anomaly_score: float
    composite_reliability_score: float
    statistical_divergence_p_value: float
    is_statistically_significant: bool
    revenue_exposure_usd: float
    incident_id: Optional[str]
    incident_severity: Optional[str]
    incident_status: Optional[str]
    root_cause_diagnosis: Optional[str]
    recommended_action: Optional[str]
    kpi_summary: Dict[str, Any] = field(default_factory=dict)


class SitePulseOrchestrator:
    """Enterprise orchestrator coordinating Lakehouse, Serving, Analytics, Quality, and AI layers."""

    def __init__(self, db_path: Path, lakehouse_dir: Path, spark=None):
        self.db_path = Path(db_path)
        self.lakehouse_dir = Path(lakehouse_dir)
        self.lakehouse_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.spark = spark
        self.iceberg = IcebergLakehouseManager(self.lakehouse_dir)

    def run_pipeline(
        self,
        num_sessions: int = 50,
        scenario: Optional[FailureScenario] = None
    ) -> OrchestratorExecutionResult:
        """Executes the complete 14-step chain deterministically."""
        execution_id = f"exec-{uuid.uuid4().hex[:8]}"
        now = datetime.now(timezone.utc)
        scenario = scenario or FailureScenario.NONE

        spark_sess = self.spark or get_spark_session("SitePulse-Orchestrator")

        # Working paths
        work_dir = self.lakehouse_dir / execution_id
        work_dir.mkdir(parents=True, exist_ok=True)
        raw_path = work_dir / "raw.jsonl"
        bronze_path = work_dir / "bronze"
        silver_sessions = work_dir / "silver_sessions"
        silver_conversions = work_dir / "silver_conversions"
        quarantine_path = work_dir / "quarantine"

        # 1. Ingestion / Clickstream Generation
        gen = ClickstreamGenerator(seed=42)
        events = gen.generate_batch(num_sessions=num_sessions, failure_scenario=scenario)
        gen.export_to_jsonl(events, raw_path)

        # 2. PySpark Sessionization, Deduplication & DLQ Quarantine
        sessionizer = EventSessionizer(spark=spark_sess)
        pyspark_res = sessionizer.process_raw_events(
            raw_events_path=raw_path,
            bronze_output_path=bronze_path,
            silver_sessions_path=silver_sessions,
            silver_conversions_path=silver_conversions,
            quarantine_output_path=quarantine_path
        )

        # 3. Apache Iceberg Lakehouse ACID Snapshot Commits
        snap_bronze = self.iceberg.commit_snapshot(
            table_name="bronze_events",
            records_added=pyspark_res.clean_records,
            files_added=1,
            operation="append"
        )
        snap_sessions = self.iceberg.commit_snapshot(
            table_name="silver_sessions",
            records_added=pyspark_res.total_sessions,
            files_added=1,
            operation="append"
        )
        snap_conversions = self.iceberg.commit_snapshot(
            table_name="silver_conversions",
            records_added=pyspark_res.total_conversions,
            files_added=1,
            operation="append"
        )
        snap_dlq = self.iceberg.commit_snapshot(
            table_name="quarantine_dlq",
            records_added=pyspark_res.quarantined_records,
            files_added=1 if pyspark_res.quarantined_records > 0 else 0,
            operation="append"
        )

        # 4. Star-Schema Dimensional Serving (DuckDB / SQL Server)
        serving = ServingLayerManager(db_path=self.db_path)
        serving_counts = serving.load_from_lake(
            bronze_events_path=bronze_path,
            silver_sessions_path=silver_sessions,
            silver_conversions_path=silver_conversions
        )

        # 5. Automated Data Quality Health Evaluation (0-100 Structural Score)
        quality_engine = SiteQualityEngine(conn=serving.conn)
        eval_res = quality_engine.evaluate_quality(quarantine_path=quarantine_path)
        structural_score = float(eval_res.overall_score)

        # 6. Business Metric Anomaly & Divergence Detection
        anomaly_engine = BusinessMetricAnomalyEngine(db_conn=serving.conn)
        biz_eval = anomaly_engine.evaluate_business_metrics(structural_health_score=structural_score)
        business_score = float(biz_eval.business_health_score)
        composite_score = float(biz_eval.composite_reliability_score)

        # 7. Statistical Significance Engine (Mobile vs Desktop Divergence)
        stat_engine = StatisticalEngine(db_conn=serving.conn)
        div_test = stat_engine.test_device_conversion_divergence()
        z_stat = div_test.z_statistic
        p_val = div_test.p_value
        is_sig = div_test.is_statistically_significant

        # 8. Business & Financial Revenue Exposure Modeling
        impact_modeler = BusinessImpactModeler(db_conn=serving.conn)
        mob_impact = impact_modeler.quantify_segment_impact("mobile")
        revenue_exposure = float(mob_impact.revenue_exposure_usd)

        # 9. Executive KPI Variance Engine
        variance_engine = VarianceEngine(db_conn=serving.conn)
        variances = variance_engine.compute_kpi_variances()
        variance_map = {v.metric_name: v.actual for v in variances}

        # 10. ITIL Incident Lifecycle Automation
        incident_id = None
        incident_sev = None
        incident_status = None
        diagnosis_headline = None
        primary_remediation = None

        if (scenario and scenario != FailureScenario.NONE) or composite_score < 75.0:
            sev = "CRITICAL" if composite_score < 75.0 else "HIGH"
            incident_mgr = IncidentLifecycleManager(db_conn=serving.conn)
            inc = incident_mgr.create_incident(
                severity=sev,
                affected_metric="Conversion Rate / Site Quality",
                affected_segment="mobile" if scenario == FailureScenario.FAIL_MOBILE_DROP else "all",
                root_cause_classification=scenario.name if scenario else "UNKNOWN",
                business_impact_usd=revenue_exposure,
                owner_team="Data Engineering & Content Ops",
                assigned_engineer="On-Call Analytics Engineer"
            )
            incident_id = inc.incident_id
            incident_sev = inc.severity

            # Advance state: DETECTED -> TRIAGED -> INVESTIGATING
            incident_mgr.transition_status(incident_id, "TRIAGED", "On-call SRE confirmed anomaly metrics.")
            incident_mgr.transition_status(incident_id, "INVESTIGATING", "Triggered autonomous AI RCA investigation.")

            # 11 & 12. Autonomous RCA Investigation
            investigator = RCAInvestigator(db_conn=serving.conn)
            rca_report = investigator.investigate("Why did conversion or revenue drop?")
            diagnosis_headline = rca_report.headline
            if rca_report.actionable_remediations:
                primary_remediation = rca_report.actionable_remediations[0]

            incident_mgr.transition_status(
                incident_id,
                "ROOT_CAUSE_IDENTIFIED",
                f"RCA Agent Identified: {diagnosis_headline}"
            )
            inc_obj = incident_mgr.get_incident(incident_id)
            incident_status = inc_obj.status if inc_obj else "ROOT_CAUSE_IDENTIFIED"

        # Clean up temporary execution working dir
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass

        return OrchestratorExecutionResult(
            execution_id=execution_id,
            scenario=scenario,
            timestamp=now,
            events_generated=pyspark_res.total_raw_records,
            clean_events=pyspark_res.clean_records,
            quarantined_events=pyspark_res.quarantined_records,
            deduplicated_events=pyspark_res.deduplicated_records,
            sessions_built=pyspark_res.total_sessions,
            conversions_built=pyspark_res.total_conversions,
            iceberg_snapshots={
                "bronze_events": snap_bronze.snapshot_id,
                "silver_sessions": snap_sessions.snapshot_id,
                "silver_conversions": snap_conversions.snapshot_id,
                "quarantine_dlq": snap_dlq.snapshot_id
            },
            structural_dq_score=structural_score,
            business_anomaly_score=business_score,
            composite_reliability_score=composite_score,
            statistical_divergence_p_value=p_val,
            is_statistically_significant=is_sig,
            revenue_exposure_usd=revenue_exposure,
            incident_id=incident_id,
            incident_severity=incident_sev,
            incident_status=incident_status,
            root_cause_diagnosis=diagnosis_headline,
            recommended_action=primary_remediation,
            kpi_summary={
                "total_sessions": variance_map.get("Total Sessions", 0),
                "total_revenue": variance_map.get("Tracked Revenue", 0.0),
                "conversion_rate": variance_map.get("Conversion Rate", 0.0),
                "avg_order_value": variance_map.get("Average Order Value (AOV)", 0.0)
            }
        )
