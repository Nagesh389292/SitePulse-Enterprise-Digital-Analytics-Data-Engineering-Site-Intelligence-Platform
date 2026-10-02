"""Demonstration Harness: End-to-End Failure Injections, Detection, Governance, and Business Impact Analysis."""
import os
import sys
import shutil
import time
from pathlib import Path
from datetime import datetime, timezone

from sitepulse.generator.event_stream import ClickstreamGenerator, FailureScenario
from sitepulse.pyspark_engine.spark_session import get_spark_session
from sitepulse.pyspark_engine.sessionizer import EventSessionizer
from sitepulse.serving.star_schema import ServingLayerManager
from sitepulse.quality_engine.evaluator import SiteQualityEngine
from sitepulse.quality_engine.business_anomaly import BusinessMetricAnomalyEngine
from sitepulse.semantic.catalog import SemanticCatalog
from sitepulse.ai_agent.nl_sql import NLToSQLAgent
from sitepulse.ai_agent.rca_investigator import RCAInvestigator
from sitepulse.analytics.impact_modeler import BusinessImpactModeler
from sitepulse.analytics.variance_engine import VarianceEngine
from sitepulse.incident_mgmt.lifecycle import IncidentLifecycleManager

DEMO_DIR = Path("C:/Users/NAGESH~1/.gemini/antigravity-ide/scratch/sitepulse/data/demo_scenarios")


def run_scenario(
    spark,
    scenario_name: str,
    failure_scenario: FailureScenario,
    num_sessions: int = 100
):
    print("\n" + "=" * 80)
    print(f"  RUNNING SCENARIO: {scenario_name}")
    print(f"  Failure Mode: {failure_scenario.value}")
    print("=" * 80)

    scenario_path = DEMO_DIR / failure_scenario.value
    if scenario_path.exists():
        shutil.rmtree(scenario_path, ignore_errors=True)
    scenario_path.mkdir(parents=True, exist_ok=True)

    raw_path = scenario_path / "raw.jsonl"
    bronze_path = scenario_path / "bronze"
    silver_sessions = scenario_path / "silver_sessions"
    silver_conversions = scenario_path / "silver_conversions"
    quarantine_path = scenario_path / "quarantine"
    db_path = scenario_path / "serving.duckdb"

    # 1. Event Stream Generation
    gen = ClickstreamGenerator(seed=42)
    events = gen.generate_batch(num_sessions=num_sessions, failure_scenario=failure_scenario)
    gen.export_to_jsonl(events, raw_path)
    print(f"  [1. Ingestion] Generated {len(events)} events across {num_sessions} visitor sessions.")

    # 2. PySpark Processing
    sessionizer = EventSessionizer(spark=spark)
    pyspark_res = sessionizer.process_raw_events(
        raw_events_path=raw_path,
        bronze_output_path=bronze_path,
        silver_sessions_path=silver_sessions,
        silver_conversions_path=silver_conversions,
        quarantine_output_path=quarantine_path
    )
    print(f"  [2. PySpark Engine] Raw: {pyspark_res.total_raw_records} | Clean: {pyspark_res.clean_records} | Quarantined: {pyspark_res.quarantined_records} | Deduplicated: {pyspark_res.deduplicated_records}")
    if pyspark_res.quarantine_breakdown:
        print(f"      Quarantine Breakdown: {pyspark_res.quarantine_breakdown}")

    # 3. Serving Layer Vectorized Load
    serving = ServingLayerManager(db_path=db_path)
    serving_counts = serving.load_from_lake(
        bronze_events_path=bronze_path,
        silver_sessions_path=silver_sessions,
        silver_conversions_path=silver_conversions
    )
    print(f"  [3. Serving Layer] Populated Star Schema: {serving_counts}")

    # 4. Data Quality Engine Scoring
    engine = SiteQualityEngine(conn=serving.conn)
    eval_res = engine.evaluate_quality(quarantine_path=quarantine_path)
    print(f"  [4. Data Quality Layer] STRUCTURAL HEALTH SCORE: {eval_res.overall_score}/100 [{eval_res.status}]")
    print(f"      - Freshness: {eval_res.freshness_score} | Completeness: {eval_res.completeness_score} | Schema Stability: {eval_res.schema_score}")
    print(f"      - Volume Z-Score: {eval_res.volume_z_score} (Score: {eval_res.volume_score}) | Referential: {eval_res.referential_score}")

    # 5. Business Metric Anomaly Engine
    anomaly_engine = BusinessMetricAnomalyEngine(db_conn=serving.conn)
    biz_eval = anomaly_engine.evaluate_business_metrics(structural_health_score=eval_res.overall_score)
    print(f"  [5. Business Metric Layer] BUSINESS HEALTH SCORE: {biz_eval.business_health_score}/100 | COMPOSITE RELIABILITY: {biz_eval.composite_reliability_score}/100 [{biz_eval.status}]")
    if biz_eval.anomalies:
        for anom in biz_eval.anomalies:
            print(f"      [ANOMALY] [{anom.severity}] {anom.anomaly_type} ({anom.segment}): {anom.root_cause_hint}")

    # 6. Governed Natural Language -> SQL Query Demonstration
    catalog = SemanticCatalog()
    nl_agent = NLToSQLAgent(db_conn=serving.conn, catalog=catalog)
    nl_result = nl_agent.translate_and_execute("Show conversion rate and revenue by device category")
    print("  [6. Governed NL-to-SQL Execution]")
    print(f"      Query: 'Show conversion rate and revenue by device category'")
    for r in nl_result.get("rows", []):
        dev = r.get("device_type") or "Unknown"
        sess = r.get("total_sessions", 0)
        cr = r.get("conversion_rate_pct", 0.0)
        rev = r.get("total_revenue", 0.0)
        print(f"      * {dev:<10} | Sessions: {sess:<4} | CR: {cr}% | Revenue: ${rev:,.2f}")

    # 7. Executive Analytics & Financial Exposure
    impact_modeler = BusinessImpactModeler(db_conn=serving.conn)
    mob_impact = impact_modeler.quantify_segment_impact("mobile")
    print("  [7. Executive Variance & Financial Exposure]")
    print(f"      Mobile Conversion Gap: {mob_impact.conversion_gap} orders | Revenue Exposure: ${mob_impact.revenue_exposure_usd:,.2f} ({mob_impact.exposure_type})")
    print(f"      Ad Spend Risk: {mob_impact.marketing_ad_spend_risk}")

    # 8. Autonomous RCA & Incident Lifecycle State Machine
    if biz_eval.anomalies or eval_res.incidents_generated:
        print("  [8. Autonomous RCA & ITIL Incident Lifecycle]")
        investigator = RCAInvestigator(db_conn=serving.conn)
        rca_report = investigator.investigate("Why did conversion or revenue drop?")
        print(f"      [DIAGNOSIS] Classification: {rca_report.classification} (Confidence: {rca_report.confidence_score * 100:.0f}%)")
        print(f"      [HEADLINE] {rca_report.headline}")
        print(f"      [IMPACT] {rca_report.downstream_business_impact}")
        print(f"      [REMEDIATION] Primary Action: {rca_report.actionable_remediations[0]}")

        # Incident Lifecycle Tracking
        lifecycle_mgr = IncidentLifecycleManager(db_conn=serving.conn)
        inc = lifecycle_mgr.create_incident(
            severity="CRITICAL" if biz_eval.status == "CRITICAL" else "HIGH",
            affected_metric="conversion_rate",
            affected_segment="device:mobile" if failure_scenario == FailureScenario.FAIL_MOBILE_DROP else "global",
            root_cause_classification=rca_report.classification,
            business_impact_usd=mob_impact.revenue_exposure_usd,
            action_items=rca_report.actionable_remediations
        )
        lifecycle_mgr.transition_status(inc.incident_id, "TRIAGED", notes="Acknowledged by Lead Content Ops Engineer")
        investigating = lifecycle_mgr.transition_status(inc.incident_id, "INVESTIGATING", notes="Root-cause diagnosed via RCA Investigator")
        print(f"      [INCIDENT LIFECYCLE] Created {inc.incident_id} -> Status: {investigating.status} | Owner: {investigating.owner_team}")

    serving.close()
    return eval_res, biz_eval


def main():
    print("\n" + "#" * 80)
    print("  SITEPULSE: REAL-WORLD FAILURE SCENARIO DEMONSTRATION")
    print("  Validating Automated Quality Detection, Governance & Business Impact")
    print("#" * 80)

    spark = get_spark_session("SitePulse-Demo")

    scenarios = [
        ("Clean Baseline Operations", FailureScenario.NONE),
        ("Tag Renaming Release Bug", FailureScenario.FAIL_SCHEMA_DRIFT),
        ("Mobile Telemetry Silent Drop", FailureScenario.FAIL_MOBILE_DROP),
        ("Double-Firing Tag Duplicates", FailureScenario.FAIL_DUPLICATE_SPIKE),
        ("Missing Identity Null Spike", FailureScenario.FAIL_NULL_EXPLOSION),
        ("Out-of-Bounds Revenue Injections", FailureScenario.FAIL_INVALID_REVENUE)
    ]

    for title, scenario in scenarios:
        run_scenario(spark, title, scenario, num_sessions=100)

    spark.stop()
    print("\n" + "#" * 80)
    print("  ALL DEMONSTRATION SCENARIOS COMPLETED SUCCESSFULLY.")
    print("#" * 80)


if __name__ == "__main__":
    main()
