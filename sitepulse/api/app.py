"""FastAPI Operational, Governance & Autonomous Investigation Service.

Exposes REST APIs for:
1. Health and Quality Score summaries
2. Data Quality & Quarantine Incidents
3. Business Metric Anomalies
4. Semantic Metric Catalog inspection
5. Governed NL-to-SQL Query Execution (AST guarded)
6. Autonomous Root Cause Analysis (RCA) Investigation
7. Executive KPI & Variance Dashboard
8. Enterprise Data Governance & Risk Register
9. ITIL-Aligned Incident Lifecycle State Machine
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
from fastapi import FastAPI, HTTPException, Query, Path as FPath
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from sitepulse.config import SERVING_DB_PATH
from sitepulse.serving.star_schema import get_serving_connection, init_serving_schema
from sitepulse.semantic.catalog import SemanticCatalog
from sitepulse.ai_agent.sql_guard import SQLGuard
from sitepulse.ai_agent.nl_sql import NLToSQLAgent
from sitepulse.ai_agent.rca_investigator import RCAInvestigator, RCAReport
from sitepulse.quality_engine.business_anomaly import BusinessMetricAnomalyEngine
from sitepulse.governance.data_dictionary import DataGovernanceCatalog
from sitepulse.governance.risk_register import RiskRegister
from sitepulse.analytics.impact_modeler import BusinessImpactModeler
from sitepulse.analytics.variance_engine import VarianceEngine
from sitepulse.incident_mgmt.lifecycle import IncidentLifecycleManager

UI_FILE_PATH = Path(__file__).parent.parent / "ui" / "index.html"


app = FastAPI(
    title="SitePulse - Digital Analytics Data Quality, Site Intelligence & RCA Platform",
    description="Enterprise API bridging Data Engineering, Site Quality, Governance, and Decision-Ready Analytics.",
    version="2.0.0"
)

# Shared singletons
catalog = SemanticCatalog()
gov_catalog = DataGovernanceCatalog()
risk_reg = RiskRegister()


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def serve_dashboard():
    """Serves the stakeholder-facing executive and AI investigation dashboard."""
    if UI_FILE_PATH.exists():
        return HTMLResponse(content=UI_FILE_PATH.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>SitePulse Dashboard</h1><p>UI file not found.</p>")


class QueryRequest(BaseModel):
    query: str = Field(..., json_schema_extra={"example": "Show conversion rate and revenue by device category"})


class InvestigateRequest(BaseModel):
    inquiry: str = Field(..., json_schema_extra={"example": "Why did mobile conversion drop yesterday?"})


class TransitionRequest(BaseModel):
    new_status: str = Field(..., json_schema_extra={"example": "TRIAGED"})
    notes: Optional[str] = Field(None, json_schema_extra={"example": "Assigned to Content Ops on-call."})
    updated_by: Optional[str] = Field("DataEngineer", json_schema_extra={"example": "DataEngineer"})


@app.get("/health")
def health_check():
    return {"status": "UP", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/v1/health/summary")
def get_health_summary():
    """Retrieve composite, structural, and business health scores."""
    conn = get_serving_connection()
    try:
        init_serving_schema(conn)
        latest_dq = conn.execute("""
            SELECT evaluation_date, overall_score, freshness_score, completeness_score,
                   schema_score, volume_score, referential_score, status
            FROM quality_scores_daily
            ORDER BY evaluation_date DESC
            LIMIT 1
        """).fetchone()

        anomaly_engine = BusinessMetricAnomalyEngine(conn)
        struct_score = latest_dq[1] if latest_dq else 100.0
        biz_eval = anomaly_engine.evaluate_business_metrics(structural_health_score=struct_score)

        return {
            "evaluation_date": biz_eval.evaluation_date,
            "composite_reliability_score": biz_eval.composite_reliability_score,
            "status": biz_eval.status,
            "structural_quality": {
                "overall_score": struct_score,
                "freshness": latest_dq[2] if latest_dq else 100.0,
                "completeness": latest_dq[3] if latest_dq else 100.0,
                "schema_stability": latest_dq[4] if latest_dq else 100.0,
                "volume_score": latest_dq[5] if latest_dq else 100.0,
                "referential_score": latest_dq[6] if latest_dq else 100.0,
            },
            "business_health": {
                "score": biz_eval.business_health_score,
                "active_anomalies_count": len(biz_eval.anomalies),
                "metrics_summary": biz_eval.metrics_summary
            }
        }
    finally:
        conn.close()


@app.get("/api/v1/executive/summary")
def get_executive_summary():
    """Executive Performance Dashboard with KPI variance and financial exposure."""
    conn = get_serving_connection()
    try:
        init_serving_schema(conn)
        variance_engine = VarianceEngine(conn)
        variances = variance_engine.compute_kpi_variances()

        impact_modeler = BusinessImpactModeler(conn)
        mobile_impact = impact_modeler.quantify_segment_impact("mobile")
        global_exposure = impact_modeler.quantify_global_exposure()

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "kpi_variances": [
                {
                    "metric": v.metric_name,
                    "actual": v.actual,
                    "baseline": v.baseline,
                    "variance_pct": v.variance_pct,
                    "status": v.status,
                    "unit": v.unit
                }
                for v in variances
            ],
            "financial_exposure": {
                "global_exposure_usd": global_exposure["estimated_financial_exposure_usd"],
                "mobile_segment_exposure_usd": mobile_impact.revenue_exposure_usd,
                "mobile_conversion_gap": mobile_impact.conversion_gap,
                "marketing_ad_spend_risk": mobile_impact.marketing_ad_spend_risk
            }
        }
    finally:
        conn.close()


@app.get("/api/v1/governance/tables")
def get_governance_tables():
    """Enterprise Data Dictionary: Data Owners, Stewards, SLAs, and retention."""
    tables = []
    for t in gov_catalog.list_tables():
        tables.append({
            "table_name": t.table_name,
            "domain": t.domain,
            "type": t.table_type,
            "data_owner": t.data_owner,
            "data_steward": t.data_steward,
            "refresh_frequency": t.refresh_frequency,
            "refresh_sla_minutes": t.refresh_sla_minutes,
            "quality_sla_completeness_pct": t.quality_sla_completeness_pct,
            "retention": t.retention_period,
            "column_count": len(t.columns)
        })
    return {"count": len(tables), "tables": tables}


@app.get("/api/v1/governance/risks")
def get_risk_register():
    """Operational Risk Register & Engineering Mitigations."""
    risks = []
    for r in risk_reg.list_risks():
        risks.append({
            "risk_id": r.risk_id,
            "title": r.title,
            "category": r.category,
            "probability": r.probability,
            "impact": r.impact,
            "risk_score": r.risk_score,
            "business_consequence": r.business_consequence,
            "engineering_mitigation": r.engineering_mitigation,
            "control_owner": r.control_owner,
            "residual_risk": r.residual_risk
        })
    return {"summary": risk_reg.get_risk_summary(), "risks": risks}


@app.get("/api/v1/incidents/lifecycle")
def list_incident_lifecycle(status: Optional[str] = None):
    """Fetch ITIL Incident Lifecycle records."""
    conn = get_serving_connection()
    try:
        mgr = IncidentLifecycleManager(conn)
        incidents = mgr.list_incidents(status=status)
        return {
            "count": len(incidents),
            "incidents": [
                {
                    "incident_id": i.incident_id,
                    "detected_at": str(i.detected_at),
                    "status": i.status,
                    "severity": i.severity,
                    "affected_metric": i.affected_metric,
                    "affected_segment": i.affected_segment,
                    "root_cause_classification": i.root_cause_classification,
                    "business_impact_usd": i.business_impact_usd,
                    "owner_team": i.owner_team,
                    "assigned_engineer": i.assigned_engineer,
                    "action_items": i.action_items,
                    "resolution_notes": i.resolution_notes
                }
                for i in incidents
            ]
        }
    finally:
        conn.close()


@app.post("/api/v1/incidents/{incident_id}/transition")
def transition_incident(incident_id: str, req: TransitionRequest):
    """Transition an incident through the state machine."""
    conn = get_serving_connection()
    try:
        mgr = IncidentLifecycleManager(conn)
        try:
            updated = mgr.transition_status(
                incident_id=incident_id,
                new_status=req.new_status,
                notes=req.notes,
                updated_by=req.updated_by
            )
            return {"success": True, "incident": updated}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
    finally:
        conn.close()


@app.get("/api/v1/catalog/metrics")
def get_metric_catalog():
    """List all governed analytical metrics from the Semantic Catalog."""
    metrics = []
    for m in catalog.list_metrics():
        metrics.append({
            "key": m.key,
            "name": m.name,
            "category": m.category,
            "formula": m.formula,
            "unit": m.unit,
            "business_definition": m.business_definition,
            "supported_dimensions": m.supported_dimensions
        })
    return {"count": len(metrics), "metrics": metrics}


@app.get("/api/v1/scenarios")
def list_scenarios():
    """Returns available enterprise simulation scenarios and their expected outcomes across pipeline steps."""
    return {
        "scenarios": [
            {
                "id": "NONE",
                "name": "Clean Baseline Operations",
                "tag": "HEALTHY",
                "color": "#10b981",
                "description": "Standard GA4 benchmark clickstream with normal conversion funnel across all devices.",
                "expected_reliability": 100.0,
                "exposure_usd": 0.0
            },
            {
                "id": "FAIL_MOBILE_DROP",
                "name": "Silent Mobile Telemetry Drop",
                "tag": "CRITICAL ANOMALY",
                "color": "#f43f5e",
                "description": "Client-side JS event listener crash silently drops mobile purchase events. Structural DQ is 100% but Behavioral Anomaly catches it!",
                "expected_reliability": 50.0,
                "exposure_usd": 240.0
            },
            {
                "id": "FAIL_SCHEMA_DRIFT",
                "name": "Event Tag Renaming (Schema Drift)",
                "tag": "SCHEMA VIOLATION",
                "color": "#f59e0b",
                "description": "Tag release renames 'order_completed' to 'order_completed_v2'. PySpark contract routes 6 events to DLQ.",
                "expected_reliability": 62.5,
                "exposure_usd": 600.0
            },
            {
                "id": "FAIL_DUPLICATE_SPIKE",
                "name": "Tag Double-Firing Spike",
                "tag": "AUTO-REMEDIATED",
                "color": "#8b5cf6",
                "description": "GTM trigger double-fires 20% of events. PySpark deduplication engine eliminates 44 duplicate events.",
                "expected_reliability": 94.0,
                "exposure_usd": 0.0
            },
            {
                "id": "FAIL_NULL_EXPLOSION",
                "name": "Missing Identity Token Explosion",
                "tag": "DATA LOSS",
                "color": "#ef4444",
                "description": "Consent banner crash causes 32% of events to emit NULL visitor IDs. Completeness score collapses to 0/100.",
                "expected_reliability": 48.0,
                "exposure_usd": 180.0
            },
            {
                "id": "FAIL_INVALID_REVENUE",
                "name": "Extreme Revenue Outlier Injection",
                "tag": "FINANCIAL DLQ",
                "color": "#ec4899",
                "description": "Payment webhook currency glitch injects $99,999 transactions. Intercepted by financial boundary rules.",
                "expected_reliability": 75.0,
                "exposure_usd": 199998.0
            }
        ]
    }


class ScenarioSimulateRequest(BaseModel):
    scenario: str = Field("FAIL_MOBILE_DROP", json_schema_extra={"example": "FAIL_MOBILE_DROP"})


@app.post("/api/v1/scenarios/simulate")
def simulate_scenario(req: ScenarioSimulateRequest):
    """Returns comprehensive 9-step telemetry and analysis for the chosen scenario."""
    sc = req.scenario.upper()

    # Pre-calculated deterministic telemetry payloads for instant 60fps UI inspection
    telemetry_db = {
        "NONE": {
            "scenario": "NONE",
            "title": "Clean Baseline Operations",
            "overall_status": "HEALTHY",
            "composite_score": 100.0,
            "structural_score": 100.0,
            "business_score": 100.0,
            "steps": [
                {
                    "step_id": 1,
                    "title": "Raw Ingestion & Event Schema",
                    "badge": "222 Events",
                    "status": "PASS",
                    "latency": "120ms",
                    "summary": "222 clean GA4 e-commerce events ingested across desktop, mobile, and tablet.",
                    "details": {
                        "total_events": 222,
                        "valid_format_pct": 100.0,
                        "event_distribution": {"page_view": 124, "add_to_cart": 52, "begin_checkout": 36, "purchase": 10},
                        "device_distribution": {"desktop": 98, "mobile": 94, "tablet": 30},
                        "sample_payload": '{"event_name": "purchase", "visitor_id": "v-8392", "device": {"device_type": "mobile", "os": "iOS"}, "ecommerce": {"order_id": "ORD-9912", "revenue": 120.0}}'
                    }
                },
                {
                    "step_id": 2,
                    "title": "PySpark 30-min Sessionization & DLQ",
                    "badge": "0 Quarantined",
                    "status": "PASS",
                    "latency": "380ms",
                    "summary": "30-minute inactivity sliding window materialized 50 visitor sessions. 0 events quarantined to DLQ.",
                    "details": {
                        "sessions_built": 50,
                        "deduplicated_events": 222,
                        "quarantined_dlq_count": 0,
                        "window_timeout_seconds": 1800,
                        "quarantine_reasons": {}
                    }
                },
                {
                    "step_id": 3,
                    "title": "Apache Iceberg ACID Lakehouse Commits",
                    "badge": "4 Snapshots",
                    "status": "PASS",
                    "latency": "210ms",
                    "summary": "ACID snapshot appends committed across bronze_events, silver_sessions, and silver_conversions.",
                    "details": {
                        "snapshots": {
                            "bronze_events": "snap-3c562e253892",
                            "silver_sessions": "snap-87388828fd03",
                            "silver_conversions": "snap-42194ba11da7",
                            "quarantine_dlq": "snap-bce69606491f"
                        },
                        "lakehouse_format": "Apache Iceberg v2",
                        "commit_type": "ACID Append"
                    }
                },
                {
                    "step_id": 4,
                    "title": "Analytical Star Schema Marts",
                    "badge": "5 Star Tables",
                    "status": "PASS",
                    "latency": "95ms",
                    "summary": "Dimensional surrogate keys (page_sk, device_sk, campaign_sk) mapped to fact_sessions and fact_conversions.",
                    "details": {
                        "tables": {
                            "fact_sessions": 50,
                            "fact_conversions": 5,
                            "dim_page": 7,
                            "dim_device": 5,
                            "dim_campaign": 4
                        },
                        "storage": "DuckDB Columnstore / SQL Server",
                        "total_revenue_loaded": "$600.00"
                    }
                },
                {
                    "step_id": 5,
                    "title": "5-Pillar Structural Data Quality",
                    "badge": "100/100",
                    "status": "PASS",
                    "latency": "45ms",
                    "summary": "Freshness, Completeness, Schema Stability, Volume Z-Score, and Referential Integrity all at 100%.",
                    "details": {
                        "freshness_score": 100.0,
                        "completeness_score": 100.0,
                        "schema_stability_score": 100.0,
                        "volume_z_score": 100.0,
                        "referential_integrity_score": 100.0,
                        "overall_structural_score": 100.0
                    }
                },
                {
                    "step_id": 6,
                    "title": "Behavioral Anomaly & Z-Test",
                    "badge": "p = 0.833 (No Anomaly)",
                    "status": "PASS",
                    "latency": "55ms",
                    "summary": "Two-sample proportion Z-test confirms mobile (4.8%) and desktop (5.2%) conversion rates are consistent.",
                    "details": {
                        "mobile_cr": 4.8,
                        "desktop_cr": 5.2,
                        "tablet_cr": 4.5,
                        "z_statistic": 0.21,
                        "p_value": 0.833,
                        "statistically_significant": False
                    }
                },
                {
                    "step_id": 7,
                    "title": "Executive KPI Variance & Exposure",
                    "badge": "$0 Exposure",
                    "status": "PASS",
                    "latency": "40ms",
                    "summary": "All KPIs tracking on-target against 14-day rolling baseline. Zero financial risk.",
                    "details": {
                        "sessions_variance": "0.0%",
                        "conversions_variance": "0.0%",
                        "revenue_variance": "0.0%",
                        "financial_exposure_usd": 0.0,
                        "marketing_risk": "Nominal operations"
                    }
                },
                {
                    "step_id": 8,
                    "title": "AST-Guarded NL-to-SQL & AI RCA",
                    "badge": "AST Validated",
                    "status": "PASS",
                    "latency": "140ms",
                    "summary": "Natural language query translated to validated AST SELECT. AI RCA classifies telemetry as completely healthy.",
                    "details": {
                        "ast_security": "ALLOWED (SELECT only)",
                        "generated_sql": "SELECT d.device_type, COUNT(s.session_id) as sessions, SUM(s.has_converted) as conversions FROM fact_sessions s JOIN dim_device d ON s.device_sk = d.device_sk GROUP BY 1",
                        "rca_diagnosis": "System operating nominally. All telemetry contracts satisfied."
                    }
                },
                {
                    "step_id": 9,
                    "title": "ITIL Incident Lifecycle & Risk",
                    "badge": "0 Incidents",
                    "status": "PASS",
                    "latency": "25ms",
                    "summary": "Zero open P1/P2 incidents. Operational risks fully mitigated by PySpark and Quality Gates.",
                    "details": {
                        "open_incidents": 0,
                        "active_p1": 0,
                        "sla_compliance_pct": 100.0,
                        "risk_level": "LOW (Monitored)"
                    }
                }
            ]
        },
        "FAIL_MOBILE_DROP": {
            "scenario": "FAIL_MOBILE_DROP",
            "title": "Silent Mobile Telemetry Drop",
            "overall_status": "CRITICAL ANOMALY",
            "composite_score": 50.0,
            "structural_score": 100.0,
            "business_score": 0.0,
            "steps": [
                {
                    "step_id": 1,
                    "title": "Raw Ingestion & Event Schema",
                    "badge": "198 Events (Drop)",
                    "status": "WARNING",
                    "latency": "130ms",
                    "summary": "198 events ingested. Mobile purchase events silently missing due to client-side JS runtime exception.",
                    "details": {
                        "total_events": 198,
                        "valid_format_pct": 100.0,
                        "event_distribution": {"page_view": 124, "add_to_cart": 52, "begin_checkout": 22, "purchase": 0},
                        "device_distribution": {"desktop": 98, "mobile": 72, "tablet": 28},
                        "sample_payload": '{"event_name": "begin_checkout", "device": {"device_type": "mobile"}, "error": "Uncaught TypeError: window.dataLayer.push in checkout.js:142"}'
                    }
                },
                {
                    "step_id": 2,
                    "title": "PySpark 30-min Sessionization & DLQ",
                    "badge": "50 Sessions Built",
                    "status": "PASS",
                    "latency": "395ms",
                    "summary": "PySpark successfully processes all 198 events because events themselves are valid. Tag drop is invisible to schema contracts!",
                    "details": {
                        "sessions_built": 50,
                        "deduplicated_events": 198,
                        "quarantined_dlq_count": 0,
                        "note": "Silent failure mode: Schema contracts pass because no malformed events were emitted."
                    }
                },
                {
                    "step_id": 3,
                    "title": "Apache Iceberg ACID Lakehouse Commits",
                    "badge": "4 Snapshots",
                    "status": "PASS",
                    "latency": "205ms",
                    "summary": "Snapshots committed cleanly. Silver conversions table has 0 mobile conversion records.",
                    "details": {
                        "snapshots": {
                            "bronze_events": "snap-9a81f3e7210b",
                            "silver_sessions": "snap-552bc9a018ec",
                            "silver_conversions": "snap-000000000000",
                            "quarantine_dlq": "snap-bce69606491f"
                        },
                        "lakehouse_format": "Apache Iceberg v2",
                        "commit_type": "ACID Append"
                    }
                },
                {
                    "step_id": 4,
                    "title": "Analytical Star Schema Marts",
                    "badge": "Mobile CR: 0.0%",
                    "status": "WARNING",
                    "latency": "102ms",
                    "summary": "fact_sessions shows 24 mobile sessions with 0 orders and $0 revenue; desktop conversions remain normal.",
                    "details": {
                        "tables": {
                            "fact_sessions": 50,
                            "fact_conversions": 0,
                            "dim_page": 7,
                            "dim_device": 5,
                            "dim_campaign": 4
                        },
                        "mobile_orders": 0,
                        "desktop_orders": 5,
                        "total_revenue_loaded": "$0.00 (Mobile Lost)"
                    }
                },
                {
                    "step_id": 5,
                    "title": "5-Pillar Structural Data Quality",
                    "badge": "100/100 (Blind Spot)",
                    "status": "PASS",
                    "latency": "42ms",
                    "summary": "Structural DQ reports 100/100 because incoming data has no NULLs or schema syntax errors!",
                    "details": {
                        "freshness_score": 100.0,
                        "completeness_score": 100.0,
                        "schema_stability_score": 100.0,
                        "volume_z_score": 100.0,
                        "referential_integrity_score": 100.0,
                        "overall_structural_score": 100.0,
                        "architectural_insight": "Highlights why Structural DQ alone is insufficient without Behavioral Anomaly Detection."
                    }
                },
                {
                    "step_id": 6,
                    "title": "Behavioral Anomaly & Z-Test",
                    "badge": "Z = 2.84, p = 0.0045",
                    "status": "CRITICAL",
                    "latency": "48ms",
                    "summary": "CRITICAL DIVERGENCE! Two-sample Z-test confirms mobile conversion collapse (0% vs baseline 5%) is statistically significant (p < 0.01).",
                    "details": {
                        "mobile_cr": 0.0,
                        "desktop_cr": 5.1,
                        "z_statistic": 2.84,
                        "p_value": 0.0045,
                        "statistically_significant": True,
                        "hypothesis_result": "REJECT NULL HYPOTHESIS: Severe segment divergence detected."
                    }
                },
                {
                    "step_id": 7,
                    "title": "Executive KPI Variance & Exposure",
                    "badge": "$240 Exposure / hr",
                    "status": "CRITICAL",
                    "latency": "38ms",
                    "summary": "Mobile conversion variance: -100%. Financial revenue exposure modeled at $240/batch ($28,800/day).",
                    "details": {
                        "mobile_conversion_variance": "-100.0%",
                        "mobile_revenue_variance": "-100.0%",
                        "revenue_exposure_usd": 240.0,
                        "marketing_risk": "Smart Bidding / Performance Max algorithms will infer 0% mobile ROAS, defunding mobile campaigns."
                    }
                },
                {
                    "step_id": 8,
                    "title": "AST-Guarded NL-to-SQL & AI RCA",
                    "badge": "Root Cause Diagnosed",
                    "status": "RESOLVED",
                    "latency": "155ms",
                    "summary": "AI RCA correlates mobile session count, checkout touches, and conversion absence to pinpoint client tag crash.",
                    "details": {
                        "ast_security": "ALLOWED (SELECT only)",
                        "classification": "FAIL_MOBILE_DROP",
                        "headline": "Silent mobile checkout failure caused by client-side event listener crash in v2.4.1 release.",
                        "actionable_remediations": [
                            "Rollback mobile web release v2.4.1 to restore checkout event telemetry.",
                            "Temporarily suppress mobile Paid Search (CPC) campaigns to prevent automated Smart Bidding bid drops.",
                            "Add client-side telemetry heartbeat monitoring to automated end-to-end Cypress/Playwright suites."
                        ]
                    }
                },
                {
                    "step_id": 9,
                    "title": "ITIL Incident Lifecycle & Risk",
                    "badge": "P1 Incident Logged",
                    "status": "INVESTIGATING",
                    "latency": "32ms",
                    "summary": "Automated ITIL P1 incident INC-49B2926A raised with $240 financial exposure and assigned to Content Ops.",
                    "details": {
                        "incident_id": "INC-49B2926A",
                        "severity": "CRITICAL (P1)",
                        "affected_metric": "Conversion Rate / Site Quality",
                        "affected_segment": "mobile",
                        "status": "ROOT_CAUSE_IDENTIFIED",
                        "assigned_engineer": "On-Call Analytics Engineer",
                        "owner_team": "Data Engineering & Content Ops"
                    }
                }
            ]
        },
        "FAIL_SCHEMA_DRIFT": {
            "scenario": "FAIL_SCHEMA_DRIFT",
            "title": "Event Tag Renaming (Schema Drift)",
            "overall_status": "DEGRADED",
            "composite_score": 62.5,
            "structural_score": 75.0,
            "business_score": 50.0,
            "steps": [
                {
                    "step_id": 1,
                    "title": "Raw Ingestion & Event Schema",
                    "badge": "6 Renamed Tags",
                    "status": "WARNING",
                    "latency": "115ms",
                    "summary": "6 purchase events emitted as 'order_completed_v2' instead of governed 'order_completed'.",
                    "details": {
                        "total_events": 222,
                        "schema_divergence": "order_completed -> order_completed_v2",
                        "affected_tag": "purchase_completed",
                        "sample_payload": '{"event_name": "order_completed_v2", "order_id": "ORD-8821", "revenue": 145.0}'
                    }
                },
                {
                    "step_id": 2,
                    "title": "PySpark 30-min Sessionization & DLQ",
                    "badge": "6 Quarantined in DLQ",
                    "status": "CRITICAL",
                    "latency": "410ms",
                    "summary": "PySpark strict schema contract rejects 6 unmapped events and routes them to Quarantine DLQ.",
                    "details": {
                        "sessions_built": 50,
                        "quarantined_dlq_count": 6,
                        "rejection_rule": "UNRECOGNIZED_EVENT_NAME",
                        "dlq_table": "quarantine_events"
                    }
                },
                {
                    "step_id": 3,
                    "title": "Apache Iceberg ACID Lakehouse Commits",
                    "badge": "DLQ Snapshot Tagged",
                    "status": "PASS",
                    "latency": "190ms",
                    "summary": "Quarantine DLQ snapshot commits 6 rejected records for replay once schema mapping is patched.",
                    "details": {
                        "quarantine_snapshot": "snap-bce69606491f",
                        "replay_ready": True
                    }
                },
                {
                    "step_id": 4,
                    "title": "Analytical Star Schema Marts",
                    "badge": "0 Orders Loaded",
                    "status": "WARNING",
                    "latency": "98ms",
                    "summary": "fact_conversions has 0 rows because order events were trapped in Quarantine.",
                    "details": {
                        "fact_conversions_rows": 0,
                        "fact_sessions_rows": 50
                    }
                },
                {
                    "step_id": 5,
                    "title": "5-Pillar Structural Data Quality",
                    "badge": "Schema Stability 35/100",
                    "status": "WARNING",
                    "latency": "44ms",
                    "summary": "Schema Stability pillar trips due to contract violation, dropping structural DQ score to 75.0/100.",
                    "details": {
                        "schema_stability_score": 35.0,
                        "completeness_score": 100.0,
                        "overall_structural_score": 75.0
                    }
                },
                {
                    "step_id": 6,
                    "title": "Behavioral Anomaly & Z-Test",
                    "badge": "Global Drop",
                    "status": "WARNING",
                    "latency": "52ms",
                    "summary": "Global conversion drops across all devices uniformly (not a single segment).",
                    "details": {
                        "global_cr": 0.0,
                        "pattern": "Uniform across desktop, mobile, and tablet"
                    }
                },
                {
                    "step_id": 7,
                    "title": "Executive KPI Variance & Exposure",
                    "badge": "$600 Exposure",
                    "status": "CRITICAL",
                    "latency": "36ms",
                    "summary": "Conversion Rate: -100%, Revenue: -100%. $600 batch revenue quarantined.",
                    "details": {
                        "financial_exposure_usd": 600.0,
                        "marketing_risk": "All conversion tracking paused."
                    }
                },
                {
                    "step_id": 8,
                    "title": "AST-Guarded NL-to-SQL & AI RCA",
                    "badge": "Schema Drift Identified",
                    "status": "PASS",
                    "latency": "145ms",
                    "summary": "AI RCA flags Quarantine DLQ reason: Unrecognized event 'order_completed_v2'.",
                    "details": {
                        "classification": "FAIL_SCHEMA_DRIFT",
                        "headline": "Conversion loss caused by uncoordinated tag renaming to 'order_completed_v2'.",
                        "actionable_remediations": [
                            "Deploy PySpark schema contract mapping alias 'order_completed_v2' -> 'order_completed'.",
                            "Trigger Iceberg DLQ replay pipeline to backfill quarantined conversions.",
                            "Enforce CI/CD schema contract registry on GTM tag publishes."
                        ]
                    }
                },
                {
                    "step_id": 9,
                    "title": "ITIL Incident Lifecycle & Risk",
                    "badge": "P2 Incident Logged",
                    "status": "TRIAGED",
                    "latency": "28ms",
                    "summary": "P2 incident INC-SCH-812B created with remediation task for Analytics Engineering.",
                    "details": {
                        "incident_id": "INC-SCH-812B",
                        "severity": "HIGH (P2)",
                        "affected_metric": "Schema Stability",
                        "status": "TRIAGED"
                    }
                }
            ]
        },
        "FAIL_DUPLICATE_SPIKE": {
            "scenario": "FAIL_DUPLICATE_SPIKE",
            "title": "Tag Double-Firing Spike",
            "overall_status": "AUTO-REMEDIATED",
            "composite_score": 94.0,
            "structural_score": 92.0,
            "business_score": 96.0,
            "steps": [
                {
                    "step_id": 1,
                    "title": "Raw Ingestion & Event Schema",
                    "badge": "44 Duplicate Tags",
                    "status": "WARNING",
                    "latency": "135ms",
                    "summary": "Tag manager trigger double-firing generated 266 events containing 44 duplicate event IDs.",
                    "details": {
                        "total_events": 266,
                        "duplicate_count": 44,
                        "duplicate_pct": "16.5%"
                    }
                },
                {
                    "step_id": 2,
                    "title": "PySpark 30-min Sessionization & DLQ",
                    "badge": "44 Deduplicated",
                    "status": "PASS",
                    "latency": "425ms",
                    "summary": "PySpark window deduplication eliminates 44 duplicate events, preserving exactly 222 clean events.",
                    "details": {
                        "events_in": 266,
                        "deduplicated_events": 222,
                        "duplicates_dropped": 44,
                        "dedup_algorithm": "ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY timestamp)"
                    }
                },
                {
                    "step_id": 3,
                    "title": "Apache Iceberg ACID Lakehouse Commits",
                    "badge": "Clean Commits",
                    "status": "PASS",
                    "latency": "195ms",
                    "summary": "Silver sessions and conversions committed without any duplicate inflation.",
                    "details": {
                        "silver_sessions": 50,
                        "silver_conversions": 5
                    }
                },
                {
                    "step_id": 4,
                    "title": "Analytical Star Schema Marts",
                    "badge": "Accurate Metrics",
                    "status": "PASS",
                    "latency": "92ms",
                    "summary": "Conversion metrics and revenue saved from 20% artificial inflation.",
                    "details": {
                        "reported_revenue": "$600.00",
                        "unfiltered_inflated_revenue": "$720.00",
                        "inflation_averted": "$120.00"
                    }
                },
                {
                    "step_id": 5,
                    "title": "5-Pillar Structural Data Quality",
                    "badge": "Volume Z-Score 88/100",
                    "status": "PASS",
                    "latency": "40ms",
                    "summary": "Volume stability flags raw ingestion burst (+20%), but downstream tables are protected.",
                    "details": {
                        "volume_z_score": 88.0,
                        "overall_structural_score": 92.0
                    }
                },
                {
                    "step_id": 6,
                    "title": "Behavioral Anomaly & Z-Test",
                    "badge": "No Business Distortion",
                    "status": "PASS",
                    "latency": "50ms",
                    "summary": "Business ratios completely normal after automated PySpark deduplication.",
                    "details": {
                        "z_statistic": 0.15,
                        "p_value": 0.88,
                        "statistically_significant": False
                    }
                },
                {
                    "step_id": 7,
                    "title": "Executive KPI Variance & Exposure",
                    "badge": "$0 Exposure",
                    "status": "PASS",
                    "latency": "35ms",
                    "summary": "Zero financial exposure. Over-reporting error prevented by ETL layer.",
                    "details": {
                        "financial_exposure_usd": 0.0,
                        "prevented_over_reporting_usd": 120.0
                    }
                },
                {
                    "step_id": 8,
                    "title": "AST-Guarded NL-to-SQL & AI RCA",
                    "badge": "Auto-Remediation Verified",
                    "status": "PASS",
                    "latency": "135ms",
                    "summary": "AI RCA verifies deduplication succeeded; recommends checking GTM trigger firing conditions.",
                    "details": {
                        "classification": "FAIL_DUPLICATE_SPIKE",
                        "headline": "Tag double-firing intercepted and normalized by PySpark deduplication engine.",
                        "actionable_remediations": [
                            "Inspect Google Tag Manager trigger 'Once per page' vs 'Once per event'.",
                            "Check SPA route change event listeners for unmount cleanup."
                        ]
                    }
                },
                {
                    "step_id": 9,
                    "title": "ITIL Incident Lifecycle & Risk",
                    "badge": "Resolved by Pipeline",
                    "status": "CLOSED",
                    "latency": "22ms",
                    "summary": "P3 ticket auto-closed with resolution note: Remediated by PySpark Bronze->Silver boundary.",
                    "details": {
                        "incident_id": "INC-DUP-331C",
                        "severity": "LOW (P3)",
                        "status": "CLOSED",
                        "resolution_notes": "Automatically mitigated by PySpark deduplication."
                    }
                }
            ]
        },
        "FAIL_NULL_EXPLOSION": {
            "scenario": "FAIL_NULL_EXPLOSION",
            "title": "Missing Identity Token Explosion",
            "overall_status": "DATA LOSS",
            "composite_score": 48.0,
            "structural_score": 40.0,
            "business_score": 56.0,
            "steps": [
                {
                    "step_id": 1,
                    "title": "Raw Ingestion & Event Schema",
                    "badge": "71 NULL Visitors",
                    "status": "CRITICAL",
                    "latency": "125ms",
                    "summary": "Cookie consent script failure causes 32% of events to emit NULL visitor_id.",
                    "details": {
                        "total_events": 222,
                        "null_visitor_count": 71,
                        "null_rate_pct": "32.0%"
                    }
                },
                {
                    "step_id": 2,
                    "title": "PySpark 30-min Sessionization & DLQ",
                    "badge": "71 Quarantined in DLQ",
                    "status": "CRITICAL",
                    "latency": "400ms",
                    "summary": "PySpark identity resolution contract rejects all events lacking visitor ID.",
                    "details": {
                        "quarantined_dlq_count": 71,
                        "sessions_built": 34,
                        "rejection_rule": "NULL_VISITOR_ID"
                    }
                },
                {
                    "step_id": 3,
                    "title": "Apache Iceberg ACID Lakehouse Commits",
                    "badge": "Quarantine Spike",
                    "status": "WARNING",
                    "latency": "198ms",
                    "summary": "DLQ Iceberg table commits 71 anonymous orphaned events.",
                    "details": {
                        "quarantine_snapshot": "snap-718a221f001c"
                    }
                },
                {
                    "step_id": 4,
                    "title": "Analytical Star Schema Marts",
                    "badge": "34 Valid Sessions",
                    "status": "WARNING",
                    "latency": "90ms",
                    "summary": "Only 34 clean sessions materialized; unidentifiable journeys excluded from marts.",
                    "details": {
                        "fact_sessions_count": 34,
                        "baseline_sessions": 50
                    }
                },
                {
                    "step_id": 5,
                    "title": "5-Pillar Structural Data Quality",
                    "badge": "Completeness 0/100",
                    "status": "CRITICAL",
                    "latency": "42ms",
                    "summary": "Completeness pillar collapses from 100 to 0/100, breaching data SLA.",
                    "details": {
                        "completeness_score": 0.0,
                        "overall_structural_score": 40.0
                    }
                },
                {
                    "step_id": 6,
                    "title": "Behavioral Anomaly & Z-Test",
                    "badge": "Volume Collapse",
                    "status": "WARNING",
                    "latency": "49ms",
                    "summary": "Total attributed session volume drops by 32% across all visitor segments.",
                    "details": {
                        "session_loss_pct": "32.0%"
                    }
                },
                {
                    "step_id": 7,
                    "title": "Executive KPI Variance & Exposure",
                    "badge": "$180 Exposure",
                    "status": "CRITICAL",
                    "latency": "38ms",
                    "summary": "Session volume variance -32%, revenue variance -30%. $180 revenue unattributed.",
                    "details": {
                        "financial_exposure_usd": 180.0
                    }
                },
                {
                    "step_id": 8,
                    "title": "AST-Guarded NL-to-SQL & AI RCA",
                    "badge": "Identity Bug Diagnosed",
                    "status": "PASS",
                    "latency": "148ms",
                    "summary": "AI RCA flags Consent Management Platform (CMP) cookie blocking regression.",
                    "details": {
                        "classification": "FAIL_NULL_EXPLOSION",
                        "headline": "Identity token null explosion triggered by CMP cookie banner blocking local storage keys.",
                        "actionable_remediations": [
                            "Rollback CMP banner update deployed at 14:00 UTC.",
                            "Verify 1st-party cookie fallback mechanism in client analytics wrapper."
                        ]
                    }
                },
                {
                    "step_id": 9,
                    "title": "ITIL Incident Lifecycle & Risk",
                    "badge": "P2 Incident Logged",
                    "status": "TRIAGED",
                    "latency": "25ms",
                    "summary": "P2 incident INC-ID-904A opened for Frontend Consent Engineering team.",
                    "details": {
                        "incident_id": "INC-ID-904A",
                        "severity": "HIGH (P2)",
                        "affected_metric": "Identity Completeness",
                        "status": "TRIAGED"
                    }
                }
            ]
        },
        "FAIL_INVALID_REVENUE": {
            "scenario": "FAIL_INVALID_REVENUE",
            "title": "Extreme Revenue Outlier Injection",
            "overall_status": "FINANCIAL DLQ",
            "composite_score": 75.0,
            "structural_score": 85.0,
            "business_score": 65.0,
            "steps": [
                {
                    "step_id": 1,
                    "title": "Raw Ingestion & Event Schema",
                    "badge": "2 Bogus $99,999 Orders",
                    "status": "CRITICAL",
                    "latency": "118ms",
                    "summary": "2 anomalous orders with extreme values of $99,999.00 injected by webhook currency bug.",
                    "details": {
                        "total_events": 222,
                        "outlier_orders": 2,
                        "outlier_value": "$99,999.00 each"
                    }
                },
                {
                    "step_id": 2,
                    "title": "PySpark 30-min Sessionization & DLQ",
                    "badge": "2 Quarantined in DLQ",
                    "status": "PASS",
                    "latency": "415ms",
                    "summary": "PySpark financial guard (revenue <= $10,000) catches and quarantines both outlier rows.",
                    "details": {
                        "quarantined_dlq_count": 2,
                        "rejection_rule": "OUTLIER_REVENUE_BOUNDS",
                        "safe_conversions_built": 5
                    }
                },
                {
                    "step_id": 3,
                    "title": "Apache Iceberg ACID Lakehouse Commits",
                    "badge": "Financial DLQ",
                    "status": "PASS",
                    "latency": "190ms",
                    "summary": "Quarantined in DLQ with financial audit tag; Gold reporting marts protected.",
                    "details": {
                        "quarantine_snapshot": "snap-9999fa00bc12"
                    }
                },
                {
                    "step_id": 4,
                    "title": "Analytical Star Schema Marts",
                    "badge": "True Revenue: $600",
                    "status": "PASS",
                    "latency": "94ms",
                    "summary": "Star schema reports legitimate $600.00 revenue instead of distorted $200,598.00.",
                    "details": {
                        "protected_revenue": "$600.00",
                        "distortion_averted": "$199,998.00"
                    }
                },
                {
                    "step_id": 5,
                    "title": "5-Pillar Structural Data Quality",
                    "badge": "Referential/Value 85/100",
                    "status": "WARNING",
                    "latency": "41ms",
                    "summary": "Value boundary check detects out-of-range transactions, triggering automated quarantine.",
                    "details": {
                        "referential_score": 85.0,
                        "overall_structural_score": 85.0
                    }
                },
                {
                    "step_id": 6,
                    "title": "Behavioral Anomaly & Z-Test",
                    "badge": "AOV Spike Averted",
                    "status": "PASS",
                    "latency": "51ms",
                    "summary": "Average Order Value (AOV) kept at nominal $120.00 rather than surging to $40,000.",
                    "details": {
                        "nominal_aov": 120.0,
                        "unfiltered_aov": 40119.6
                    }
                },
                {
                    "step_id": 7,
                    "title": "Executive KPI Variance & Exposure",
                    "badge": "$199,998 Protected",
                    "status": "PASS",
                    "latency": "35ms",
                    "summary": "Executive dashboards and finance reporting protected from massive $199,998 distortion.",
                    "details": {
                        "prevented_financial_distortion_usd": 199998.0
                    }
                },
                {
                    "step_id": 8,
                    "title": "AST-Guarded NL-to-SQL & AI RCA",
                    "badge": "Payment Glitch Diagnosed",
                    "status": "PASS",
                    "latency": "142ms",
                    "summary": "AI RCA identifies payment gateway webhook currency decimal missing (e.g. 99999 instead of 999.99).",
                    "details": {
                        "classification": "FAIL_INVALID_REVENUE",
                        "headline": "Extreme transaction revenue outlier intercepted at Silver ingestion boundary.",
                        "actionable_remediations": [
                            "Inspect payment gateway currency decimal formatting in webhook payload parser.",
                            "Verify currency multiplier in cart checkout handler."
                        ]
                    }
                },
                {
                    "step_id": 9,
                    "title": "ITIL Incident Lifecycle & Risk",
                    "badge": "P2 Financial Alert",
                    "status": "TRIAGED",
                    "latency": "24ms",
                    "summary": "P2 incident INC-FIN-552F logged for Payment Integrations engineering team.",
                    "details": {
                        "incident_id": "INC-FIN-552F",
                        "severity": "HIGH (P2)",
                        "affected_metric": "Financial Governance & Revenue Validity",
                        "status": "TRIAGED"
                    }
                }
            ]
        }
    }

    # Return selected scenario or default to FAIL_MOBILE_DROP
    return telemetry_db.get(sc, telemetry_db["FAIL_MOBILE_DROP"])


@app.post("/api/v1/query")
def execute_natural_query(req: QueryRequest):
    """Translate natural language query to AST-governed SQL and execute against DuckDB."""
    conn = get_serving_connection()
    try:
        init_serving_schema(conn)
        agent = NLToSQLAgent(db_conn=conn, catalog=catalog)
        result = agent.translate_and_execute(req.query)
        return result
    finally:
        conn.close()


@app.post("/api/v1/investigate")
def investigate_root_cause(req: InvestigateRequest):
    """Autonomous cross-layer investigation correlating telemetry, data quality, and business metrics."""
    conn = get_serving_connection()
    try:
        init_serving_schema(conn)
        investigator = RCAInvestigator(db_conn=conn)
        report: RCAReport = investigator.investigate(req.inquiry)
        return {
            "inquiry": report.inquiry,
            "investigation_timestamp": report.investigation_timestamp.isoformat(),
            "classification": report.classification,
            "headline": report.headline,
            "is_telemetry_defect": report.is_telemetry_defect,
            "confidence_score": report.confidence_score,
            "metric_evidence": report.metric_evidence,
            "funnel_evidence": report.funnel_evidence,
            "quarantine_evidence": report.quarantine_evidence,
            "downstream_business_impact": report.downstream_business_impact,
            "actionable_remediations": report.actionable_remediations
        }
    finally:
        conn.close()

