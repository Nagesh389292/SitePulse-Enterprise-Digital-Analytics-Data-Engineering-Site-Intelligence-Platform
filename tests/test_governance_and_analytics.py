"""Unit tests for Governance, Risk Register, Impact Modeling, Variance, and Incident Lifecycle."""
import duckdb
import pytest
from fastapi.testclient import TestClient

from sitepulse.serving.star_schema import init_serving_schema
from sitepulse.governance.data_dictionary import DataGovernanceCatalog
from sitepulse.governance.risk_register import RiskRegister
from sitepulse.analytics.impact_modeler import BusinessImpactModeler
from sitepulse.analytics.variance_engine import VarianceEngine
from sitepulse.incident_mgmt.lifecycle import IncidentLifecycleManager
from sitepulse.api.app import app


@pytest.fixture
def mock_db():
    conn = duckdb.connect(":memory:")
    init_serving_schema(conn)

    # Insert dimensions
    conn.execute("""
    INSERT INTO dim_device (device_sk, device_type, os, browser) VALUES
    ('dev_mobile', 'mobile', 'iOS', 'Safari'),
    ('dev_desktop', 'desktop', 'macOS', 'Chrome');
    """)

    # Insert 100 sessions (50 mobile with 0 conversions, 50 desktop with 4 conversions)
    for i in range(100):
        is_mobile = (i < 50)
        has_purch = 1 if (not is_mobile and i % 10 == 0) else 0
        rev = 120.0 if has_purch else 0.0

        conn.execute(f"""
        INSERT INTO fact_sessions (session_sk, session_id, visitor_id, session_start, session_end,
                                  duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
                                  total_revenue, device_sk, campaign_sk, session_date)
        VALUES ('sess_{i}', 's_{i}', 'v_{i}', '2026-10-01 10:00:00', '2026-10-01 10:05:00',
                300, 4, 1, 1, {has_purch}, {rev},
                '{"dev_mobile" if is_mobile else "dev_desktop"}', 'camp_direct', '2026-10-01');
        """)
    return conn


def test_data_governance_catalog():
    catalog = DataGovernanceCatalog()
    fact_sess = catalog.get_table("fact_sessions")
    assert fact_sess is not None
    assert fact_sess.data_owner == "Head of Content Ops & Site Quality (Dentsu)"
    assert fact_sess.refresh_sla_minutes == 30
    assert any(c.name == "visitor_id" and c.sensitivity == "CONFIDENTIAL_PII" for c in fact_sess.columns)


def test_risk_register():
    reg = RiskRegister()
    risks = reg.list_risks()
    assert len(risks) >= 6
    assert any(r.risk_id == "RSK-001" and "Schema Drift" in r.title for r in risks)
    assert any(r.risk_id == "RSK-006" and "sqlglot" in r.engineering_mitigation for r in risks)

    summary = reg.get_risk_summary()
    assert summary["mitigation_coverage_pct"] == 100.0


def test_business_impact_modeler(mock_db):
    modeler = BusinessImpactModeler(mock_db)
    mobile_impact = modeler.quantify_segment_impact("mobile")

    assert mobile_impact.sessions == 50
    assert mobile_impact.actual_conversions == 0
    # Expected 50 * 5% = 2.5 ~ 2 to 3
    assert mobile_impact.expected_conversions >= 2
    assert mobile_impact.conversion_gap >= 2
    assert mobile_impact.revenue_exposure_usd > 0
    assert mobile_impact.exposure_type == "UNTRACKED_TELEMETRY_REVENUE"
    assert "Smart Bidding" in mobile_impact.marketing_ad_spend_risk


def test_variance_engine(mock_db):
    engine = VarianceEngine(mock_db)
    variances = engine.compute_kpi_variances()
    assert len(variances) >= 4
    ascii_card = engine.generate_ascii_dashboard()
    assert "SITEPULSE EXECUTIVE ANALYTICS" in ascii_card
    assert "Conversion Rate" in ascii_card


def test_incident_lifecycle_state_machine(mock_db):
    mgr = IncidentLifecycleManager(mock_db)

    # 1. Create incident in DETECTED status
    inc = mgr.create_incident(
        severity="CRITICAL",
        affected_metric="conversion_rate",
        affected_segment="device:mobile",
        root_cause_classification="TELEMETRY_CLIENT_SDK_EXCEPTION",
        business_impact_usd=360.0,
        action_items=["Inspect client JS error logs", "Verify mobile checkout trigger"]
    )
    assert inc.status == "DETECTED"

    # 2. Transition DETECTED -> TRIAGED
    triaged = mgr.transition_status(inc.incident_id, "TRIAGED", notes="Acknowledged by Lead Content Ops Engineer")
    assert triaged.status == "TRIAGED"

    # 3. Transition TRIAGED -> INVESTIGATING
    investigating = mgr.transition_status(inc.incident_id, "INVESTIGATING", notes="Root-cause confirmed via RCA Investigator")
    assert investigating.status == "INVESTIGATING"

    # 4. Transition INVESTIGATING -> ROOT_CAUSE_IDENTIFIED
    rc_id = mgr.transition_status(inc.incident_id, "ROOT_CAUSE_IDENTIFIED", notes="Mobile touch handler TypeError in checkout.js")
    assert rc_id.status == "ROOT_CAUSE_IDENTIFIED"

    # 5. Invalid transition: cannot jump from ROOT_CAUSE_IDENTIFIED straight to CLOSED
    with pytest.raises(ValueError):
        mgr.transition_status(inc.incident_id, "CLOSED")

    # 6. Complete lifecycle
    mgr.transition_status(inc.incident_id, "REMEDIATION", notes="Patch deployed to GTM")
    mgr.transition_status(inc.incident_id, "VALIDATED", notes="Mobile events flowing")
    closed = mgr.transition_status(inc.incident_id, "CLOSED", notes="Incident resolved")
    assert closed.status == "CLOSED"
    assert closed.resolved_at is not None


def test_executive_and_governance_api_endpoints():
    client = TestClient(app)

    # Executive Summary API
    res_exec = client.get("/api/v1/executive/summary")
    assert res_exec.status_code == 200
    assert "kpi_variances" in res_exec.json()
    assert "financial_exposure" in res_exec.json()

    # Governance Tables API
    res_gov = client.get("/api/v1/governance/tables")
    assert res_gov.status_code == 200
    assert res_gov.json()["count"] >= 3

    # Governance Risks API
    res_risks = client.get("/api/v1/governance/risks")
    assert res_risks.status_code == 200
    assert "summary" in res_risks.json()
    assert len(res_risks.json()["risks"]) >= 6

    # Incidents Lifecycle API
    res_inc = client.get("/api/v1/incidents/lifecycle")
    assert res_inc.status_code == 200
