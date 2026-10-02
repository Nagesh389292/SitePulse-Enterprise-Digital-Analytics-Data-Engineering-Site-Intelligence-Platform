"""Tests for Autonomous RCA Investigator and Governed NL-to-SQL Agent."""
import json
import duckdb
import pytest
from fastapi.testclient import TestClient

from sitepulse.serving.star_schema import init_serving_schema
from sitepulse.semantic.catalog import SemanticCatalog
from sitepulse.ai_agent.nl_sql import NLToSQLAgent
from sitepulse.ai_agent.rca_investigator import RCAInvestigator
from sitepulse.api.app import app


@pytest.fixture
def mock_db():
    conn = duckdb.connect(":memory:")
    init_serving_schema(conn)

    # Populate dimensions
    conn.execute("""
    INSERT INTO dim_device (device_sk, device_type, os, browser) VALUES
    ('dev_mobile', 'mobile', 'iOS', 'Safari'),
    ('dev_desktop', 'desktop', 'macOS', 'Chrome');
    """)

    # Populate sessions
    conn.execute("""
    INSERT INTO fact_sessions (session_sk, session_id, visitor_id, session_start, session_end,
                              duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
                              total_revenue, device_sk, campaign_sk, session_date) VALUES
    ('s_1', 'sess_1', 'v_1', '2026-10-01 10:00:00', '2026-10-01 10:05:00', 300, 4, 1, 1, 1, 150.0, 'dev_desktop', 'camp_1', '2026-10-01'),
    ('s_2', 'sess_2', 'v_2', '2026-10-01 10:00:00', '2026-10-01 10:05:00', 300, 3, 1, 1, 0, 0.0, 'dev_mobile', 'camp_1', '2026-10-01');
    """)

    # Populate conversions
    conn.execute("""
    INSERT INTO fact_conversions (conversion_sk, conversion_event_id, session_sk, session_id,
                                  visitor_id, conversion_time, order_id, revenue, items_count,
                                  currency, device_sk, campaign_sk, conversion_date) VALUES
    ('conv_1', 'evt_1', 's_1', 'sess_1', 'v_1', '2026-10-01 10:04:00', 'ord_1', 150.0, 1, 'USD', 'dev_desktop', 'camp_1', '2026-10-01');
    """)
    return conn


def test_nl_to_sql_execution(mock_db):
    agent = NLToSQLAgent(db_conn=mock_db)
    res = agent.translate_and_execute("Show conversion rate by device category")

    assert res["success"] is True
    assert "sanitized_sql" in res
    assert len(res["rows"]) >= 1
    assert any("mobile" in str(r.values()) or "desktop" in str(r.values()) for r in res["rows"])


def test_rca_detects_schema_drift_quarantine(mock_db):
    # Log a schema drift incident
    mock_db.execute("""
    INSERT INTO quality_incidents (incident_id, detected_at, severity, failure_type, metric_affected, description, details_json)
    VALUES ('inc_1', '2026-10-01 11:00:00', 'CRITICAL', 'SCHEMA_CONTRACT_VIOLATION', 'conversion_rate',
            '6 unknown events diverted to DLQ', '{"quarantine_count": 6}');
    """)

    investigator = RCAInvestigator(db_conn=mock_db)
    report = investigator.investigate("Why did conversion rate drop?")

    assert report.classification == "SCHEMA_DRIFT_QUARANTINE"
    assert report.is_telemetry_defect is True
    assert report.confidence_score >= 0.90
    assert len(report.actionable_remediations) >= 1


def test_rca_detects_mobile_telemetry_drop(mock_db):
    # Add mobile sessions with zero purchases, desktop with purchases
    for i in range(20):
        mock_db.execute(f"""
        INSERT INTO fact_sessions (session_sk, session_id, visitor_id, session_start, session_end,
                                  duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
                                  total_revenue, device_sk, campaign_sk, session_date) VALUES
        ('m_{i}', 'msess_{i}', 'mv_{i}', '2026-10-01 12:00:00', '2026-10-01 12:05:00', 250, 4, 1, 1, 0, 0.0, 'dev_mobile', 'camp_1', '2026-10-01');
        """)

    investigator = RCAInvestigator(db_conn=mock_db)
    report = investigator.investigate("Why did mobile conversion drop?")

    assert report.classification == "TELEMETRY_CLIENT_SDK_EXCEPTION"
    assert report.is_telemetry_defect is True
    assert "mobile" in report.headline.lower()


def test_fastapi_endpoints():
    client = TestClient(app)

    # Health check
    res_health = client.get("/health")
    assert res_health.status_code == 200
    assert res_health.json()["status"] == "UP"

    # Catalog metrics
    res_catalog = client.get("/api/v1/catalog/metrics")
    assert res_catalog.status_code == 200
    metrics = res_catalog.json()["metrics"]
    assert any(m["key"] == "conversion_rate" for m in metrics)
    assert any(m["key"] == "revenue_per_session" for m in metrics)

    # Query endpoint
    res_query = client.post("/api/v1/query", json={"query": "Show conversion rate and revenue by device"})
    assert res_query.status_code == 200
    assert res_query.json()["success"] is True

    # Investigate endpoint
    res_inv = client.post("/api/v1/investigate", json={"inquiry": "Why did conversion drop?"})
    assert res_inv.status_code == 200
    assert "classification" in res_inv.json()
