"""Tests for the Business Metric Anomaly Engine."""
import duckdb
import pytest
from sitepulse.serving.star_schema import init_serving_schema
from sitepulse.quality_engine.business_anomaly import BusinessMetricAnomalyEngine


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
    return conn


def test_clean_baseline_no_anomalies(mock_db):
    # Insert healthy traffic
    for i in range(50):
        mock_db.execute(f"""
        INSERT INTO fact_sessions (session_sk, session_id, visitor_id, session_start, session_end,
                                  duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
                                  total_revenue, device_sk, campaign_sk, session_date)
        VALUES ('sess_{i}', 's_{i}', 'v_{i}', '2026-10-01 10:00:00', '2026-10-01 10:05:00',
                300, 4, 1, 1, {1 if i % 10 == 0 else 0}, {150.0 if i % 10 == 0 else 0.0},
                '{"dev_mobile" if i % 2 == 0 else "dev_desktop"}', 'camp_direct', '2026-10-01');
        """)

    engine = BusinessMetricAnomalyEngine(mock_db)
    eval_res = engine.evaluate_business_metrics(structural_health_score=100.0)

    assert eval_res.business_health_score >= 90.0
    assert eval_res.status == "HEALTHY"
    assert eval_res.composite_reliability_score >= 90.0


def test_detects_mobile_conversion_divergence(mock_db):
    # Desktop converts, Mobile converts at 0%
    for i in range(50):
        is_mobile = (i % 2 == 0)
        has_purch = 1 if (not is_mobile and i % 5 == 0) else 0
        rev = 200.0 if has_purch else 0.0

        mock_db.execute(f"""
        INSERT INTO fact_sessions (session_sk, session_id, visitor_id, session_start, session_end,
                                  duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
                                  total_revenue, device_sk, campaign_sk, session_date)
        VALUES ('sess_{i}', 's_{i}', 'v_{i}', '2026-10-01 10:00:00', '2026-10-01 10:05:00',
                300, 4, 1, 1, {has_purch}, {rev},
                '{"dev_mobile" if is_mobile else "dev_desktop"}', 'camp_direct', '2026-10-01');
        """)

    engine = BusinessMetricAnomalyEngine(mock_db)
    # Even if structural quality is 100/100, business health must catch the divergence!
    eval_res = engine.evaluate_business_metrics(structural_health_score=100.0)

    assert any(a.anomaly_type == "DEVICE_SEGMENT_DIVERGENCE" for a in eval_res.anomalies)
    assert eval_res.business_health_score < 100.0
    # Composite score must reflect the business drop
    assert eval_res.composite_reliability_score < 100.0


def test_detects_revenue_collapse(mock_db):
    # 30 sessions, 0 revenue
    for i in range(30):
        mock_db.execute(f"""
        INSERT INTO fact_sessions (session_sk, session_id, visitor_id, session_start, session_end,
                                  duration_seconds, page_view_count, has_cart, has_checkout, has_purchase,
                                  total_revenue, device_sk, campaign_sk, session_date)
        VALUES ('sess_{i}', 's_{i}', 'v_{i}', '2026-10-01 10:00:00', '2026-10-01 10:05:00',
                300, 3, 0, 0, 0, 0.0, 'dev_desktop', 'camp_direct', '2026-10-01');
        """)

    engine = BusinessMetricAnomalyEngine(mock_db)
    eval_res = engine.evaluate_business_metrics(structural_health_score=100.0)

    assert any(a.anomaly_type == "REVENUE_COLLAPSE" for a in eval_res.anomalies)
    assert eval_res.business_health_score < 100.0
