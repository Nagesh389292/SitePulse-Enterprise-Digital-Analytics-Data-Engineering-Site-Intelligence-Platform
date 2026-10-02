"""Tests for Site Quality Engine: Mathematical Scoring and Failure Diagnosis."""
import shutil
from pathlib import Path
from datetime import datetime, timezone
import pytest

from sitepulse.generator.event_stream import ClickstreamGenerator, FailureScenario
from sitepulse.pyspark_engine.spark_session import get_spark_session
from sitepulse.pyspark_engine.sessionizer import EventSessionizer
from sitepulse.serving.star_schema import ServingLayerManager
from sitepulse.quality_engine.evaluator import SiteQualityEngine

TEST_BASE = Path(__file__).resolve().parent.parent / "data" / "test_quality_tmp"


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("SitePulse-Quality-Tests")
    yield session
    session.stop()


def run_pipeline(spark, failure_scenario: FailureScenario, run_dir: Path, num_sessions: int = 100):
    if run_dir.exists():
        shutil.rmtree(run_dir, ignore_errors=True)
    run_dir.mkdir(parents=True, exist_ok=True)

    raw_path = run_dir / "raw.jsonl"
    bronze_path = run_dir / "bronze"
    silver_sessions_path = run_dir / "silver_sessions"
    silver_conversions_path = run_dir / "silver_conversions"
    quarantine_path = run_dir / "quarantine"
    db_path = run_dir / "serving.duckdb"

    generator = ClickstreamGenerator(seed=42)
    events = generator.generate_batch(num_sessions=num_sessions, failure_scenario=failure_scenario)
    generator.export_to_jsonl(events, raw_path)

    sessionizer = EventSessionizer(spark=spark)
    sessionizer.process_raw_events(
        raw_events_path=raw_path,
        bronze_output_path=bronze_path,
        silver_sessions_path=silver_sessions_path,
        silver_conversions_path=silver_conversions_path,
        quarantine_output_path=quarantine_path
    )

    serving = ServingLayerManager(db_path=db_path)
    serving.load_from_lake(
        bronze_events_path=bronze_path,
        silver_sessions_path=silver_sessions_path,
        silver_conversions_path=silver_conversions_path
    )

    return serving, quarantine_path


def test_quality_engine_clean_data(spark):
    run_dir = TEST_BASE / "clean_run"
    serving, quarantine_path = run_pipeline(spark, FailureScenario.NONE, run_dir, num_sessions=100)

    engine = SiteQualityEngine(conn=serving.conn)
    result = engine.evaluate_quality(quarantine_path=quarantine_path)

    # Clean data should be HEALTHY with high scores
    assert result.status == "HEALTHY"
    assert result.overall_score >= 90.0
    assert result.schema_score == 100.0
    assert result.schema_violations_count == 0
    assert result.completeness_score >= 95.0

    serving.close()
    shutil.rmtree(run_dir, ignore_errors=True)


def test_quality_engine_detects_schema_drift(spark):
    run_dir = TEST_BASE / "drift_run"
    serving, quarantine_path = run_pipeline(spark, FailureScenario.FAIL_SCHEMA_DRIFT, run_dir, num_sessions=150)

    engine = SiteQualityEngine(conn=serving.conn)
    result = engine.evaluate_quality(quarantine_path=quarantine_path)

    # Schema drift must flag CRITICAL status and drop schema_score to 0
    assert result.status == "CRITICAL"
    assert result.schema_score == 0.0
    assert result.schema_violations_count > 0

    # Must generate a SCHEMA_CONTRACT_VIOLATION incident
    incident_types = [inc["failure_type"] for inc in result.incidents_generated]
    assert "SCHEMA_CONTRACT_VIOLATION" in incident_types

    serving.close()
    shutil.rmtree(run_dir, ignore_errors=True)


def test_quality_engine_detects_null_explosion(spark):
    run_dir = TEST_BASE / "null_run"
    serving, quarantine_path = run_pipeline(spark, FailureScenario.FAIL_NULL_EXPLOSION, run_dir, num_sessions=80)

    engine = SiteQualityEngine(conn=serving.conn)
    result = engine.evaluate_quality(quarantine_path=quarantine_path)

    # Null explosion degrades completeness score and flags incident
    assert result.null_rate_pct > 2.0
    assert result.completeness_score < 90.0
    
    incident_types = [inc["failure_type"] for inc in result.incidents_generated]
    assert "NULL_RATE_SPIKE" in incident_types

    serving.close()
    shutil.rmtree(run_dir, ignore_errors=True)
