"""Integration tests for PySpark Ingestion, Deduplication, and Sessionization."""
import shutil
from pathlib import Path
import pytest

from sitepulse.generator.event_stream import ClickstreamGenerator, FailureScenario
from sitepulse.pyspark_engine.spark_session import get_spark_session
from sitepulse.pyspark_engine.sessionizer import EventSessionizer

TEST_BASE = Path(__file__).resolve().parent.parent / "data" / "test_tmp"


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("SitePulse-PySpark-Tests")
    yield session
    session.stop()


@pytest.fixture
def temp_dirs():
    if TEST_BASE.exists():
        shutil.rmtree(TEST_BASE, ignore_errors=True)
    TEST_BASE.mkdir(parents=True, exist_ok=True)

    raw_path = TEST_BASE / "raw.jsonl"
    bronze_path = TEST_BASE / "bronze"
    silver_sessions_path = TEST_BASE / "silver_sessions"
    silver_conversions_path = TEST_BASE / "silver_conversions"
    quarantine_path = TEST_BASE / "quarantine"

    yield {
        "raw": raw_path,
        "bronze": bronze_path,
        "silver_sessions": silver_sessions_path,
        "silver_conversions": silver_conversions_path,
        "quarantine": quarantine_path
    }

    if TEST_BASE.exists():
        shutil.rmtree(TEST_BASE, ignore_errors=True)


def test_sessionizer_clean_pipeline(spark, temp_dirs):
    # 1. Generate clean batch
    generator = ClickstreamGenerator(seed=99)
    events = generator.generate_batch(num_sessions=20, failure_scenario=FailureScenario.NONE)
    generator.export_to_jsonl(events, temp_dirs["raw"])

    # 2. Run sessionizer
    sessionizer = EventSessionizer(spark=spark)
    result = sessionizer.process_raw_events(
        raw_events_path=temp_dirs["raw"],
        bronze_output_path=temp_dirs["bronze"],
        silver_sessions_path=temp_dirs["silver_sessions"],
        silver_conversions_path=temp_dirs["silver_conversions"],
        quarantine_output_path=temp_dirs["quarantine"]
    )

    # 3. Assertions
    assert result.total_raw_records == len(events)
    assert result.clean_records == len(events)
    assert result.quarantined_records == 0
    assert result.deduplicated_records == len(events)
    assert result.total_sessions >= 20
    assert temp_dirs["bronze"].exists()
    assert temp_dirs["silver_sessions"].exists()


def test_sessionizer_handles_duplicates(spark, temp_dirs):
    # Generate batch with duplicate event IDs
    generator = ClickstreamGenerator(seed=101)
    events = generator.generate_batch(num_sessions=20, failure_scenario=FailureScenario.FAIL_DUPLICATE_SPIKE)
    generator.export_to_jsonl(events, temp_dirs["raw"])

    sessionizer = EventSessionizer(spark=spark)
    result = sessionizer.process_raw_events(
        raw_events_path=temp_dirs["raw"],
        bronze_output_path=temp_dirs["bronze"],
        silver_sessions_path=temp_dirs["silver_sessions"],
        silver_conversions_path=temp_dirs["silver_conversions"],
        quarantine_output_path=temp_dirs["quarantine"]
    )

    # Deduplicated records should be strictly less than total raw records
    assert result.deduplicated_records < result.total_raw_records
    assert result.clean_records == result.total_raw_records


def test_sessionizer_quarantines_schema_drift(spark, temp_dirs):
    # Generate batch with schema drift (purchase_complete unknown event)
    generator = ClickstreamGenerator(seed=77)
    events = generator.generate_batch(num_sessions=150, failure_scenario=FailureScenario.FAIL_SCHEMA_DRIFT)
    generator.export_to_jsonl(events, temp_dirs["raw"])

    sessionizer = EventSessionizer(spark=spark)
    result = sessionizer.process_raw_events(
        raw_events_path=temp_dirs["raw"],
        bronze_output_path=temp_dirs["bronze"],
        silver_sessions_path=temp_dirs["silver_sessions"],
        silver_conversions_path=temp_dirs["silver_conversions"],
        quarantine_output_path=temp_dirs["quarantine"]
    )

    # Should have quarantined records
    assert result.quarantined_records > 0
    assert "ERR_SCHEMA_DRIFT_UNKNOWN_EVENT" in result.quarantine_breakdown
