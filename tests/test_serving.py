"""Tests for DuckDB Analytical Serving Layer and Star Schema Marts."""
import shutil
from pathlib import Path
import pytest
import duckdb

from sitepulse.generator.event_stream import ClickstreamGenerator, FailureScenario
from sitepulse.pyspark_engine.spark_session import get_spark_session
from sitepulse.pyspark_engine.sessionizer import EventSessionizer
from sitepulse.serving.star_schema import ServingLayerManager

TEST_BASE = Path(__file__).resolve().parent.parent / "data" / "test_serving_tmp"


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("SitePulse-Serving-Tests")
    yield session
    session.stop()


@pytest.fixture
def lake_data(spark):
    if TEST_BASE.exists():
        shutil.rmtree(TEST_BASE, ignore_errors=True)
    TEST_BASE.mkdir(parents=True, exist_ok=True)

    raw_path = TEST_BASE / "raw.jsonl"
    bronze_path = TEST_BASE / "bronze"
    silver_sessions_path = TEST_BASE / "silver_sessions"
    silver_conversions_path = TEST_BASE / "silver_conversions"
    quarantine_path = TEST_BASE / "quarantine"
    db_path = TEST_BASE / "serving.duckdb"

    # Generate 50 sessions
    generator = ClickstreamGenerator(seed=123)
    events = generator.generate_batch(num_sessions=50, failure_scenario=FailureScenario.NONE)
    generator.export_to_jsonl(events, raw_path)

    # Run PySpark Sessionizer
    sessionizer = EventSessionizer(spark=spark)
    sessionizer.process_raw_events(
        raw_events_path=raw_path,
        bronze_output_path=bronze_path,
        silver_sessions_path=silver_sessions_path,
        silver_conversions_path=silver_conversions_path,
        quarantine_output_path=quarantine_path
    )

    yield {
        "bronze": bronze_path,
        "silver_sessions": silver_sessions_path,
        "silver_conversions": silver_conversions_path,
        "quarantine": quarantine_path,
        "db_path": db_path
    }

    if TEST_BASE.exists():
        shutil.rmtree(TEST_BASE, ignore_errors=True)


def test_serving_layer_load(lake_data):
    manager = ServingLayerManager(db_path=lake_data["db_path"])
    counts = manager.load_from_lake(
        bronze_events_path=lake_data["bronze"],
        silver_sessions_path=lake_data["silver_sessions"],
        silver_conversions_path=lake_data["silver_conversions"]
    )

    # 1. Assert dimensions are populated
    assert counts["dim_page"] > 0
    assert counts["dim_device"] > 0
    assert counts["dim_campaign"] > 0
    assert counts["fact_sessions"] >= 50

    # 2. Test star-schema joins
    conn = manager.conn
    join_query = """
    SELECT
        d.device_type,
        count(s.session_sk) AS session_count,
        sum(s.has_purchase) AS purchases
    FROM fact_sessions s
    JOIN dim_device d ON s.device_sk = d.device_sk
    GROUP BY d.device_type
    """
    results = conn.execute(join_query).fetchall()
    assert len(results) > 0

    # 3. Verify surrogate key uniqueness
    dup_sk = conn.execute("""
        SELECT session_sk, count(*)
        FROM fact_sessions
        GROUP BY session_sk
        HAVING count(*) > 1
    """).fetchall()
    assert len(dup_sk) == 0

    manager.close()
