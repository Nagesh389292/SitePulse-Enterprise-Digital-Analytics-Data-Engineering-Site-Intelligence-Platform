"""Tests for Apache Iceberg Manager and SitePulse Unified Orchestrator."""
from pathlib import Path
import tempfile
import pytest

from sitepulse.lakehouse.iceberg_manager import IcebergLakehouseManager
from sitepulse.orchestrator import SitePulseOrchestrator
from sitepulse.generator.event_stream import FailureScenario


from sitepulse.pyspark_engine.spark_session import get_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session("SitePulse-Orchestrator-Tests")
    yield session
    session.stop()


def test_iceberg_manager_lifecycle():
    with tempfile.TemporaryDirectory() as tmp_dir:
        lakehouse_dir = Path(tmp_dir) / "lakehouse"
        mgr = IcebergLakehouseManager(lakehouse_dir)

        # Verify tables initialized
        assert "bronze_events" in mgr._tables
        assert "silver_sessions" in mgr._tables
        assert "silver_conversions" in mgr._tables
        assert "quarantine_dlq" in mgr._tables

        # Commit snapshot
        snap1 = mgr.commit_snapshot("bronze_events", records_added=1000, files_added=1)
        assert snap1.snapshot_id.startswith("snap-")
        assert snap1.operation == "append"
        assert snap1.summary["added-records"] == "1000"

        # Commit second snapshot
        snap2 = mgr.commit_snapshot("bronze_events", records_added=500, files_added=1)
        assert snap2.parent_snapshot_id == snap1.snapshot_id
        assert snap2.summary["total-records"] == "1500"

        # Rollback test
        rollback_snap = mgr.rollback_to_snapshot("bronze_events", snap1.snapshot_id)
        assert rollback_snap.operation == "rollback"
        assert rollback_snap.summary["restored-to-snapshot"] == snap1.snapshot_id

        # Verify history
        history = mgr.get_table_history("bronze_events")
        assert len(history) == 3


def test_orchestrator_clean_run(spark):
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "serving.duckdb"
        lakehouse_dir = Path(tmp_dir) / "lakehouse"

        orch = SitePulseOrchestrator(db_path=db_path, lakehouse_dir=lakehouse_dir, spark=spark)
        res = orch.run_pipeline(num_sessions=100, scenario=None)

        assert res.events_generated > 0
        assert res.clean_events > 0
        assert res.sessions_built > 0
        assert res.structural_dq_score >= 85.0
        assert res.composite_reliability_score >= 70.0


def test_orchestrator_mobile_drop_scenario(spark):
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "serving.duckdb"
        lakehouse_dir = Path(tmp_dir) / "lakehouse"

        orch = SitePulseOrchestrator(db_path=db_path, lakehouse_dir=lakehouse_dir, spark=spark)
        res = orch.run_pipeline(num_sessions=100, scenario=FailureScenario.FAIL_MOBILE_DROP)

        assert res.events_generated > 0
        # Mobile drop has clean structural schema (zero nulls, clean contracts)
        assert res.structural_dq_score >= 85.0
        # But behavioral anomaly should be detected!
        assert res.composite_reliability_score < 90.0
        assert res.incident_id is not None
        assert res.incident_status == "ROOT_CAUSE_IDENTIFIED"
        assert res.revenue_exposure_usd >= 0.0
        assert res.root_cause_diagnosis is not None
