"""Site Quality Engine: Mathematical Health Scoring and Anomaly Detection."""
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Any
import json
import uuid
import duckdb

from sitepulse.config import (
    WEIGHT_FRESHNESS,
    WEIGHT_COMPLETENESS,
    WEIGHT_SCHEMA,
    WEIGHT_VOLUME,
    WEIGHT_REFERENTIAL
)
from sitepulse.serving.star_schema import get_serving_connection


@dataclass
class QualityScoreResult:
    evaluation_date: str
    overall_score: float
    freshness_score: float
    completeness_score: float
    schema_score: float
    volume_score: float
    referential_score: float
    status: str  # HEALTHY, DEGRADED, CRITICAL
    latency_minutes: float
    volume_z_score: float
    null_rate_pct: float
    schema_violations_count: int
    incidents_generated: List[Dict[str, Any]]


class SiteQualityEngine:
    """Evaluates data reliability across 5 weighted dimensions and generates operational alerts."""

    def __init__(self, conn: Optional[duckdb.DuckDBPyConnection] = None):
        self.conn = conn or get_serving_connection()

    def evaluate_quality(
        self,
        quarantine_path: Optional[Path] = None,
        evaluation_time: Optional[datetime] = None
    ) -> QualityScoreResult:
        """Execute full mathematical quality scoring and anomaly diagnosis."""
        eval_dt = evaluation_time or datetime.now(timezone.utc)
        eval_date_str = eval_dt.date().isoformat()
        incidents: List[Dict[str, Any]] = []

        # -------------------------------------------------------------
        # 1. Freshness Dimension (Weight: 20%)
        # -------------------------------------------------------------
        latest_session_row = self.conn.execute("""
            SELECT max(session_end) FROM fact_sessions
        """).fetchone()
        
        latest_ts = latest_session_row[0] if latest_session_row and latest_session_row[0] else None
        
        if latest_ts:
            if isinstance(latest_ts, str):
                latest_dt = datetime.fromisoformat(latest_ts)
            else:
                latest_dt = latest_ts
            if latest_dt.tzinfo is None:
                latest_dt = latest_dt.replace(tzinfo=timezone.utc)
            latency_minutes = max(0.0, (eval_dt - latest_dt).total_seconds() / 60.0)
        else:
            latency_minutes = 0.0

        if latency_minutes <= 30.0:
            freshness_score = 100.0
        else:
            delay_blocks = (latency_minutes - 30.0) / 15.0
            freshness_score = max(0.0, round(100.0 - (10.0 * delay_blocks), 2))

        if latency_minutes > 120.0:
            incidents.append(self._create_incident_dict(
                severity="DEGRADED",
                failure_type="PIPELINE_LATENCY_DELAY",
                metric_affected="Freshness",
                description=f"Pipeline data latency is {latency_minutes:.1f} minutes, exceeding SLA of 30 minutes.",
                details={"latency_minutes": latency_minutes}
            ))

        # -------------------------------------------------------------
        # 2. Inspect Quarantine Lakehouse Partition
        # -------------------------------------------------------------
        schema_violations = 0
        missing_payload_violations = 0
        quarantine_breakdown = {}

        if quarantine_path and quarantine_path.exists():
            clean_q_path = str(quarantine_path).replace("\\", "/") + "/*.parquet"
            try:
                file_count = self.conn.execute(f"SELECT count(*) FROM glob('{clean_q_path}')").fetchone()[0]
                if file_count > 0:
                    q_rows = self.conn.execute(f"""
                        SELECT quarantine_reason, count(*)
                        FROM read_parquet('{clean_q_path}')
                        GROUP BY quarantine_reason
                    """).fetchall()
                    quarantine_breakdown = {row[0]: row[1] for row in q_rows}
                    schema_violations = quarantine_breakdown.get("ERR_SCHEMA_DRIFT_UNKNOWN_EVENT", 0)
                    missing_payload_violations = (
                        quarantine_breakdown.get("ERR_MISSING_VISITOR_ID", 0) +
                        quarantine_breakdown.get("ERR_MISSING_DEVICE_PAYLOAD", 0) +
                        quarantine_breakdown.get("ERR_MISSING_PAGE_PAYLOAD", 0)
                    )
            except Exception:
                schema_violations = 0
                missing_payload_violations = 0

        # -------------------------------------------------------------
        # 3. Completeness Dimension (Weight: 25%)
        # -------------------------------------------------------------
        completeness_row = self.conn.execute("""
            SELECT
                count(*) AS total_rows,
                count(visitor_id) AS visitors,
                count(session_start) AS starts,
                count(device_sk) AS devices
            FROM fact_sessions
        """).fetchone()

        total_sessions = completeness_row[0] if completeness_row else 0
        total_eval_points = (total_sessions * 3) + missing_payload_violations
        
        if total_eval_points > 0:
            null_count = missing_payload_violations
            if completeness_row:
                valid_fields = completeness_row[1] + completeness_row[2] + completeness_row[3]
                null_count += ((total_sessions * 3) - valid_fields)
            
            null_rate = null_count / float(total_eval_points)
            null_rate_pct = round(null_rate * 100.0, 2)
            # Threshold: > 5% null rate degrades score to 0
            completeness_score = max(0.0, round(100.0 * (1.0 - (null_rate / 0.05)), 2))
        else:
            null_rate_pct = 0.0
            completeness_score = 100.0

        if null_rate_pct > 2.0:
            incidents.append(self._create_incident_dict(
                severity="DEGRADED" if null_rate_pct < 10.0 else "CRITICAL",
                failure_type="NULL_RATE_SPIKE",
                metric_affected="Completeness",
                description=f"High null/missing payload rate detected: {null_rate_pct:.1f}% unparseable or null fields.",
                details={"null_rate_pct": null_rate_pct, "quarantine_nulls": missing_payload_violations}
            ))

        # -------------------------------------------------------------
        # 4. Schema Stability Dimension (Weight: 25%)
        # -------------------------------------------------------------
        if schema_violations > 0:
            schema_score = 0.0
            incidents.append(self._create_incident_dict(
                severity="CRITICAL",
                failure_type="SCHEMA_CONTRACT_VIOLATION",
                metric_affected="Schema Stability",
                description=f"Detected {schema_violations} schema violations diverted to Quarantine DLQ.",
                details=quarantine_breakdown
            ))
        else:
            schema_score = 100.0

        # -------------------------------------------------------------
        # 5. Volume Stability Dimension (Weight: 15%)
        # -------------------------------------------------------------
        baseline_mean = 100.0
        baseline_std = 15.0
        current_volume = float(total_sessions)
        volume_z_score = round((current_volume - baseline_mean) / baseline_std, 2)

        abs_z = abs(volume_z_score)
        if abs_z <= 2.0:
            volume_score = 100.0
        else:
            volume_score = max(0.0, round(100.0 - 25.0 * (abs_z - 2.0), 2))

        if abs_z >= 2.5:
            severity = "CRITICAL" if abs_z >= 3.5 else "DEGRADED"
            incidents.append(self._create_incident_dict(
                severity=severity,
                failure_type="TRAFFIC_VOLUME_ANOMALY",
                metric_affected="Volume Stability",
                description=f"Traffic volume abnormal: Z-score is {volume_z_score:.2f} (Observed: {current_volume:.0f}, Expected: {baseline_mean:.0f}).",
                details={"observed_volume": current_volume, "z_score": volume_z_score}
            ))

        # -------------------------------------------------------------
        # 6. Referential Integrity Dimension (Weight: 15%)
        # -------------------------------------------------------------
        orphan_check_row = self.conn.execute("""
            SELECT
                count(*) AS total_conversions,
                count(c.session_sk) FILTER (WHERE s.session_sk IS NULL) AS orphan_conversions
            FROM fact_conversions c
            LEFT JOIN fact_sessions s ON c.session_sk = s.session_sk
        """).fetchone()

        total_conversions = orphan_check_row[0] if orphan_check_row else 0
        orphan_count = orphan_check_row[1] if orphan_check_row else 0

        if total_conversions > 0:
            orphan_ratio = orphan_count / float(total_conversions)
            referential_score = max(0.0, round(100.0 * (1.0 - orphan_ratio), 2))
        else:
            referential_score = 100.0

        if orphan_count > 0:
            incidents.append(self._create_incident_dict(
                severity="CRITICAL",
                failure_type="REFERENTIAL_INTEGRITY_ORPHAN",
                metric_affected="Referential Integrity",
                description=f"Detected {orphan_count} orphaned conversions with non-existent session records.",
                details={"orphan_count": orphan_count}
            ))

        # -------------------------------------------------------------
        # Overall Weighted Health Score Formulation
        # -------------------------------------------------------------
        overall_score = round(
            (WEIGHT_FRESHNESS * freshness_score) +
            (WEIGHT_COMPLETENESS * completeness_score) +
            (WEIGHT_SCHEMA * schema_score) +
            (WEIGHT_VOLUME * volume_score) +
            (WEIGHT_REFERENTIAL * referential_score),
            2
        )

        if schema_violations > 0 or overall_score < 70.0:
            status = "CRITICAL"
        elif overall_score < 90.0 or abs_z >= 2.5:
            status = "DEGRADED"
        else:
            status = "HEALTHY"

        # -------------------------------------------------------------
        # Persist into Serving DB
        # -------------------------------------------------------------
        self.conn.execute("""
        INSERT INTO quality_scores_daily (
            evaluation_date, overall_score, freshness_score, completeness_score,
            schema_score, volume_score, referential_score, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (evaluation_date) DO UPDATE SET
            overall_score = EXCLUDED.overall_score,
            freshness_score = EXCLUDED.freshness_score,
            completeness_score = EXCLUDED.completeness_score,
            schema_score = EXCLUDED.schema_score,
            volume_score = EXCLUDED.volume_score,
            referential_score = EXCLUDED.referential_score,
            status = EXCLUDED.status;
        """, [
            eval_date_str, overall_score, freshness_score, completeness_score,
            schema_score, volume_score, referential_score, status
        ])

        for inc in incidents:
            self.conn.execute("""
            INSERT INTO quality_incidents (
                incident_id, detected_at, severity, failure_type, metric_affected, description, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (incident_id) DO NOTHING;
            """, [
                inc["incident_id"], inc["detected_at"], inc["severity"],
                inc["failure_type"], inc["metric_affected"], inc["description"],
                inc["details_json"]
            ])

        return QualityScoreResult(
            evaluation_date=eval_date_str,
            overall_score=overall_score,
            freshness_score=freshness_score,
            completeness_score=completeness_score,
            schema_score=schema_score,
            volume_score=volume_score,
            referential_score=referential_score,
            status=status,
            latency_minutes=latency_minutes,
            volume_z_score=volume_z_score,
            null_rate_pct=null_rate_pct,
            schema_violations_count=schema_violations,
            incidents_generated=incidents
        )

    def _create_incident_dict(
        self,
        severity: str,
        failure_type: str,
        metric_affected: str,
        description: str,
        details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Format structured incident entry."""
        return {
            "incident_id": f"INC-{uuid.uuid4().hex[:8].upper()}",
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "severity": severity,
            "failure_type": failure_type,
            "metric_affected": metric_affected,
            "description": description,
            "details_json": json.dumps(details)
        }
