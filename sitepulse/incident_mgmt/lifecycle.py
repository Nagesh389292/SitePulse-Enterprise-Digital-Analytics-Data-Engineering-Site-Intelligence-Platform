"""Operational Incident Lifecycle Management.

Implements an enterprise ITIL-aligned Incident State Machine:
DETECTED -> TRIAGED -> INVESTIGATING -> ROOT_CAUSE_IDENTIFIED -> REMEDIATION -> VALIDATED -> CLOSED
Tracks ownership, SLA timers, impact assessments, and resolution notes.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from typing import Dict, Any, List, Optional
import uuid
import duckdb


VALID_TRANSITIONS = {
    "DETECTED": ["TRIAGED", "CLOSED"],
    "TRIAGED": ["INVESTIGATING", "CLOSED"],
    "INVESTIGATING": ["ROOT_CAUSE_IDENTIFIED", "TRIAGED"],
    "ROOT_CAUSE_IDENTIFIED": ["REMEDIATION", "INVESTIGATING"],
    "REMEDIATION": ["VALIDATED", "INVESTIGATING"],
    "VALIDATED": ["CLOSED", "REMEDIATION"],
    "CLOSED": ["DETECTED"]  # Reopen if recurring
}


@dataclass
class LifecycleIncident:
    incident_id: str
    detected_at: datetime
    status: str
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW
    affected_metric: str
    affected_segment: str
    root_cause_classification: str
    business_impact_usd: float
    owner_team: str
    assigned_engineer: str
    action_items: List[str] = field(default_factory=list)
    resolution_notes: Optional[str] = None
    updated_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None


def init_incident_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Ensure incident_lifecycle table exists."""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS incident_lifecycle (
        incident_id VARCHAR PRIMARY KEY,
        detected_at TIMESTAMP NOT NULL,
        status VARCHAR NOT NULL,
        severity VARCHAR NOT NULL,
        affected_metric VARCHAR NOT NULL,
        affected_segment VARCHAR NOT NULL,
        root_cause_classification VARCHAR,
        business_impact_usd DOUBLE,
        owner_team VARCHAR,
        assigned_engineer VARCHAR,
        action_items_json VARCHAR,
        resolution_notes VARCHAR,
        updated_at TIMESTAMP NOT NULL,
        resolved_at TIMESTAMP
    );
    """)


class IncidentLifecycleManager:
    """Manages operational telemetry and analytical incidents."""

    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        self.conn = db_conn
        init_incident_schema(self.conn)

    def create_incident(
        self,
        severity: str,
        affected_metric: str,
        affected_segment: str,
        root_cause_classification: str,
        business_impact_usd: float,
        owner_team: str = "Data Engineering & Content Ops",
        assigned_engineer: str = "On-Call Analytics Engineer",
        action_items: Optional[List[str]] = None,
        incident_id: Optional[str] = None
    ) -> LifecycleIncident:
        """Create a new incident in DETECTED status."""
        inc_id = incident_id or f"INC-{uuid.uuid4().hex[:8].upper()}"
        now = datetime.now(timezone.utc)
        actions = action_items or []

        self.conn.execute("""
            INSERT INTO incident_lifecycle (
                incident_id, detected_at, status, severity, affected_metric,
                affected_segment, root_cause_classification, business_impact_usd,
                owner_team, assigned_engineer, action_items_json, resolution_notes,
                updated_at, resolved_at
            )
            VALUES (?, ?, 'DETECTED', ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, NULL)
            ON CONFLICT (incident_id) DO NOTHING
        """, (
            inc_id, now, severity, affected_metric, affected_segment,
            root_cause_classification, business_impact_usd, owner_team,
            assigned_engineer, json.dumps(actions), now
        ))

        return LifecycleIncident(
            incident_id=inc_id,
            detected_at=now,
            status="DETECTED",
            severity=severity,
            affected_metric=affected_metric,
            affected_segment=affected_segment,
            root_cause_classification=root_cause_classification,
            business_impact_usd=business_impact_usd,
            owner_team=owner_team,
            assigned_engineer=assigned_engineer,
            action_items=actions,
            updated_at=now
        )

    def transition_status(
        self,
        incident_id: str,
        new_status: str,
        notes: Optional[str] = None,
        updated_by: Optional[str] = None
    ) -> LifecycleIncident:
        """Transition incident state adhering to ITIL lifecycle invariants."""
        current = self.get_incident(incident_id)
        if not current:
            raise ValueError(f"Incident '{incident_id}' not found.")

        valid_targets = VALID_TRANSITIONS.get(current.status, [])
        if new_status not in valid_targets:
            raise ValueError(
                f"Invalid transition from '{current.status}' to '{new_status}'. "
                f"Permitted transitions: {valid_targets}"
            )

        now = datetime.now(timezone.utc)
        resolved_at = now if new_status == "CLOSED" else current.resolved_at

        full_notes = (f"[{updated_by or 'System'}] {notes}") if notes else current.resolution_notes

        self.conn.execute("""
            UPDATE incident_lifecycle
            SET status = ?, resolution_notes = ?, updated_at = ?, resolved_at = ?
            WHERE incident_id = ?
        """, (new_status, full_notes, now, resolved_at, incident_id))

        return self.get_incident(incident_id)

    def get_incident(self, incident_id: str) -> Optional[LifecycleIncident]:
        """Fetch single incident by ID."""
        row = self.conn.execute("""
            SELECT incident_id, detected_at, status, severity, affected_metric,
                   affected_segment, root_cause_classification, business_impact_usd,
                   owner_team, assigned_engineer, action_items_json, resolution_notes,
                   updated_at, resolved_at
            FROM incident_lifecycle
            WHERE incident_id = ?
        """, (incident_id,)).fetchone()

        if not row:
            return None

        return LifecycleIncident(
            incident_id=row[0],
            detected_at=row[1],
            status=row[2],
            severity=row[3],
            affected_metric=row[4],
            affected_segment=row[5],
            root_cause_classification=row[6],
            business_impact_usd=row[7],
            owner_team=row[8],
            assigned_engineer=row[9],
            action_items=json.loads(row[10]) if row[10] else [],
            resolution_notes=row[11],
            updated_at=row[12],
            resolved_at=row[13]
        )

    def list_incidents(self, status: Optional[str] = None) -> List[LifecycleIncident]:
        """List all incidents, optionally filtered by status."""
        query = "SELECT incident_id FROM incident_lifecycle"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY detected_at DESC"

        rows = self.conn.execute(query, params).fetchall()
        return [self.get_incident(r[0]) for r in rows if r[0]]
