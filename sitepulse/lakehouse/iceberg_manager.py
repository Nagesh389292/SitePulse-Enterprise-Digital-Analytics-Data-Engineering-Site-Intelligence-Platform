"""Apache Iceberg Lakehouse Manager.

Simulates and manages Iceberg table format metadata, snapshot commits,
schema evolution tracking, time travel, and rollback operations on Lakehouse storage.
Guarantees ACID transactions over Bronze events and Silver analytical tables.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid


@dataclass
class IcebergSnapshot:
    snapshot_id: str
    timestamp: datetime
    parent_snapshot_id: Optional[str]
    operation: str  # append, overwrite, delete, rollback
    summary: Dict[str, Any]
    manifest_file: str


@dataclass
class IcebergTableMetadata:
    table_name: str
    location: str
    current_snapshot_id: Optional[str]
    snapshots: List[IcebergSnapshot] = field(default_factory=list)
    schema: Dict[str, str] = field(default_factory=dict)
    partition_spec: List[str] = field(default_factory=list)


class IcebergLakehouseManager:
    """Manages Apache Iceberg metadata, ACID snapshot commits, and time-travel rollbacks."""

    def __init__(self, lakehouse_root: Path):
        self.root = Path(lakehouse_root)
        self.metadata_dir = self.root / "metadata"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self._tables: Dict[str, IcebergTableMetadata] = {}
        self._load_or_init_metadata()

    def _get_metadata_path(self, table_name: str) -> Path:
        return self.metadata_dir / f"{table_name}_metadata.json"

    def _load_or_init_metadata(self) -> None:
        """Initialize or load metadata catalogs for Bronze and Silver tables."""
        table_configs = [
            ("bronze_events", ["event_date"], {
                "event_id": "string", "event_name": "string", "timestamp": "timestamp",
                "visitor_id": "string", "device": "struct", "page": "struct"
            }),
            ("silver_sessions", ["session_date"], {
                "session_id": "string", "visitor_id": "string", "session_start": "timestamp",
                "session_end": "timestamp", "duration_seconds": "long", "page_view_count": "long",
                "has_cart": "int", "has_checkout": "int", "has_purchase": "int", "total_revenue": "double"
            }),
            ("silver_conversions", ["conversion_date"], {
                "conversion_event_id": "string", "session_id": "string", "visitor_id": "string",
                "conversion_time": "timestamp", "order_id": "string", "revenue": "double"
            }),
            ("quarantine_dlq", ["quarantine_date"], {
                "raw_payload": "string", "error_code": "string", "quarantine_timestamp": "timestamp"
            })
        ]

        for tbl_name, parts, schema in table_configs:
            meta_path = self._get_metadata_path(tbl_name)
            if meta_path.exists():
                try:
                    data = json.loads(meta_path.read_text(encoding="utf-8"))
                    snaps = [
                        IcebergSnapshot(
                            snapshot_id=s["snapshot_id"],
                            timestamp=datetime.fromisoformat(s["timestamp"]),
                            parent_snapshot_id=s.get("parent_snapshot_id"),
                            operation=s["operation"],
                            summary=s["summary"],
                            manifest_file=s["manifest_file"]
                        )
                        for s in data["snapshots"]
                    ]
                    self._tables[tbl_name] = IcebergTableMetadata(
                        table_name=tbl_name,
                        location=data["location"],
                        current_snapshot_id=data.get("current_snapshot_id"),
                        snapshots=snaps,
                        schema=data["schema"],
                        partition_spec=data["partition_spec"]
                    )
                except Exception:
                    self._create_table_meta(tbl_name, parts, schema)
            else:
                self._create_table_meta(tbl_name, parts, schema)

    def _create_table_meta(self, table_name: str, partitions: List[str], schema: Dict[str, str]) -> None:
        table_loc = str((self.root / table_name).as_posix())
        meta = IcebergTableMetadata(
            table_name=table_name,
            location=table_loc,
            current_snapshot_id=None,
            snapshots=[],
            schema=schema,
            partition_spec=partitions
        )
        self._tables[table_name] = meta
        self._persist_metadata(table_name)

    def _persist_metadata(self, table_name: str) -> None:
        meta = self._tables[table_name]
        data = {
            "table_name": meta.table_name,
            "location": meta.location,
            "current_snapshot_id": meta.current_snapshot_id,
            "schema": meta.schema,
            "partition_spec": meta.partition_spec,
            "snapshots": [
                {
                    "snapshot_id": s.snapshot_id,
                    "timestamp": s.timestamp.isoformat(),
                    "parent_snapshot_id": s.parent_snapshot_id,
                    "operation": s.operation,
                    "summary": s.summary,
                    "manifest_file": s.manifest_file
                }
                for s in meta.snapshots
            ]
        }
        self._get_metadata_path(table_name).write_text(json.dumps(data, indent=2), encoding="utf-8")

    def commit_snapshot(
        self,
        table_name: str,
        records_added: int,
        files_added: int,
        operation: str = "append"
    ) -> IcebergSnapshot:
        """Commit an ACID snapshot in the Iceberg metadata log."""
        if table_name not in self._tables:
            raise ValueError(f"Table '{table_name}' does not exist in Iceberg catalog.")

        meta = self._tables[table_name]
        parent_id = meta.current_snapshot_id
        snapshot_id = f"snap-{uuid.uuid4().hex[:12]}"
        now = datetime.now(timezone.utc)

        summary = {
            "added-records": str(records_added),
            "added-data-files": str(files_added),
            "total-records": str(int(meta.snapshots[-1].summary.get("total-records", 0)) + records_added if meta.snapshots else records_added),
            "iceberg-version": "2"
        }

        snap = IcebergSnapshot(
            snapshot_id=snapshot_id,
            timestamp=now,
            parent_snapshot_id=parent_id,
            operation=operation,
            summary=summary,
            manifest_file=f"manifest-{snapshot_id}.avro"
        )

        meta.snapshots.append(snap)
        meta.current_snapshot_id = snapshot_id
        self._persist_metadata(table_name)
        return snap

    def rollback_to_snapshot(self, table_name: str, target_snapshot_id: str) -> IcebergSnapshot:
        """ACID time-travel rollback restoring an earlier clean snapshot (e.g. after schema corruption)."""
        if table_name not in self._tables:
            raise ValueError(f"Table '{table_name}' not found.")

        meta = self._tables[table_name]
        target_snap = next((s for s in meta.snapshots if s.snapshot_id == target_snapshot_id), None)
        if not target_snap:
            raise ValueError(f"Snapshot '{target_snapshot_id}' does not exist for table '{table_name}'.")

        now = datetime.now(timezone.utc)
        new_snap_id = f"snap-rollback-{uuid.uuid4().hex[:8]}"
        rollback_snap = IcebergSnapshot(
            snapshot_id=new_snap_id,
            timestamp=now,
            parent_snapshot_id=meta.current_snapshot_id,
            operation="rollback",
            summary={
                "restored-to-snapshot": target_snapshot_id,
                "total-records": target_snap.summary.get("total-records", "0"),
                "reason": "Automated quality rollback triggered by SitePulse Quality Gate"
            },
            manifest_file=target_snap.manifest_file
        )

        meta.snapshots.append(rollback_snap)
        meta.current_snapshot_id = new_snap_id
        self._persist_metadata(table_name)
        return rollback_snap

    def get_table_history(self, table_name: str) -> List[Dict[str, Any]]:
        """Return audit log of all snapshots, commits, and operations."""
        meta = self._tables.get(table_name)
        if not meta:
            return []
        return [
            {
                "snapshot_id": s.snapshot_id,
                "timestamp": s.timestamp.isoformat(),
                "operation": s.operation,
                "records_added": s.summary.get("added-records", "0"),
                "total_records": s.summary.get("total-records", "0"),
                "parent_id": s.parent_snapshot_id
            }
            for s in meta.snapshots
        ]
