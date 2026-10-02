import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_EVENTS_DIR = DATA_DIR / "raw_events"
BRONZE_DIR = DATA_DIR / "bronze_lake"
SILVER_DIR = DATA_DIR / "silver_lake"
GOLD_DIR = DATA_DIR / "gold_marts"
QUARANTINE_DIR = DATA_DIR / "quarantine"
SERVING_DB_PATH = DATA_DIR / "serving_db.duckdb"

# Create directories if they do not exist
for directory in [DATA_DIR, RAW_EVENTS_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, QUARANTINE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Quality Health Weights
WEIGHT_FRESHNESS = 0.20
WEIGHT_COMPLETENESS = 0.25
WEIGHT_SCHEMA = 0.25
WEIGHT_VOLUME = 0.15
WEIGHT_REFERENTIAL = 0.15

# Sessionization timeout in seconds
SESSION_TIMEOUT_SECONDS = 1800  # 30 minutes
