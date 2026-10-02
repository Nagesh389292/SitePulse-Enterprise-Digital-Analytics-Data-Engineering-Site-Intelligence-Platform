"""Event Schema Contracts, Enums, and PySpark Schemas for SitePulse."""
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, Dict, Any
import json

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, IntegerType
)


class EventName(str, Enum):
    PAGE_VIEW = "page_view"
    SCROLL = "scroll"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    CHECKOUT_START = "checkout_start"
    PURCHASE_COMPLETED = "purchase_completed"
    CONTENT_SHARE = "content_share"


VALID_EVENT_NAMES = {e.value for e in EventName}

REQUIRED_EVENT_FIELDS = {
    "event_id",
    "event_name",
    "event_timestamp",
    "visitor_id",
    "device",
    "page",
    "schema_version"
}

# Strict PySpark schema ensuring struct types for nested fields
CLICKSTREAM_SPARK_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("visitor_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device", StructType([
        StructField("device_type", StringType(), True),
        StructField("os", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("user_agent", StringType(), True)
    ]), True),
    StructField("page", StructType([
        StructField("url", StringType(), True),
        StructField("path", StringType(), True),
        StructField("title", StringType(), True),
        StructField("content_category", StringType(), True)
    ]), True),
    StructField("campaign", StructType([
        StructField("utm_source", StringType(), True),
        StructField("utm_medium", StringType(), True),
        StructField("utm_campaign", StringType(), True)
    ]), True),
    StructField("ecommerce", StructType([
        StructField("order_id", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("items_count", IntegerType(), True)
    ]), True)
])


@dataclass
class Device:
    device_type: str  # desktop, mobile, tablet
    os: str          # iOS, Android, Windows, macOS
    browser: str     # Chrome, Safari, Firefox, Edge
    user_agent: Optional[str] = None


@dataclass
class Page:
    url: str
    path: str
    title: str
    content_category: Optional[str] = None


@dataclass
class Campaign:
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None


@dataclass
class Ecommerce:
    order_id: Optional[str] = None
    currency: str = "USD"
    revenue: float = 0.0
    items_count: int = 0


@dataclass
class ClickstreamEvent:
    event_id: str
    event_name: str
    event_timestamp: str  # ISO-8601 string
    visitor_id: str
    device: Device
    page: Page
    schema_version: str = "1.2.0"
    session_id: Optional[str] = None
    campaign: Optional[Campaign] = None
    ecommerce: Optional[Ecommerce] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
