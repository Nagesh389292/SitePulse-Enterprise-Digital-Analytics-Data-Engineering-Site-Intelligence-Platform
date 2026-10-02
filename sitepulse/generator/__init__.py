"""Clickstream Event Generator and Schema Contracts."""
from .event_schema import (
    ClickstreamEvent,
    Device,
    Page,
    Campaign,
    Ecommerce,
    EventName,
    VALID_EVENT_NAMES,
    REQUIRED_EVENT_FIELDS
)
from .event_stream import ClickstreamGenerator, FailureScenario

__all__ = [
    "ClickstreamEvent",
    "Device",
    "Page",
    "Campaign",
    "Ecommerce",
    "EventName",
    "VALID_EVENT_NAMES",
    "REQUIRED_EVENT_FIELDS",
    "ClickstreamGenerator",
    "FailureScenario"
]
