"""Tests for SitePulse Clickstream Event Generator and Schema."""
from sitepulse.generator.event_schema import (
    EventName, VALID_EVENT_NAMES, REQUIRED_EVENT_FIELDS
)
from sitepulse.generator.event_stream import (
    ClickstreamGenerator, FailureScenario
)


def test_generator_clean_batch():
    generator = ClickstreamGenerator(seed=123)
    events = generator.generate_batch(num_sessions=10, failure_scenario=FailureScenario.NONE)
    assert len(events) > 0

    for ev in events:
        # Check required fields
        for field in REQUIRED_EVENT_FIELDS:
            assert field in ev, f"Missing required field {field}"
        assert ev["event_name"] in VALID_EVENT_NAMES
        assert ev["device"]["device_type"] in ["desktop", "mobile", "tablet"]


def test_generator_schema_drift():
    generator = ClickstreamGenerator(seed=42)
    # Generate sufficient sessions to ensure purchases occur
    events = generator.generate_batch(num_sessions=150, failure_scenario=FailureScenario.FAIL_SCHEMA_DRIFT)
    
    # Verify that 'purchase_complete' was injected instead of 'purchase_completed'
    names = {ev["event_name"] for ev in events}
    assert "purchase_complete" in names, f"Expected 'purchase_complete' in event names, found {names}"
    assert "purchase_completed" not in names, "'purchase_completed' should have drifted to 'purchase_complete'"


def test_generator_duplicate_spike():
    generator = ClickstreamGenerator(seed=42)
    events = generator.generate_batch(num_sessions=30, failure_scenario=FailureScenario.FAIL_DUPLICATE_SPIKE)
    event_ids = [ev["event_id"] for ev in events]
    assert len(event_ids) > len(set(event_ids)), "Should have duplicate event IDs"


def test_generator_null_explosion():
    generator = ClickstreamGenerator(seed=42)
    events = generator.generate_batch(num_sessions=30, failure_scenario=FailureScenario.FAIL_NULL_EXPLOSION)
    null_visitors = [ev for ev in events if ev["visitor_id"] is None]
    assert len(null_visitors) > 0, "Should have injected null visitor IDs"
