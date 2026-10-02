"""Synthetic Clickstream Event Generator with Deterministic Failure Injections."""
import uuid
import random
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional

from .event_schema import (
    ClickstreamEvent,
    Device,
    Page,
    Campaign,
    Ecommerce,
    EventName
)


class FailureScenario(str, Enum):
    NONE = "NONE"
    FAIL_SCHEMA_DRIFT = "FAIL_SCHEMA_DRIFT"          # Renames 'purchase_completed' -> 'purchase_complete'
    FAIL_MOBILE_DROP = "FAIL_MOBILE_DROP"            # Drops 85% of mobile conversions
    FAIL_DUPLICATE_SPIKE = "FAIL_DUPLICATE_SPIKE"    # 20% duplicate event IDs emitted
    FAIL_NULL_EXPLOSION = "FAIL_NULL_EXPLOSION"      # Spikes NULLs in visitor_id & device_type
    FAIL_LATE_ARRIVAL = "FAIL_LATE_ARRIVAL"          # Injects 14-hour late events
    FAIL_INVALID_REVENUE = "FAIL_INVALID_REVENUE"    # Negative or extreme revenue values


PAGES = [
    Page(url="https://store.brand.com/", path="/", title="Home Page", content_category="landing"),
    Page(url="https://store.brand.com/products/shoes", path="/products/shoes", title="Running Shoes", content_category="catalog"),
    Page(url="https://store.brand.com/products/jackets", path="/products/jackets", title="Winter Jackets", content_category="catalog"),
    Page(url="https://store.brand.com/products/accessories", path="/products/accessories", title="Accessories", content_category="catalog"),
    Page(url="https://store.brand.com/cart", path="/cart", title="Shopping Cart", content_category="checkout"),
    Page(url="https://store.brand.com/checkout", path="/checkout", title="Checkout Review", content_category="checkout"),
    Page(url="https://store.brand.com/order-success", path="/order-success", title="Order Confirmation", content_category="confirmation")
]

DEVICES = [
    Device(device_type="desktop", os="Windows", browser="Chrome"),
    Device(device_type="desktop", os="macOS", browser="Safari"),
    Device(device_type="mobile", os="iOS", browser="Safari"),
    Device(device_type="mobile", os="Android", browser="Chrome"),
    Device(device_type="tablet", os="iOS", browser="Safari"),
]

CAMPAIGNS = [
    Campaign(utm_source="google", utm_medium="cpc", utm_campaign="fall_sale_2026"),
    Campaign(utm_source="meta", utm_medium="social", utm_campaign="influencer_promo"),
    Campaign(utm_source="newsletter", utm_medium="email", utm_campaign="weekly_digest"),
    Campaign(utm_source="direct", utm_medium="none", utm_campaign="organic_brand"),
    None
]


class ClickstreamGenerator:
    """Generates realistic clickstream batches with configurable failure scenarios."""

    def __init__(self, seed: Optional[int] = 42):
        if seed is not None:
            random.seed(seed)

    def generate_visitor_session(
        self,
        visitor_id: str,
        start_time: datetime,
        failure_scenario: FailureScenario = FailureScenario.NONE
    ) -> List[Dict[str, Any]]:
        """Simulate a single user journey across multiple clicks/pages."""
        events: List[Dict[str, Any]] = []
        device = random.choice(DEVICES)
        campaign = random.choice(CAMPAIGNS)
        current_time = start_time

        # 1. Landing page view
        landing_page = PAGES[0]
        events.append(self._create_event_dict(
            event_name=EventName.PAGE_VIEW.value,
            timestamp=current_time,
            visitor_id=visitor_id,
            device=device,
            page=landing_page,
            campaign=campaign
        ))

        # Scroll on landing page
        current_time += timedelta(seconds=random.randint(5, 20))
        events.append(self._create_event_dict(
            event_name=EventName.SCROLL.value,
            timestamp=current_time,
            visitor_id=visitor_id,
            device=device,
            page=landing_page,
            campaign=campaign
        ))

        # 2. Browse Catalog
        catalog_page = random.choice(PAGES[1:4])
        current_time += timedelta(seconds=random.randint(10, 45))
        events.append(self._create_event_dict(
            event_name=EventName.PAGE_VIEW.value,
            timestamp=current_time,
            visitor_id=visitor_id,
            device=device,
            page=catalog_page,
            campaign=campaign
        ))

        # Click event on product
        current_time += timedelta(seconds=random.randint(8, 25))
        events.append(self._create_event_dict(
            event_name=EventName.CLICK.value,
            timestamp=current_time,
            visitor_id=visitor_id,
            device=device,
            page=catalog_page,
            campaign=campaign
        ))

        # 3. Add to Cart (45% probability)
        if random.random() < 0.45:
            current_time += timedelta(seconds=random.randint(15, 60))
            cart_page = PAGES[4]
            events.append(self._create_event_dict(
                event_name=EventName.ADD_TO_CART.value,
                timestamp=current_time,
                visitor_id=visitor_id,
                device=device,
                page=cart_page,
                campaign=campaign
            ))

            # 4. Checkout Start (50% of carts)
            if random.random() < 0.50:
                current_time += timedelta(seconds=random.randint(20, 80))
                checkout_page = PAGES[5]
                events.append(self._create_event_dict(
                    event_name=EventName.CHECKOUT_START.value,
                    timestamp=current_time,
                    visitor_id=visitor_id,
                    device=device,
                    page=checkout_page,
                    campaign=campaign
                ))

                # 5. Purchase Completed (60% of checkouts)
                if random.random() < 0.60:
                    current_time += timedelta(seconds=random.randint(30, 120))
                    success_page = PAGES[6]
                    order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
                    revenue = round(random.uniform(25.0, 350.0), 2)
                    items_count = random.randint(1, 4)

                    ecommerce = Ecommerce(
                        order_id=order_id,
                        currency="USD",
                        revenue=revenue,
                        items_count=items_count
                    )

                    purchase_event_name = EventName.PURCHASE_COMPLETED.value
                    
                    # Scenario: Schema Drift (renamed event tag)
                    if failure_scenario == FailureScenario.FAIL_SCHEMA_DRIFT:
                        purchase_event_name = "purchase_complete"

                    # Scenario: Mobile Drop (silent drop on mobile devices)
                    is_mobile = device.device_type == "mobile"
                    if failure_scenario == FailureScenario.FAIL_MOBILE_DROP and is_mobile and random.random() < 0.85:
                        pass  # Silently skip emitting purchase event for mobile!
                    else:
                        # Scenario: Invalid Revenue
                        if failure_scenario == FailureScenario.FAIL_INVALID_REVENUE:
                            revenue = -99.99 if random.random() < 0.5 else 999999.00
                            ecommerce.revenue = revenue

                        events.append(self._create_event_dict(
                            event_name=purchase_event_name,
                            timestamp=current_time,
                            visitor_id=visitor_id,
                            device=device,
                            page=success_page,
                            campaign=campaign,
                            ecommerce=ecommerce
                        ))

        # Apply global failure scenario transformations to events in this session
        final_events: List[Dict[str, Any]] = []
        for ev in events:
            # Failure: Duplicate Spike
            if failure_scenario == FailureScenario.FAIL_DUPLICATE_SPIKE and random.random() < 0.20:
                final_events.append(dict(ev))  # original
                dup = dict(ev)
                # emitted 150ms later with same event_id
                final_events.append(dup)
                continue

            # Failure: Null Explosion
            if failure_scenario == FailureScenario.FAIL_NULL_EXPLOSION and random.random() < 0.30:
                ev["visitor_id"] = None
                if "device" in ev and ev["device"] is not None:
                    ev["device"]["device_type"] = None

            # Failure: Late Arrival
            if failure_scenario == FailureScenario.FAIL_LATE_ARRIVAL and random.random() < 0.25:
                # 14 hours delayed offset in timestamp
                orig_dt = datetime.fromisoformat(ev["event_timestamp"])
                late_dt = orig_dt - timedelta(hours=14)
                ev["event_timestamp"] = late_dt.isoformat()

            final_events.append(ev)

        return final_events

    def generate_batch(
        self,
        num_sessions: int = 100,
        start_time: Optional[datetime] = None,
        failure_scenario: FailureScenario = FailureScenario.NONE
    ) -> List[Dict[str, Any]]:
        """Generate a complete batch of events across multiple sessions."""
        if start_time is None:
            start_time = datetime.now(timezone.utc)

        all_events: List[Dict[str, Any]] = []
        for i in range(num_sessions):
            visitor_id = f"vis_{uuid.uuid4().hex[:16]}"
            session_time = start_time + timedelta(seconds=random.randint(0, 3600))
            session_events = self.generate_visitor_session(
                visitor_id=visitor_id,
                start_time=session_time,
                failure_scenario=failure_scenario
            )
            all_events.extend(session_events)

        # Sort chronologically
        all_events.sort(key=lambda x: x["event_timestamp"] if x["event_timestamp"] else "")
        return all_events

    def export_to_jsonl(self, events: List[Dict[str, Any]], output_path: Path) -> Path:
        """Export batch of events to JSONL file."""
        import json
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            for ev in events:
                f.write(json.dumps(ev) + "\n")
        return output_path

    def _create_event_dict(
        self,
        event_name: str,
        timestamp: datetime,
        visitor_id: str,
        device: Device,
        page: Page,
        campaign: Optional[Campaign] = None,
        ecommerce: Optional[Ecommerce] = None
    ) -> Dict[str, Any]:
        """Construct raw dictionary conforming to schema."""
        ev = ClickstreamEvent(
            event_id=str(uuid.uuid4()),
            event_name=event_name,
            event_timestamp=timestamp.isoformat(),
            visitor_id=visitor_id,
            device=device,
            page=page,
            campaign=campaign,
            ecommerce=ecommerce
        )
        return ev.to_dict()
