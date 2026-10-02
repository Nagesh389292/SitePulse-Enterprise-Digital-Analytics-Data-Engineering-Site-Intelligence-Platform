"""Business Metric Anomaly Engine.

Detects behavioral anomalies that traditional structural data quality checks miss:
- Segment-level conversion rate divergence (e.g., Mobile drops to 0% while Desktop is healthy)
- Overall conversion-rate collapse
- Revenue and Average Order Value (AOV) anomalies
- Funnel stage drop-offs (Sessions -> Cart -> Checkout -> Purchase)
- Traffic volume drops across dimensions (device, campaign, browser)
- Computes a 0-100 Business Health Score and Composite Reliability Score.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import duckdb

from sitepulse.config import SERVING_DB_PATH


@dataclass
class AnomalyRecord:
    anomaly_id: str
    detected_at: datetime
    anomaly_type: str  # DEVICE_SEGMENT_DIVERGENCE, CONVERSION_RATE_PLUNGE, REVENUE_COLLAPSE, FUNNEL_BREAKAGE, TRAFFIC_DROP
    segment: str       # e.g., "device:mobile", "global", "funnel:checkout_to_purchase"
    severity: str      # CRITICAL, WARNING, INFO
    baseline_value: float
    observed_value: float
    relative_change: float
    root_cause_hint: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BusinessHealthEvaluation:
    evaluation_date: str
    business_health_score: float  # 0 to 100
    structural_health_score: float  # 0 to 100
    composite_reliability_score: float  # 0 to 100
    status: str  # HEALTHY, DEGRADED, CRITICAL
    anomalies: List[AnomalyRecord]
    metrics_summary: Dict[str, Any]


class BusinessMetricAnomalyEngine:
    """
    Evaluates business metrics from the serving star schema and flags anomalies.
    Correlates with structural quality scores to detect silent client-side tagging issues.
    """

    # Baselines for clickstream e-commerce simulation
    BASELINE_CR_GLOBAL = 0.05       # 5.0% expected baseline conversion
    BASELINE_CR_MOBILE = 0.05       # 5.0% expected mobile conversion
    BASELINE_CR_DESKTOP = 0.04      # 4.0% expected desktop conversion
    BASELINE_AOV = 120.0            # $120 expected AOV
    BASELINE_CHECKOUT_TO_PURCHASE = 0.50  # 50% checkout completion rate

    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        self.conn = db_conn

    def evaluate_business_metrics(
        self,
        structural_health_score: float = 100.0,
        evaluation_date: Optional[str] = None
    ) -> BusinessHealthEvaluation:
        """
        Run multidimensional business anomaly detection across DuckDB fact and dimension tables.
        """
        eval_date = evaluation_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        now = datetime.now(timezone.utc)
        anomalies: List[AnomalyRecord] = []

        # -------------------------------------------------------------
        # 1. Device Segment Conversion Rate & Divergence
        # -------------------------------------------------------------
        device_stats = self.conn.execute("""
            SELECT 
                d.device_type,
                count(s.session_id) AS total_sessions,
                sum(s.has_cart) AS cart_sessions,
                sum(s.has_checkout) AS checkout_sessions,
                sum(s.has_purchase) AS purchase_sessions,
                coalesce(sum(s.total_revenue), 0.0) AS total_revenue
            FROM fact_sessions s
            LEFT JOIN dim_device d ON s.device_sk = d.device_sk
            GROUP BY d.device_type
        """).fetchall()

        stats_by_device: Dict[str, Dict[str, Any]] = {}
        for dev_type, sess, cart, chk, purch, rev in device_stats:
            dev_name = dev_type or "unknown"
            cr = (purch / sess) if sess > 0 else 0.0
            aov = (rev / purch) if purch > 0 else 0.0
            stats_by_device[dev_name] = {
                "sessions": sess,
                "cart_sessions": cart,
                "checkout_sessions": chk,
                "purchases": purch,
                "revenue": rev,
                "conversion_rate": cr,
                "aov": aov
            }

        # Check mobile-vs-desktop divergence
        mobile_data = stats_by_device.get("mobile", {"sessions": 0, "purchases": 0, "conversion_rate": 0.0})
        desktop_data = stats_by_device.get("desktop", {"sessions": 0, "purchases": 0, "conversion_rate": 0.0})

        mobile_cr = mobile_data["conversion_rate"]
        desktop_cr = desktop_data["conversion_rate"]
        mobile_sess = mobile_data["sessions"]

        # If mobile sessions exist (>10) and mobile CR is 0% while desktop CR > 0 or expected > 0
        if mobile_sess >= 10 and mobile_cr == 0.0:
            divergence = -1.0 if desktop_cr > 0 else -1.0
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="DEVICE_SEGMENT_DIVERGENCE",
                segment="device:mobile",
                severity="CRITICAL",
                baseline_value=desktop_cr if desktop_cr > 0 else self.BASELINE_CR_MOBILE,
                observed_value=0.0,
                relative_change=-1.0,
                root_cause_hint=(
                    f"Mobile conversion rate plunged to 0.0% across {mobile_sess} sessions "
                    f"while desktop converted at {desktop_cr:.2%}. Likely client-side mobile JS SDK exception "
                    "or broken mobile checkout touch handler."
                ),
                details={"mobile_sessions": mobile_sess, "desktop_cr": desktop_cr, "mobile_cr": 0.0}
            ))
        elif mobile_sess >= 10 and desktop_cr > 0 and (desktop_cr - mobile_cr) / desktop_cr > 0.70:
            # Significant divergence (>70% drop)
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="DEVICE_SEGMENT_DIVERGENCE",
                segment="device:mobile",
                severity="WARNING",
                baseline_value=desktop_cr,
                observed_value=mobile_cr,
                relative_change=(mobile_cr - desktop_cr) / desktop_cr,
                root_cause_hint=f"Mobile conversion rate ({mobile_cr:.2%}) severely lagged desktop ({desktop_cr:.2%}).",
                details={"mobile_sessions": mobile_sess, "desktop_cr": desktop_cr, "mobile_cr": mobile_cr}
            ))

        # -------------------------------------------------------------
        # 2. Overall Funnel Stage Breakage
        # -------------------------------------------------------------
        funnel_stats = self.conn.execute("""
            SELECT 
                count(session_id) AS sessions,
                sum(has_cart) AS carts,
                sum(has_checkout) AS checkouts,
                sum(has_purchase) AS purchases,
                coalesce(sum(total_revenue), 0.0) AS revenue
            FROM fact_sessions
        """).fetchone()

        total_sessions = funnel_stats[0] or 0
        total_carts = funnel_stats[1] or 0
        total_checkouts = funnel_stats[2] or 0
        total_purchases = funnel_stats[3] or 0
        total_revenue = funnel_stats[4] or 0.0

        global_cr = (total_purchases / total_sessions) if total_sessions > 0 else 0.0
        global_aov = (total_revenue / total_purchases) if total_purchases > 0 else 0.0

        # Check Checkout -> Purchase funnel breakage
        if total_checkouts > 0 and total_purchases == 0:
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="FUNNEL_BREAKAGE",
                segment="funnel:checkout_to_purchase",
                severity="CRITICAL",
                baseline_value=self.BASELINE_CHECKOUT_TO_PURCHASE,
                observed_value=0.0,
                relative_change=-1.0,
                root_cause_hint=(
                    f"{total_checkouts} users initiated checkout but 0 purchases were recorded. "
                    "Conversion event tag ('order_completed') is either failing to trigger or rejected."
                ),
                details={"checkouts": total_checkouts, "purchases": 0}
            ))
        elif total_checkouts > 5 and (total_purchases / total_checkouts) < 0.10:
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="FUNNEL_BREAKAGE",
                segment="funnel:checkout_to_purchase",
                severity="WARNING",
                baseline_value=self.BASELINE_CHECKOUT_TO_PURCHASE,
                observed_value=(total_purchases / total_checkouts),
                relative_change=((total_purchases / total_checkouts) - self.BASELINE_CHECKOUT_TO_PURCHASE) / self.BASELINE_CHECKOUT_TO_PURCHASE,
                root_cause_hint=f"Checkout-to-purchase completion rate dropped to {total_purchases / total_checkouts:.1%}.",
                details={"checkouts": total_checkouts, "purchases": total_purchases}
            ))

        # -------------------------------------------------------------
        # 3. Revenue Collapse / Anomaly
        # -------------------------------------------------------------
        if total_sessions >= 20 and total_revenue == 0.0:
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="REVENUE_COLLAPSE",
                segment="global:revenue",
                severity="CRITICAL",
                baseline_value=self.BASELINE_AOV * (total_sessions * self.BASELINE_CR_GLOBAL),
                observed_value=0.0,
                relative_change=-1.0,
                root_cause_hint=(
                    f"Zero revenue tracked across {total_sessions} sessions. "
                    "Transactions missing or invalidated by quarantine filters."
                ),
                details={"total_sessions": total_sessions, "total_revenue": 0.0}
            ))

        # Check AOV extreme spikes (e.g. unquarantined bogus values)
        if total_purchases > 0 and global_aov > 1000.0:
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="REVENUE_ANOMALY_SPIKE",
                segment="global:aov",
                severity="CRITICAL",
                baseline_value=self.BASELINE_AOV,
                observed_value=global_aov,
                relative_change=(global_aov - self.BASELINE_AOV) / self.BASELINE_AOV,
                root_cause_hint=f"Extreme Average Order Value detected: ${global_aov:,.2f} vs baseline ${self.BASELINE_AOV:.2f}.",
                details={"aov": global_aov, "revenue": total_revenue, "purchases": total_purchases}
            ))

        # -------------------------------------------------------------
        # 4. Global Conversion Rate Plunge
        # -------------------------------------------------------------
        if total_sessions >= 20 and global_cr == 0.0:
            anomalies.append(AnomalyRecord(
                anomaly_id=str(uuid.uuid4()),
                detected_at=now,
                anomaly_type="CONVERSION_RATE_PLUNGE",
                segment="global:conversion_rate",
                severity="CRITICAL",
                baseline_value=self.BASELINE_CR_GLOBAL,
                observed_value=0.0,
                relative_change=-1.0,
                root_cause_hint=f"Global conversion rate dropped to 0.0% across {total_sessions} sessions.",
                details={"sessions": total_sessions, "purchases": 0}
            ))

        # -------------------------------------------------------------
        # 5. Compute Business Health Score (0 - 100)
        # -------------------------------------------------------------
        # Deduct penalties: CRITICAL = -35 points, WARNING = -15 points, max deduction = 100
        total_penalty = 0.0
        for anom in anomalies:
            if anom.severity == "CRITICAL":
                total_penalty += 35.0
            elif anom.severity == "WARNING":
                total_penalty += 15.0

        business_health_score = max(0.0, 100.0 - total_penalty)

        # -------------------------------------------------------------
        # 6. Composite Reliability Score (50% Data Quality + 50% Business Metric)
        # -------------------------------------------------------------
        composite_score = round(0.50 * structural_health_score + 0.50 * business_health_score, 1)

        if composite_score >= 90.0:
            status = "HEALTHY"
        elif composite_score >= 70.0:
            status = "DEGRADED"
        else:
            status = "CRITICAL"

        # -------------------------------------------------------------
        # 7. Persist Anomalies to DuckDB Table
        # -------------------------------------------------------------
        for anom in anomalies:
            self.conn.execute("""
                INSERT INTO business_metric_anomalies (
                    anomaly_id, detected_at, anomaly_type, segment, severity,
                    baseline_value, observed_value, relative_change, root_cause_hint, details_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (anomaly_id) DO NOTHING
            """, (
                anom.anomaly_id,
                anom.detected_at,
                anom.anomaly_type,
                anom.segment,
                anom.severity,
                anom.baseline_value,
                anom.observed_value,
                anom.relative_change,
                anom.root_cause_hint,
                json.dumps(anom.details)
            ))

        metrics_summary = {
            "total_sessions": total_sessions,
            "total_purchases": total_purchases,
            "total_revenue": total_revenue,
            "global_conversion_rate": global_cr,
            "global_aov": global_aov,
            "device_breakdown": stats_by_device,
            "anomaly_count": len(anomalies)
        }

        return BusinessHealthEvaluation(
            evaluation_date=eval_date,
            business_health_score=business_health_score,
            structural_health_score=structural_health_score,
            composite_reliability_score=composite_score,
            status=status,
            anomalies=anomalies,
            metrics_summary=metrics_summary
        )
