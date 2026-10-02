"""Business Impact & Financial Revenue Exposure Modeler.

Translates technical telemetry defects, schema quarantines, and conversion rate drops
into quantified business metrics: conversion gap, revenue exposure, and marketing risk.
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional
import duckdb


@dataclass
class ImpactAssessment:
    segment: str
    sessions: int
    actual_conversions: int
    expected_conversions: int
    conversion_gap: int
    actual_revenue: float
    expected_revenue: float
    revenue_exposure_usd: float
    exposure_type: str  # UNTRACKED_TELEMETRY_REVENUE, ACTUAL_LOST_REVENUE, PROTECTED_CORRUPTED_REVENUE
    marketing_ad_spend_risk: str
    executive_summary: str


class BusinessImpactModeler:
    """Calculates financial exposure and marketing attribution risk from data anomalies."""

    BASELINE_CR = {
        "mobile": 0.05,    # 5.0%
        "desktop": 0.04,   # 4.0%
        "tablet": 0.05,    # 5.0%
        "global": 0.045    # 4.5%
    }
    BASELINE_AOV = 120.0   # $120 Average Order Value

    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        self.conn = db_conn

    def quantify_segment_impact(self, segment: str = "mobile") -> ImpactAssessment:
        """Quantify conversion shortfall and revenue exposure for a specific segment."""
        row = self.conn.execute("""
            SELECT 
                count(s.session_id) AS total_sessions,
                sum(s.has_purchase) AS purchases,
                coalesce(sum(s.total_revenue), 0.0) AS revenue
            FROM fact_sessions s
            LEFT JOIN dim_device d ON s.device_sk = d.device_sk
            WHERE d.device_type = ?
        """, (segment,)).fetchone()

        sessions = int(row[0] or 0) if row else 0
        actual_conv = int(row[1] or 0) if row else 0
        actual_rev = float(row[2] or 0.0) if row else 0.0

        baseline_cr = self.BASELINE_CR.get(segment, self.BASELINE_CR["global"])
        expected_conv = round(sessions * baseline_cr)
        conv_gap = max(0, expected_conv - actual_conv)

        expected_rev = round(expected_conv * self.BASELINE_AOV, 2)
        revenue_exposure = round(conv_gap * self.BASELINE_AOV, 2)

        if actual_conv == 0 and sessions >= 10:
            exposure_type = "UNTRACKED_TELEMETRY_REVENUE"
            ad_risk = (
                "CRITICAL: Performance Max and Smart Bidding algorithms will infer 0% mobile ROAS, "
                "triggering automated budget down-weighting on high-intent mobile search terms."
            )
            exec_summary = (
                f"Severe conversion tracking breakdown on {segment}: {conv_gap} transactions untracked, "
                f"representing ${revenue_exposure:,.2f} in unrecorded e-commerce revenue."
            )
        elif conv_gap > 0:
            exposure_type = "PARTIAL_REVENUE_SHORTFALL"
            ad_risk = "MODERATE: Attribution models report reduced channel efficiency."
            exec_summary = (
                f"Conversion underperformance on {segment}: {conv_gap} order shortfall vs baseline, "
                f"representing ${revenue_exposure:,.2f} in revenue gap."
            )
        else:
            exposure_type = "HEALTHY_ALIGNED"
            ad_risk = "NONE: Telemetry and conversion tracking operating within target variance."
            exec_summary = f"{segment.capitalize()} operations performing at or above target baseline."

        return ImpactAssessment(
            segment=segment,
            sessions=sessions,
            actual_conversions=actual_conv,
            expected_conversions=expected_conv,
            conversion_gap=conv_gap,
            actual_revenue=actual_rev,
            expected_revenue=expected_rev,
            revenue_exposure_usd=revenue_exposure,
            exposure_type=exposure_type,
            marketing_ad_spend_risk=ad_risk,
            executive_summary=exec_summary
        )

    def quantify_global_exposure(self) -> Dict[str, Any]:
        """Quantify global e-commerce and site performance exposure across all dimensions."""
        global_row = self.conn.execute("""
            SELECT 
                count(session_id) AS total_sessions,
                sum(has_purchase) AS total_purchases,
                coalesce(sum(total_revenue), 0.0) AS total_revenue
            FROM fact_sessions
        """).fetchone()

        sess = int(global_row[0] or 0) if global_row else 0
        purch = int(global_row[1] or 0) if global_row else 0
        rev = float(global_row[2] or 0.0) if global_row else 0.0

        exp_conv = round(sess * self.BASELINE_CR["global"])
        conv_gap = max(0, exp_conv - purch)
        rev_exposure = round(conv_gap * self.BASELINE_AOV, 2)

        return {
            "total_sessions": sess,
            "actual_purchases": purch,
            "expected_purchases": exp_conv,
            "conversion_gap": conv_gap,
            "actual_revenue": rev,
            "expected_revenue": round(exp_conv * self.BASELINE_AOV, 2),
            "estimated_financial_exposure_usd": rev_exposure
        }
