"""Executive Analytics & Variance Reporting Engine.

Computes actual vs baseline variances across KPIs, evaluates dimensional performance,
and generates decision-ready reporting tables for executive and analyst stakeholders.
"""
from dataclasses import dataclass
from typing import Dict, Any, List
import duckdb


@dataclass
class MetricVariance:
    metric_name: str
    actual: float
    baseline: float
    variance_abs: float
    variance_pct: float
    status: str  # ON_TARGET, BELOW_BASELINE, CRITICAL_DROP, OUTPERFORMING
    unit: str    # count, currency, percentage


class VarianceEngine:
    """Computes actual vs baseline variances and executive summaries."""

    BASELINES = {
        "sessions": 100.0,
        "conversions": 5.0,
        "conversion_rate": 5.0,  # 5.0%
        "revenue": 600.0,        # $600
        "aov": 120.0,            # $120
    }

    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        self.conn = db_conn

    def compute_kpi_variances(self) -> List[MetricVariance]:
        """Compute top-line KPI variances against historical baselines."""
        row = self.conn.execute("""
            SELECT 
                count(session_id) AS sessions,
                sum(has_purchase) AS purchases,
                coalesce(sum(total_revenue), 0.0) AS revenue
            FROM fact_sessions
        """).fetchone()

        sessions = float(row[0] or 0)
        purchases = float(row[1] or 0)
        revenue = float(row[2] or 0.0)
        cr = (purchases / sessions * 100.0) if sessions > 0 else 0.0
        aov = (revenue / purchases) if purchases > 0 else 0.0

        metrics_data = [
            ("Total Sessions", sessions, self.BASELINES["sessions"], "count"),
            ("Completed Orders", purchases, self.BASELINES["conversions"], "count"),
            ("Conversion Rate", cr, self.BASELINES["conversion_rate"], "percentage"),
            ("Tracked Revenue", revenue, self.BASELINES["revenue"], "currency"),
            ("Average Order Value (AOV)", aov, self.BASELINES["aov"], "currency")
        ]

        variances: List[MetricVariance] = []
        for name, actual, base, unit in metrics_data:
            diff = actual - base
            pct = (diff / base * 100.0) if base > 0 else 0.0

            if pct < -40.0:
                status = "CRITICAL_DROP"
            elif pct < -10.0:
                status = "BELOW_BASELINE"
            elif pct > 20.0:
                status = "OUTPERFORMING"
            else:
                status = "ON_TARGET"

            variances.append(MetricVariance(
                metric_name=name,
                actual=round(actual, 2),
                baseline=round(base, 2),
                variance_abs=round(diff, 2),
                variance_pct=round(pct, 1),
                status=status,
                unit=unit
            ))

        return variances

    def generate_ascii_dashboard(self) -> str:
        """Render a formatted executive ASCII dashboard card."""
        variances = self.compute_kpi_variances()

        lines = [
            "================================================================================",
            "                   SITEPULSE EXECUTIVE ANALYTICS & VARIANCE REPORT               ",
            "================================================================================",
            f"{'Metric':<28} | {'Actual':<12} | {'Baseline':<12} | {'Variance %':<12} | {'Status'}",
            "-" * 80
        ]

        for v in variances:
            if v.unit == "percentage":
                act_str = f"{v.actual:.2f}%"
                base_str = f"{v.baseline:.2f}%"
            elif v.unit == "currency":
                act_str = f"${v.actual:,.2f}"
                base_str = f"${v.baseline:,.2f}"
            else:
                act_str = f"{int(v.actual):,}"
                base_str = f"{int(v.baseline):,}"

            sign = "+" if v.variance_pct > 0 else ""
            var_str = f"{sign}{v.variance_pct:.1f}%"
            lines.append(f"{v.metric_name:<28} | {act_str:<12} | {base_str:<12} | {var_str:<12} | {v.status}")

        lines.append("=" * 80)
        return "\n".join(lines)
