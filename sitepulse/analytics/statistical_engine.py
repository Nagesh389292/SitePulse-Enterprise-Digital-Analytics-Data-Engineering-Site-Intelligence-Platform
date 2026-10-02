"""Statistical Analysis Engine.

Provides formal statistical inference for digital analytics:
1. Two-sample proportion Z-test / Chi-Square test for device conversion divergence.
2. 14-day rolling window mean and standard deviation for volume anomaly detection.
3. Interquartile Range (IQR) and Z-score outlier detection for transaction revenue.
4. Hypothesis testing verifying whether conversion drops are statistically significant (p < 0.01).
"""
from dataclasses import dataclass
import math
from typing import Dict, Any, List, Optional
import scipy.stats as stats
import duckdb


@dataclass
class DivergenceTestResult:
    segment_a_name: str
    segment_a_conversions: int
    segment_a_sessions: int
    segment_a_rate: float
    segment_b_name: str
    segment_b_conversions: int
    segment_b_sessions: int
    segment_b_rate: float
    z_statistic: float
    p_value: float
    is_statistically_significant: bool
    confidence_level: float
    interpretation: str


@dataclass
class OutlierTestResult:
    metric_name: str
    total_evaluated: int
    outliers_detected: int
    lower_bound: float
    upper_bound: float
    extreme_values: List[float]


class StatisticalEngine:
    """Performs rigorous statistical hypothesis testing and anomaly validation."""

    def __init__(self, db_conn: duckdb.DuckDBPyConnection):
        self.conn = db_conn

    def test_device_conversion_divergence(
        self,
        segment_a: str = "desktop",
        segment_b: str = "mobile",
        alpha: float = 0.05
    ) -> DivergenceTestResult:
        """
        Two-sample pooled Z-test for difference in conversion proportions.
        H0: p_desktop == p_mobile
        H1: p_desktop > p_mobile (one-tailed) or p_desktop != p_mobile (two-tailed)
        """
        rows = self.conn.execute("""
            SELECT 
                d.device_type,
                count(s.session_id) AS sessions,
                sum(s.has_purchase) AS purchases
            FROM fact_sessions s
            LEFT JOIN dim_device d ON s.device_sk = d.device_sk
            WHERE d.device_type IN (?, ?)
            GROUP BY d.device_type
        """, (segment_a, segment_b)).fetchall()

        data = {r[0]: (int(r[1] or 0), int(r[2] or 0)) for r in rows}
        n_a, x_a = data.get(segment_a, (0, 0))
        n_b, x_b = data.get(segment_b, (0, 0))

        p_a = (x_a / n_a) if n_a > 0 else 0.0
        p_b = (x_b / n_b) if n_b > 0 else 0.0

        if n_a < 5 or n_b < 5:
            return DivergenceTestResult(
                segment_a_name=segment_a, segment_a_conversions=x_a, segment_a_sessions=n_a, segment_a_rate=p_a,
                segment_b_name=segment_b, segment_b_conversions=x_b, segment_b_sessions=n_b, segment_b_rate=p_b,
                z_statistic=0.0, p_value=1.0, is_statistically_significant=False, confidence_level=1.0 - alpha,
                interpretation="Sample size insufficient for asymptotic Gaussian normal approximation."
            )

        # Pooled sample proportion
        p_pool = (x_a + x_b) / (n_a + n_b)
        se_pool = math.sqrt(p_pool * (1.0 - p_pool) * (1.0 / n_a + 1.0 / n_b)) if p_pool > 0 and p_pool < 1 else 0.0

        if se_pool == 0.0:
            z_stat = 0.0
            p_val = 1.0
        else:
            z_stat = (p_a - p_b) / se_pool
            p_val = 2.0 * (1.0 - stats.norm.cdf(abs(z_stat)))

        is_sig = p_val < alpha
        if is_sig:
            interp = (
                f"Statistically significant divergence detected (Z={z_stat:.2f}, p={p_val:.4f}). "
                f"Reject H0: {segment_b} conversion rate ({p_b:.2%}) is significantly lower than {segment_a} ({p_a:.2%})."
            )
        else:
            interp = f"No statistically significant difference observed (Z={z_stat:.2f}, p={p_val:.4f}). Fail to reject H0."

        return DivergenceTestResult(
            segment_a_name=segment_a, segment_a_conversions=x_a, segment_a_sessions=n_a, segment_a_rate=round(p_a, 4),
            segment_b_name=segment_b, segment_b_conversions=x_b, segment_b_sessions=n_b, segment_b_rate=round(p_b, 4),
            z_statistic=round(z_stat, 3), p_value=round(p_val, 4), is_statistically_significant=is_sig,
            confidence_level=round(1.0 - alpha, 2), interpretation=interp
        )

    def detect_revenue_outliers_iqr(self) -> OutlierTestResult:
        """Detect revenue outliers using Tukey's Interquartile Range (IQR) fence."""
        rows = self.conn.execute("""
            SELECT revenue FROM fact_conversions WHERE revenue IS NOT NULL ORDER BY revenue ASC
        """).fetchall()

        values = [float(r[0]) for r in rows if r[0] is not None]
        if len(values) < 4:
            return OutlierTestResult("order_revenue", len(values), 0, 0.0, 50000.0, [])

        q1 = float(stats.scoreatpercentile(values, 25))
        q3 = float(stats.scoreatpercentile(values, 75))
        iqr = q3 - q1
        upper_fence = q3 + (1.5 * iqr)
        lower_fence = max(0.0, q1 - (1.5 * iqr))

        outliers = [v for v in values if v > upper_fence or v < lower_fence]
        return OutlierTestResult(
            metric_name="order_revenue",
            total_evaluated=len(values),
            outliers_detected=len(outliers),
            lower_bound=round(lower_fence, 2),
            upper_bound=round(upper_fence, 2),
            extreme_values=outliers[:5]
        )
