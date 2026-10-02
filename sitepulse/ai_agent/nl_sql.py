"""Natural Language to Safe SQL Agent.

Translates analytical business inquiries into governed SQL queries,
validates them via sqlglot AST guardrails, and executes them safely against DuckDB.
"""
import os
import re
import time
from typing import Dict, Any, List, Optional
import duckdb

from sitepulse.semantic.catalog import SemanticCatalog
from sitepulse.ai_agent.sql_guard import SQLGuard, GuardResult


class NLToSQLAgent:
    """Governed NL-to-SQL compiler backed by Semantic Catalog and AST Guard."""

    def __init__(self, db_conn: duckdb.DuckDBPyConnection, catalog: Optional[SemanticCatalog] = None):
        self.conn = db_conn
        self.catalog = catalog or SemanticCatalog()
        self.guard = SQLGuard()

    def translate_and_execute(self, natural_query: str) -> Dict[str, Any]:
        """
        End-to-end pipeline: NL -> Candidate SQL -> AST Validation -> DuckDB Execution.
        """
        start_time = time.perf_counter()

        # 1. Generate Candidate SQL
        candidate_sql, engine_source = self._generate_sql(natural_query)

        # 2. Validate & Sanitize via AST Guard
        guard_result: GuardResult = self.guard.validate_and_sanitize(candidate_sql)

        if not guard_result.is_valid:
            return {
                "success": False,
                "natural_query": natural_query,
                "error": f"Security Guard Violation: {guard_result.error}",
                "generated_sql": candidate_sql,
                "sanitized_sql": None,
                "rows": [],
                "columns": [],
                "engine_source": engine_source,
                "execution_ms": round((time.perf_counter() - start_time) * 1000, 2)
            }

        # 3. Safe Execution against DuckDB
        try:
            cursor = self.conn.execute(guard_result.sanitized_sql)
            col_names = [desc[0] for desc in cursor.description] if cursor.description else []
            raw_rows = cursor.fetchall()

            # Format as list of dicts
            rows = [dict(zip(col_names, row)) for row in raw_rows]

            return {
                "success": True,
                "natural_query": natural_query,
                "generated_sql": candidate_sql,
                "sanitized_sql": guard_result.sanitized_sql,
                "columns": col_names,
                "rows": rows,
                "row_count": len(rows),
                "engine_source": engine_source,
                "execution_ms": round((time.perf_counter() - start_time) * 1000, 2)
            }
        except Exception as e:
            return {
                "success": False,
                "natural_query": natural_query,
                "error": f"Execution Error: {str(e)}",
                "generated_sql": candidate_sql,
                "sanitized_sql": guard_result.sanitized_sql,
                "rows": [],
                "columns": [],
                "engine_source": engine_source,
                "execution_ms": round((time.perf_counter() - start_time) * 1000, 2)
            }

    def _generate_sql(self, query: str) -> tuple[str, str]:
        """
        Generates candidate SQL using LLM if credentials exist, or semantic compiler fallback.
        """
        # Attempt LLM generation if GEMINI_API_KEY is available
        gemini_key = os.environ.get("GEMINI_API_KEY")
        if gemini_key:
            try:
                from google import genai
                client = genai.Client(api_key=gemini_key)
                prompt = (
                    f"You are a Senior Data Engineer at Dentsu Aegis generating DuckDB SQL for an analytical platform.\n"
                    f"{self.catalog.get_schema_context_markdown()}\n\n"
                    f"USER QUESTION: {query}\n"
                    f"CRITICAL RULES:\n"
                    f"1. Generate ONLY a single SELECT query.\n"
                    f"2. Use only allowed tables: dim_page, dim_device, dim_campaign, fact_sessions, fact_conversions, quality_incidents, business_metric_anomalies.\n"
                    f"3. Do not include explanation. Output ONLY valid SQL inside a markdown ```sql ... ``` block."
                )
                response = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt
                )
                text = response.text or ""
                match = re.search(r"```sql\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
                if match:
                    return match.group(1).strip(), "gemini-2.5-flash"
            except Exception:
                pass  # Fall back to semantic compiler

        # Deterministic Semantic Compiler
        sql = self._semantic_compile(query)
        return sql, "semantic_compiler"

    def _semantic_compile(self, query: str) -> str:
        """Deterministic NLP intent compiler grounded in the Semantic Metric Catalog."""
        q = query.lower()

        # Scenario 1: Incidents & Quality Alerts
        if "incident" in q or "alert" in q or "dq" in q or "failure" in q:
            return """
            SELECT 
                incident_id,
                detected_at,
                severity,
                failure_type,
                metric_affected,
                description
            FROM quality_incidents
            ORDER BY detected_at DESC
            LIMIT 50
            """.strip()

        # Scenario 2: Business Anomalies
        if "anomal" in q or "divergence" in q or "collapse" in q:
            return """
            SELECT 
                anomaly_id,
                detected_at,
                anomaly_type,
                segment,
                severity,
                baseline_value,
                observed_value,
                relative_change,
                root_cause_hint
            FROM business_metric_anomalies
            ORDER BY detected_at DESC
            LIMIT 50
            """.strip()

        # Scenario 3: Device / Mobile Conversion Rate
        if "device" in q or "mobile" in q or "desktop" in q:
            if "revenue" in q or "aov" in q:
                return """
                SELECT 
                    d.device_type,
                    count(distinct s.session_id) AS sessions,
                    count(distinct c.order_id) AS orders,
                    round(coalesce(sum(c.revenue), 0.0), 2) AS total_revenue,
                    round(coalesce(sum(c.revenue), 0.0) / nullif(count(distinct c.order_id), 0), 2) AS aov
                FROM fact_sessions s
                LEFT JOIN dim_device d ON s.device_sk = d.device_sk
                LEFT JOIN fact_conversions c ON s.session_id = c.session_id
                GROUP BY d.device_type
                ORDER BY total_revenue DESC
                """.strip()
            else:
                return """
                SELECT 
                    d.device_type,
                    count(distinct s.session_id) AS total_sessions,
                    count(distinct c.order_id) AS completed_orders,
                    round(count(distinct c.order_id)::DOUBLE / nullif(count(distinct s.session_id), 0) * 100.0, 2) AS conversion_rate_pct,
                    round(coalesce(sum(c.revenue), 0.0), 2) AS total_revenue
                FROM fact_sessions s
                LEFT JOIN dim_device d ON s.device_sk = d.device_sk
                LEFT JOIN fact_conversions c ON s.session_id = c.session_id
                GROUP BY d.device_type
                ORDER BY conversion_rate_pct DESC
                """.strip()

        # Scenario 4: Campaign Performance / Attribution
        if "campaign" in q or "source" in q or "utm" in q:
            return """
            SELECT 
                c.utm_source,
                c.utm_medium,
                count(distinct s.session_id) AS sessions,
                count(distinct conv.order_id) AS conversions,
                round(count(distinct conv.order_id)::DOUBLE / nullif(count(distinct s.session_id), 0) * 100.0, 2) AS conversion_rate_pct,
                round(coalesce(sum(conv.revenue), 0.0), 2) AS total_revenue
            FROM fact_sessions s
            LEFT JOIN dim_campaign c ON s.campaign_sk = c.campaign_sk
            LEFT JOIN fact_conversions conv ON s.session_id = conv.session_id
            GROUP BY c.utm_source, c.utm_medium
            ORDER BY total_revenue DESC
            """.strip()

        # Scenario 5: Funnel Drop-off / Cart Abandonment
        if "funnel" in q or "cart" in q or "abandon" in q or "checkout" in q:
            return """
            SELECT 
                count(session_id) AS total_sessions,
                sum(has_cart) AS cart_sessions,
                sum(has_checkout) AS checkout_sessions,
                sum(has_purchase) AS purchase_sessions,
                round((sum(has_cart) - sum(has_purchase))::DOUBLE / nullif(sum(has_cart), 0) * 100.0, 2) AS cart_abandonment_pct,
                round(sum(has_purchase)::DOUBLE / nullif(sum(has_checkout), 0) * 100.0, 2) AS checkout_completion_pct
            FROM fact_sessions
            """.strip()

        # Scenario 6: Revenue & Sales
        if "revenue" in q or "sales" in q or "order" in q:
            return """
            SELECT 
                s.session_date,
                count(distinct s.session_id) AS sessions,
                count(distinct c.order_id) AS orders,
                round(coalesce(sum(c.revenue), 0.0), 2) AS total_revenue,
                round(coalesce(sum(c.revenue), 0.0) / nullif(count(distinct c.order_id), 0), 2) AS aov,
                round(coalesce(sum(c.revenue), 0.0) / nullif(count(distinct s.session_id), 0), 2) AS rps
            FROM fact_sessions s
            LEFT JOIN fact_conversions c ON s.session_id = c.session_id
            GROUP BY s.session_date
            ORDER BY s.session_date DESC
            """.strip()

        # Default: Global Session & Conversion Summary
        return """
        SELECT 
            count(distinct s.session_id) AS total_sessions,
            count(distinct c.order_id) AS total_conversions,
            round(count(distinct c.order_id)::DOUBLE / nullif(count(distinct s.session_id), 0) * 100.0, 2) AS conversion_rate_pct,
            round(coalesce(sum(c.revenue), 0.0), 2) AS total_revenue
        FROM fact_sessions s
        LEFT JOIN fact_conversions c ON s.session_id = c.session_id
        """.strip()
