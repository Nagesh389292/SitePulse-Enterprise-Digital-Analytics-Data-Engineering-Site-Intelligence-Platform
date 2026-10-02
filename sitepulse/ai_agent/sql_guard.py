"""AST-Level SQL Security Guard using sqlglot.

Enforces zero-trust read-only governance on all generated SQL queries:
1. Validates syntactic grammar and parses into Abstract Syntax Tree (AST).
2. Prohibits multi-statement chaining.
3. Strictly enforces SELECT-only read operations (blocks DROP, DELETE, INSERT, UPDATE, ALTER, ATTACH, COPY).
4. Restricts table access exclusively to governed serving mart tables and CTEs.
5. Blocks malicious filesystem/system functions (read_parquet, read_csv, glob, pragma).
6. Automatically injects row limits (LIMIT 500) to prevent denial-of-service memory exhaustion.
"""
from dataclasses import dataclass
from typing import Set, Optional, List
import sqlglot
from sqlglot import exp


ALLOWED_TABLES: Set[str] = {
    "dim_page",
    "dim_device",
    "dim_campaign",
    "fact_sessions",
    "fact_conversions",
    "quality_incidents",
    "quality_scores_daily",
    "business_metric_anomalies",
}

FORBIDDEN_FUNCTIONS: Set[str] = {
    "read_parquet",
    "read_csv",
    "read_json",
    "scan_parquet",
    "glob",
    "pragma",
    "current_user",
    "getenv",
    "load",
    "install",
}


@dataclass
class GuardResult:
    is_valid: bool
    error: Optional[str]
    sanitized_sql: str


class SQLGuard:
    """Enforces AST-level query validation and safety boundaries."""

    def __init__(self, allowed_tables: Optional[Set[str]] = None, max_row_limit: int = 500):
        self.allowed_tables = allowed_tables or ALLOWED_TABLES
        self.max_row_limit = max_row_limit

    def validate_and_sanitize(self, sql_query: str) -> GuardResult:
        """
        Parses SQL query into AST, validates security invariants, and applies safety limits.
        """
        raw_sql = sql_query.strip()
        # Remove trailing semicolon if present
        if raw_sql.endswith(";"):
            raw_sql = raw_sql[:-1].strip()

        # 1. Parse using sqlglot
        try:
            parsed = sqlglot.parse(raw_sql, read="duckdb")
        except Exception as e:
            return GuardResult(is_valid=False, error=f"SQL Syntax Parse Error: {str(e)}", sanitized_sql="")

        # 2. Reject multi-statement queries
        if len(parsed) != 1:
            return GuardResult(
                is_valid=False,
                error="Multi-statement queries prohibited (potential SQL injection vector).",
                sanitized_sql=""
            )

        root_ast = parsed[0]

        # 3. Strictly enforce SELECT / WITH ... SELECT
        allowed_root_types = (exp.Select, exp.Union)
        if not isinstance(root_ast, allowed_root_types):
            return GuardResult(
                is_valid=False,
                error=f"Non-read statement rejected. Only SELECT queries are permitted (got {type(root_ast).__name__}).",
                sanitized_sql=""
            )

        # 4. Check for forbidden expression types anywhere in AST
        forbidden_node_types = (
            exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Alter,
            exp.Create, exp.Command, exp.Pragma, exp.Merge
        )
        for node in root_ast.walk():
            if isinstance(node, forbidden_node_types):
                return GuardResult(
                    is_valid=False,
                    error=f"Destructive or mutating operation detected: {type(node).__name__}.",
                    sanitized_sql=""
                )

        # 5. Extract defined CTE names to allow them as valid subquery tables
        cte_names: Set[str] = set()
        for with_expr in root_ast.find_all(exp.With):
            for cte in with_expr.expressions:
                if isinstance(cte, exp.CTE) and cte.alias:
                    cte_names.add(cte.alias.lower())

        # 6. Validate Table References
        referenced_tables: List[str] = []
        for table in root_ast.find_all(exp.Table):
            tbl_name = table.name.lower()
            referenced_tables.append(tbl_name)
            if tbl_name not in self.allowed_tables and tbl_name not in cte_names:
                return GuardResult(
                    is_valid=False,
                    error=f"Unauthorized table access: Table '{tbl_name}' is not in the governed allowlist.",
                    sanitized_sql=""
                )

        # 7. Check for forbidden functions
        for func in root_ast.find_all(exp.Anonymous):
            func_name = func.name.lower()
            if func_name in FORBIDDEN_FUNCTIONS:
                return GuardResult(
                    is_valid=False,
                    error=f"Forbidden system function detected: '{func_name}'.",
                    sanitized_sql=""
                )

        # 8. Check and enforce LIMIT
        has_limit = False
        for limit_node in root_ast.find_all(exp.Limit):
            has_limit = True
            try:
                limit_val = int(limit_node.expression.this)
                if limit_val > self.max_row_limit:
                    limit_node.expression.set("this", str(self.max_row_limit))
            except Exception:
                pass

        if not has_limit:
            # Append LIMIT node
            root_ast = root_ast.limit(self.max_row_limit)

        sanitized = root_ast.sql(dialect="duckdb")
        return GuardResult(is_valid=True, error=None, sanitized_sql=sanitized)
