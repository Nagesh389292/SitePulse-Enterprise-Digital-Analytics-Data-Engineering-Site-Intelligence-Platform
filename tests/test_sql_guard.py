"""Unit tests for AST-level SQL Security Guard."""
import pytest
from sitepulse.ai_agent.sql_guard import SQLGuard


@pytest.fixture
def guard():
    return SQLGuard(max_row_limit=500)


def test_guard_allows_valid_select(guard):
    sql = "SELECT session_id, duration_seconds FROM fact_sessions WHERE duration_seconds > 60"
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is True
    assert res.error is None
    assert "LIMIT 500" in res.sanitized_sql.upper()


def test_guard_allows_valid_joins(guard):
    sql = """
    SELECT d.device_type, count(s.session_id) AS total_sessions
    FROM fact_sessions s
    LEFT JOIN dim_device d ON s.device_sk = d.device_sk
    GROUP BY d.device_type
    """
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is True
    assert "fact_sessions" in res.sanitized_sql.lower()
    assert "dim_device" in res.sanitized_sql.lower()


def test_guard_blocks_drop_table(guard):
    sql = "DROP TABLE fact_sessions"
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is False
    assert "Non-read statement rejected" in res.error or "Destructive" in res.error


def test_guard_blocks_delete_and_update(guard):
    res_del = guard.validate_and_sanitize("DELETE FROM fact_conversions WHERE revenue < 10")
    assert res_del.is_valid is False

    res_upd = guard.validate_and_sanitize("UPDATE fact_sessions SET total_revenue = 0")
    assert res_upd.is_valid is False


def test_guard_blocks_multi_statement_injection(guard):
    sql = "SELECT * FROM fact_sessions; DROP TABLE fact_conversions;"
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is False
    assert "Multi-statement" in res.error


def test_guard_blocks_unauthorized_tables(guard):
    sql = "SELECT * FROM users_credentials"
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is False
    assert "Unauthorized table access" in res.error


def test_guard_blocks_filesystem_functions(guard):
    sql = "SELECT * FROM read_parquet('secret_passwords.parquet')"
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is False


def test_guard_clamps_excessive_limit(guard):
    sql = "SELECT * FROM fact_sessions LIMIT 50000"
    res = guard.validate_and_sanitize(sql)
    assert res.is_valid is True
    assert "LIMIT 500" in res.sanitized_sql.upper()
