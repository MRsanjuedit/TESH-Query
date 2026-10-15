"""
Unit tests for core token counting, SQL normalization, and query safety inspection.
"""

from teshq.core.models import SQLQuery
from teshq.core.sql_normalizer import normalize_sql
from teshq.core.token_counter import DEFAULT_TOKEN_THRESHOLD, estimate_tokens, exceeds_threshold


class TestTokenCounter:
    """Tests for token estimation heuristics."""

    def test_estimate_tokens_empty(self):
        assert estimate_tokens("") == 1

    def test_estimate_tokens_short(self):
        # 16 chars with 4 chars per token should be around 4 tokens
        assert estimate_tokens("1234567890123456") == 4

    def test_exceeds_threshold(self):
        assert exceeds_threshold("short text", threshold=100) is False
        large_text = "a" * (DEFAULT_TOKEN_THRESHOLD * 5)
        assert exceeds_threshold(large_text, threshold=DEFAULT_TOKEN_THRESHOLD) is True


class TestSqlNormalizer:
    """Tests for SQL normalization and formatting."""

    def test_normalize_empty_sql(self):
        assert normalize_sql("") == ""
        assert normalize_sql("   ") == ""

    def test_normalize_keywords_upper(self):
        sql = "select id, name from users where id = 1"
        normalized = normalize_sql(sql)
        assert "SELECT" in normalized
        assert "FROM" in normalized
        assert "WHERE" in normalized

    def test_normalize_strips_extraneous_whitespace(self):
        sql = "  SELECT   a,   b   FROM   t  "
        normalized = normalize_sql(sql)
        assert normalized.startswith("SELECT")
        assert normalized.endswith("FROM t")


class TestSQLQuerySafety:
    """Tests for SQLQuery read-only validation."""

    def test_single_select_is_read_only(self):
        q = SQLQuery(query="SELECT id, name FROM users", parameters={})
        assert q.is_read_only() is True

    def test_select_with_semicolon_is_read_only(self):
        q = SQLQuery(query="SELECT id, name FROM users;", parameters={})
        assert q.is_read_only() is True

    def test_multi_statement_blocked(self):
        q = SQLQuery(query="SELECT 1; DROP TABLE users;", parameters={})
        assert q.is_read_only() is False

    def test_insert_not_read_only(self):
        q = SQLQuery(query="INSERT INTO users (id) VALUES (1)", parameters={})
        assert q.is_read_only() is False

    def test_update_not_read_only(self):
        q = SQLQuery(query="UPDATE users SET active = 1 WHERE id = 1", parameters={})
        assert q.is_read_only() is False

    def test_quoted_semicolons_allowed_in_single_query(self):
        q = SQLQuery(query="SELECT id FROM users WHERE note = 'semi;colon'", parameters={})
        assert q.is_read_only() is True
