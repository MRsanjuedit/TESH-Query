"""
Unit tests for teshq chat enhancements and fixes:
- Local LLM provider initialization in TeshQuery and chat
- Environment and dialect detection in status bar / HUD
- Dynamic schema tree rendering from engine
- Multi-format /export (parquet, jsonl, csv, sqlite, excel)
- Error recovery with captured SQL & explainability
- Slash commands and single-tap shortcuts
"""

import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from teshq.api import TeshQuery
from teshq.session import TeshChatSession
from teshq.cli.chat import (
    app,
    _get_env_summary,
    _extract_db_name,
    _render_schema_tree,
)
from teshq.core.schema_graph import SchemaGraph, JoinEdge
from typer.testing import CliRunner

runner = CliRunner()


def test_tesh_query_local_provider_init():
    """Verify that TeshQuery initializes cleanly with provider='local' without requiring Gemini key."""
    client = TeshQuery(db_url="sqlite:///test.db", provider="local")
    assert client._provider == "local"
    assert client.db_url == "sqlite:///test.db"
    assert client.engine._provider == "local"


def test_extract_db_name_clean():
    """Verify clean database names from standard and warehouse URLs."""
    # Databricks
    db_url_databricks = (
        "databricks://token:xxx@dbc-123.cloud.databricks.com"
        "?catalog=ipl_analytics&http_path=%2Fsql%2F1.0%2Fwarehouses%2Fabc&schema=gold"
    )
    assert _extract_db_name(db_url_databricks) == "ipl_analytics.gold"

    # SQLite
    assert _extract_db_name("sqlite:///chinook.db") == "chinook"

    # Postgres
    assert _extract_db_name("postgresql://user:pass@localhost:5432/analytics") == "analytics"


def test_env_summary_dialects():
    """Verify _get_env_summary detects warehouse dialects and local provider."""
    mock_url = "databricks://token:xxx@dbc-123.databricks.com?catalog=raw&schema=bronze"
    with patch("teshq.cli.chat.get_db_url", return_value=mock_url):
        db_status, db_type, llm_name, _ = _get_env_summary(provider_override="local")
        assert db_status == "Connected"
        assert db_type == "Databricks SQL"
        assert "Local" in llm_name


def test_render_schema_tree_from_engine():
    """Verify _render_schema_tree calls engine._get_schema_graph()."""
    mock_engine = MagicMock()
    mock_engine._db_url = "sqlite:///test.db"
    mock_engine._dialect = "SQLite"

    sample_graph = SchemaGraph(
        tables={"users": ["id INTEGER PK", "name VARCHAR"], "orders": ["id INTEGER PK", "user_id INTEGER FK"]},
        joins=[JoinEdge(left_table="orders", left_column="user_id", right_table="users", right_column="id")],
        summary="Test summary"
    )
    mock_engine._get_schema_graph.return_value = sample_graph

    tree = _render_schema_tree(mock_engine)
    mock_engine._get_schema_graph.assert_called_once()
    assert tree is not None


def test_session_ask_captures_sql_on_error():
    """Verify that when execution fails after SQL generation, the SQL is retained in ChatTurn."""
    mock_client = MagicMock()
    exc = RuntimeError("Database column does not exist")
    exc.sql = "SELECT invalid_col FROM users"
    exc.parameters = {"param1": 42}
    mock_client.query_advanced.side_effect = exc

    session = TeshChatSession(client=mock_client)
    with pytest.raises(RuntimeError):
        session.ask("Show invalid column")

    assert len(session.history) == 1
    turn = session.last_turn
    assert turn is not None
    assert turn.success is False
    assert turn.sql == "SELECT invalid_col FROM users"
    assert turn.parameters == {"param1": 42}
    assert "Database column does not exist" in turn.error


def test_chat_cli_export_parquet_and_jsonl(tmp_path):
    """Verify that /export handles parquet and jsonl seamlessly."""
    csv_file = str(tmp_path / "out.csv")
    pq_file = str(tmp_path / "out.parquet")
    jsonl_file = str(tmp_path / "out.jsonl")

    mock_df = pd.DataFrame([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])
    mock_adv_res = MagicMock()
    mock_adv_res.dataframe = mock_df
    mock_adv_res.query = "SELECT id, name FROM users"
    mock_adv_res.parameters = {}
    mock_adv_res.display_results = [{"#": 1, "id": 1, "name": "Alice"}, {"#": 2, "id": 2, "name": "Bob"}]
    mock_adv_res.__len__.return_value = 2

    with patch("teshq.session.TeshChatSession.ask", return_value=mock_adv_res):
        inputs = [
            "Show users",
            f"/export parquet {pq_file}",
            f"/export jsonl {jsonl_file}",
            f"/export csv {csv_file}",
            "exit"
        ]
        result = runner.invoke(app, ["--local"], input="\n".join(inputs) + "\n")
        assert result.exit_code == 0
        assert os.path.exists(pq_file)
        assert os.path.exists(jsonl_file)
        assert os.path.exists(csv_file)
