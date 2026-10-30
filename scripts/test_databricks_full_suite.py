"""
Comprehensive Live Integration Test of All TESH-Query Functions and Methods against Databricks
"""
import os
import sys
import time
import pandas as pd
from typing import Dict, Any

# Databricks testing credentials (read from environment)
SERVER_HOSTNAME = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "dbc-ee506728-0832.cloud.databricks.com")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/36e71f1d332b5189")
PAT = os.environ.get("DATABRICKS_TOKEN") or "dummy_databricks_token"
CATALOG = os.environ.get("DATABRICKS_CATALOG", "ipl_analytics")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "gold")

DATABRICKS_URL = (
    f"databricks://token:{PAT}@{SERVER_HOSTNAME}:443/{SCHEMA}"
    f"?http_path={HTTP_PATH}&catalog={CATALOG}"
)

results: Dict[str, bool] = {}
timings: Dict[str, float] = {}

def record_test(name: str, passed: bool, duration: float, detail: str = ""):
    results[name] = passed
    timings[name] = duration
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"[{status}] {name} ({duration:.2f}s) {detail}")

print("=" * 80)
print("TESH-QUERY COMPLETE FUNCTION & METHOD TEST SUITE: DATABRICKS")
print(f"Target: {SERVER_HOSTNAME}")
print(f"Catalog: {CATALOG} | Schema: {SCHEMA}")
print("=" * 80)

# ==============================================================================
# SECTION 1: Connectors & URL Parsing
# ==============================================================================
print("\n--- 1. Connectors & URL Normalization ---")

t0 = time.time()
try:
    from teshq.core.connectors import UnifiedDatabaseConnector, DatabricksConnector
    
    # 1.1 Database type detection
    detected = UnifiedDatabaseConnector.detect_database_type(DATABRICKS_URL)
    assert detected == "databricks", f"Expected 'databricks', got {detected}"
    
    # 1.2 Get connector instance
    connector = UnifiedDatabaseConnector.get_connector(DATABRICKS_URL)
    assert isinstance(connector, DatabricksConnector)
    
    # 1.3 Required packages
    pkgs = connector.get_required_packages()
    assert "databricks-sql-connector" in pkgs
    
    # 1.4 Introspection capabilities
    caps = connector.get_introspection_config()
    assert caps.get("information_schema_available") is True
    
    # 1.5 Test connection method
    ok, msg = UnifiedDatabaseConnector.test_connection(DATABRICKS_URL)
    assert ok is True, f"Connection test failed: {msg}"
    
    # 1.6 Create SQLAlchemy engine
    engine = UnifiedDatabaseConnector.create_engine(DATABRICKS_URL)
    assert engine.dialect.name == "databricks"
    
    record_test("Connector: URL Parsing, Capabilities & Engine Creation", True, time.time() - t0, f"Driver: {engine.dialect.name}")
except Exception as e:
    record_test("Connector: URL Parsing, Capabilities & Engine Creation", False, time.time() - t0, str(e))


# ==============================================================================
# SECTION 2: Connection Management, Pooling & Credential Masking
# ==============================================================================
print("\n--- 2. Connection Management & Pooling ---")

t0 = time.time()
try:
    from teshq.core.connection import (
        ConnectionManager,
        connection_manager,
        execute_query_with_pooling,
        mask_database_url
    )
    
    # 2.1 Credential masking
    masked = mask_database_url(DATABRICKS_URL)
    assert PAT not in masked, "PAT token leaked in masked URL!"
    assert "token:***" in masked or "token:dapi***" in masked or "*****" in masked
    
    # 2.2 Connection manager get engine
    cm_engine = connection_manager.get_engine(DATABRICKS_URL)
    assert cm_engine is not None
    
    # 2.3 Query with timeout
    rows = connection_manager.execute_query_with_timeout(DATABRICKS_URL, "SELECT 42 AS answer")
    assert len(rows) == 1 and rows[0]["answer"] == 42
    
    # 2.4 execute_query_with_pooling helper
    pooled_rows = execute_query_with_pooling(DATABRICKS_URL, "SELECT 100 AS num, 'TESHQ' AS engine")
    assert pooled_rows[0]["num"] == 100 and pooled_rows[0]["engine"] == "TESHQ"
    
    record_test("Connection: Pooling, Timeouts & Credential Redacting", True, time.time() - t0, f"Masked: {masked[:45]}...")
except Exception as e:
    record_test("Connection: Pooling, Timeouts & Credential Redacting", False, time.time() - t0, str(e))


# ==============================================================================
# SECTION 3: Dialect Rules & Hints
# ==============================================================================
print("\n--- 3. SQL Dialects & Function Hints ---")

t0 = time.time()
try:
    from teshq.core.dialect import (
        SQLDialect,
        detect_dialect,
        get_dialect_rules,
        get_dialect_hints
    )
    
    d = detect_dialect(DATABRICKS_URL)
    assert d == SQLDialect.DATABRICKS, f"Expected DATABRICKS dialect, got {d}"
    
    rules = get_dialect_rules(d)
    assert len(rules) > 0, "Dialect rules empty"
    
    hints = get_dialect_hints(d)
    assert len(hints) > 0, "Dialect hints empty"
    
    record_test("Dialect: Databricks Rules & Function Hints", True, time.time() - t0, f"Found {len(rules)} rules, {len(hints)} hints")
except Exception as e:
    record_test("Dialect: Databricks Rules & Function Hints", False, time.time() - t0, str(e))


# ==============================================================================
# SECTION 4: Live Introspection & Schema Exploration
# ==============================================================================
print("\n--- 4. Live Schema Introspection ---")

t0 = time.time()
schema_info = None
try:
    from teshq.core.introspect import introspect_db
    
    schema_info = introspect_db(DATABRICKS_URL)
    assert schema_info is not None, "introspect_db returned None"
    
    tables = schema_info.get("tables", {})
    assert len(tables) > 0, "No tables introspected"
    
    # Verify gold tables were captured
    table_names = list(tables.keys())
    assert any("team_standings" in t for t in table_names), "team_standings missing"
    assert any("batting_summary" in t for t in table_names), "batting_summary missing"
    
    record_test("Introspect: Databricks Schema Introspection", True, time.time() - t0, f"Found {len(tables)} tables: {table_names}")
except Exception as e:
    record_test("Introspect: Databricks Schema Introspection", False, time.time() - t0, str(e))


# ==============================================================================
# SECTION 5: SchemaGraph, Pruning & Compressed Schema Formatting
# ==============================================================================
print("\n--- 5. SchemaGraph & Vector Pruning ---")

t0 = time.time()
try:
    from teshq.core.schema_graph import SchemaGraph
    
    assert schema_info is not None, "Schema info was not generated in Section 4"
    graph = SchemaGraph(schema_info)
    
    # 5.1 Graph table listing
    all_tbls = graph.get_all_tables()
    assert len(all_tbls) > 0
    
    # 5.2 Compressed schema representation (for LLM tokens optimization)
    comp_str = graph.to_compressed_string()
    assert len(comp_str) > 0
    assert "batting_summary" in comp_str or "team_standings" in comp_str
    
    # 5.3 Pruning for specific queries
    pruned = graph.prune_for_query(["batter", "runs", "strike_rate"])
    assert len(pruned) > 0
    
    record_test("SchemaGraph: Graph Modeling, Pruning & Token Compression", True, time.time() - t0, f"Pruned {len(pruned)} tables, Compressed size: {len(comp_str)} chars")
except Exception as e:
    record_test("SchemaGraph: Graph Modeling, Pruning & Token Compression", False, time.time() - t0, str(e))


# ==============================================================================
# SECTION 6: SQL Validation, AST Safety & Read-Only Policy
# ==============================================================================
print("\n--- 6. SQL Validation & AST Safety Enforcement ---")

t0 = time.time()
try:
    from teshq.core.sql_validator import validate_sql, ValidationError
    from teshq.core.connection import connection_manager
    
    # 6.1 Safe read-only SELECT
    safe_sql = "SELECT team, wins, points FROM gold.team_standings WHERE season = 2026"
    validate_sql(safe_sql)  # Should not raise
    assert connection_manager._is_read_only(safe_sql) is True
    
    # 6.2 Destructive DDL blocked: DROP TABLE
    drop_sql = "DROP TABLE gold.team_standings"
    blocked_drop = False
    try:
        validate_sql(drop_sql)
    except ValidationError:
        blocked_drop = True
    assert blocked_drop is True, "DROP TABLE was not blocked by AST validator!"
    assert connection_manager._is_read_only(drop_sql) is False
    
    # 6.3 Destructive DDL blocked: TRUNCATE
    blocked_trunc = False
    try:
        validate_sql("TRUNCATE TABLE gold.batting_summary")
    except ValidationError:
        blocked_trunc = True
    assert blocked_trunc is True, "TRUNCATE was not blocked by AST validator!"
    
    # 6.4 Destructive DML blocked: DELETE without WHERE
    blocked_del = False
    try:
        validate_sql("DELETE FROM gold.team_standings")
    except ValidationError:
        blocked_del = True
    assert blocked_del is True, "Unbounded DELETE was not blocked!"
    
    # 6.5 Destructive DML blocked: UPDATE without WHERE
    blocked_upd = False
    try:
        validate_sql("UPDATE gold.team_standings SET wins = 100")
    except ValidationError:
        blocked_upd = True
    assert blocked_upd is True, "Unbounded UPDATE was not blocked!"
    
    record_test("Validation: SQL AST Security, Safety Policies & Read-Only Checks", True, time.time() - t0, "DROP, TRUNCATE, DELETE, UPDATE safely enforced")
except Exception as e:
    record_test("Validation: SQL AST Security, Safety Policies & Read-Only Checks", False, time.time() - t0, str(e))


# ==============================================================================
# SECTION 7: Live Databricks Queries & Multi-Format Exporters
# ==============================================================================
print("\n--- 7. Live Databricks Queries & Multi-Format Exporters ---")

export_files = [
    "/tmp/teshq_db_test.csv",
    "/tmp/teshq_db_test.xlsx",
    "/tmp/teshq_db_test.parquet",
    "/tmp/teshq_db_test.jsonl",
    "/tmp/teshq_db_test.sqlite"
]

t0 = time.time()
try:
    from teshq.core.connection import execute_query_with_pooling
    from teshq.utils.save import (
        save_to_csv,
        save_to_excel,
        save_to_parquet,
        save_to_jsonl,
        save_to_sqlite
    )
    
    # 7.1 Execute real query against gold.batting_summary
    query = """
    SELECT batter, primary_team, total_runs, strike_rate, batting_average
    FROM gold.batting_summary
    LIMIT 5
    """
    raw_data = execute_query_with_pooling(DATABRICKS_URL, query)
    df = pd.DataFrame(raw_data)
    assert len(df) > 0, "Query returned 0 rows"
    
    # 7.2 Save to CSV & verify
    save_to_csv(df, export_files[0])
    assert os.path.exists(export_files[0]) and os.path.getsize(export_files[0]) > 0
    
    # 7.3 Save to Excel & verify
    save_to_excel(df, export_files[1])
    assert os.path.exists(export_files[1]) and os.path.getsize(export_files[1]) > 0
    
    # 7.4 Save to Parquet & verify
    save_to_parquet(df, export_files[2])
    assert os.path.exists(export_files[2]) and os.path.getsize(export_files[2]) > 0
    df_parquet = pd.read_parquet(export_files[2])
    assert len(df_parquet) == len(df)
    
    # 7.5 Save to JSONL & verify
    save_to_jsonl(df, export_files[3])
    assert os.path.exists(export_files[3]) and os.path.getsize(export_files[3]) > 0
    df_jsonl = pd.read_json(export_files[3], lines=True)
    assert len(df_jsonl) == len(df)
    
    # 7.6 Save to SQLite & verify
    save_to_sqlite(df, export_files[4], table_name="live_databricks_batting")
    assert os.path.exists(export_files[4]) and os.path.getsize(export_files[4]) > 0
    
    record_test("Exporters: CSV, Excel, Parquet, JSONL & SQLite Round-Trip", True, time.time() - t0, f"Exported {len(df)} rows across 5 formats")
except Exception as e:
    record_test("Exporters: CSV, Excel, Parquet, JSONL & SQLite Round-Trip", False, time.time() - t0, str(e))
finally:
    for f in export_files:
        if os.path.exists(f):
            try:
                os.remove(f)
            except Exception:
                pass


# ==============================================================================
# SECTION 8: Production Readiness Validation
# ==============================================================================
print("\n--- 8. Production Readiness Assessment ---")

t0 = time.time()
try:
    from teshq.core.validation import ConfigValidator, validate_production_readiness
    
    # 8.1 URL validation
    valid_url, err = ConfigValidator.validate_database_url(DATABRICKS_URL)
    assert valid_url is True, f"URL validation failed: {err}"
    
    # 8.2 Production readiness assessment
    prod_config = {
        "DATABASE_URL": DATABRICKS_URL,
        "LLM_PROVIDER": "google",
        "GEMINI_API_KEY": "AIzaSyDummyProductionTestingApiKey12345678",
        "POOL_SIZE": 5,
        "MAX_OVERFLOW": 10,
    }
    is_ready, issues = validate_production_readiness(prod_config)
    assert is_ready is True, f"Production readiness failed: {issues}"
    
    record_test("Readiness: Production Configuration & URL Validation", True, time.time() - t0, "Enterprise readiness confirmed")
except Exception as e:
    record_test("Readiness: Production Configuration & URL Validation", False, time.time() - t0, str(e))


# ==============================================================================
# SUMMARY
# ==============================================================================
print("\n" + "=" * 80)
total_tests = len(results)
passed_tests = sum(1 for p in results.values() if p)
failed_tests = total_tests - passed_tests
total_time = sum(timings.values())

print(f"RESULTS: {passed_tests}/{total_tests} PASSED in {total_time:.2f}s")
if failed_tests == 0:
    print("ALL TESH-QUERY MODULES & FUNCTIONS PASSED AGAINST DATABRICKS!")
else:
    print(f"FAILED TESTS: {failed_tests}")
print("=" * 80)

sys.exit(0 if failed_tests == 0 else 1)
