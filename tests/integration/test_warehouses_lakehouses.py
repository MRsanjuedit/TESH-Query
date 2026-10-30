"""
Integration tests for Cloud Data Warehouses, Lakehouses, Lakebases, and Delta Lake.

Tests support for:
- Delta Lake tables and directories (via DuckDB and deltalake delta-rs)
- Parquet file datasets and directories (Data Lakes)
- Warehouse connectors: Snowflake, BigQuery, Databricks, Redshift, ClickHouse, Trino, Presto, Athena
- Data Engineering export formats: Parquet, JSONL
- Dialect rules and hints for modern data platforms
"""

import os
import stat
import pandas as pd
import pytest

from teshq.core.connectors import (
    AthenaConnector,
    BigQueryConnector,
    ClickHouseConnector,
    DatabricksConnector,
    PrestoConnector,
    RedshiftConnector,
    SnowflakeConnector,
    TrinoConnector,
    UnifiedDatabaseConnector,
)
from teshq.core.connection import execute_query_with_pooling, mask_database_url
from teshq.core.dialect import (
    SQLDialect,
    detect_dialect,
    get_dialect_display_name,
    get_dialect_hints,
    get_dialect_rules,
)
from teshq.core.introspect import introspect_db
from teshq.core.validation import CLIValidator, ConfigValidator
from teshq.utils.save import save_to_jsonl, save_to_parquet


@pytest.fixture(scope="module")
def sample_delta_lake(tmp_path_factory):
    """Create a temporary multi-table Delta Lake directory with sample tables."""
    base_dir = tmp_path_factory.mktemp("delta_lake")
    orders_dir = base_dir / "orders"
    customers_dir = base_dir / "customers"

    from deltalake import write_deltalake

    df_orders = pd.DataFrame({
        "order_id": [1001, 1002, 1003],
        "customer_id": [1, 2, 1],
        "amount": [150.50, 89.99, 210.00],
    })
    write_deltalake(str(orders_dir), df_orders, mode="overwrite")

    df_customers = pd.DataFrame({
        "customer_id": [1, 2],
        "name": ["Alice Smith", "Bob Jones"],
        "tier": ["Gold", "Silver"],
    })
    write_deltalake(str(customers_dir), df_customers, mode="overwrite")

    return base_dir


@pytest.fixture(scope="module")
def sample_parquet_lake(tmp_path_factory):
    """Create a temporary data lake of Parquet files and partitioned folders."""
    base_dir = tmp_path_factory.mktemp("parquet_lake")
    products_dir = base_dir / "products"
    products_dir.mkdir(parents=True, exist_ok=True)

    df_products = pd.DataFrame({
        "sku": ["PROD-01", "PROD-02", "PROD-03"],
        "category": ["Electronics", "Books", "Electronics"],
        "price": [299.99, 19.99, 49.99],
    })
    df_products.to_parquet(str(products_dir / "data.parquet"), index=False)

    inventory_file = base_dir / "inventory.parquet"
    df_inventory = pd.DataFrame({
        "sku": ["PROD-01", "PROD-02"],
        "quantity": [100, 450],
    })
    df_inventory.to_parquet(str(inventory_file), index=False)

    return base_dir


class TestDeltaLakeIntegration:
    """Test first-class Delta Lake integration in TESH-Query."""

    def test_delta_lake_dialect_detection(self, sample_delta_lake):
        url = f"delta://{sample_delta_lake}"
        dialect = detect_dialect(url)
        assert dialect == SQLDialect.DELTALAKE
        assert get_dialect_display_name(dialect) == "Delta Lake"

    def test_delta_lake_connection_validation(self, sample_delta_lake):
        url = f"delta://{sample_delta_lake}"
        is_valid, msg = ConfigValidator.validate_database_url(url)
        assert is_valid, f"Validation failed: {msg}"

        success, msg = UnifiedDatabaseConnector.test_connection(url)
        assert success, f"Connection test failed: {msg}"

    def test_delta_lake_schema_introspection(self, sample_delta_lake):
        url = f"delta://{sample_delta_lake}"
        schema = introspect_db(url)

        assert "tables" in schema
        tables = schema["tables"]
        assert "orders" in tables
        assert "customers" in tables

        # Verify columns
        order_cols = [c["name"] for c in tables["orders"]["columns"]]
        assert "order_id" in order_cols
        assert "amount" in order_cols
        assert tables["orders"]["row_count"] == 3

        cust_cols = [c["name"] for c in tables["customers"]["columns"]]
        assert "customer_id" in cust_cols
        assert "name" in cust_cols
        assert tables["customers"]["row_count"] == 2

    def test_delta_lake_query_execution(self, sample_delta_lake):
        url = f"delta://{sample_delta_lake}"
        query = (
            "SELECT c.name, SUM(o.amount) AS total_spent "
            "FROM customers c "
            "JOIN orders o ON c.customer_id = o.customer_id "
            "GROUP BY c.name "
            "ORDER BY total_spent DESC"
        )
        rows = execute_query_with_pooling(url, query)
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice Smith"
        assert abs(rows[0]["total_spent"] - 360.50) < 0.01

    def test_single_delta_table_direct_url(self, sample_delta_lake):
        table_path = sample_delta_lake / "orders"
        url = f"delta://{table_path}"
        schema = introspect_db(url)
        assert "orders" in schema["tables"]

        rows = execute_query_with_pooling(url, "SELECT COUNT(*) AS cnt FROM orders")
        assert rows[0]["cnt"] == 3


class TestParquetLakehouseIntegration:
    """Test Parquet Data Lake / Lakehouse integration."""

    def test_parquet_lakehouse_dialect_detection(self, sample_parquet_lake):
        url = f"lakehouse://{sample_parquet_lake}"
        dialect = detect_dialect(url)
        assert dialect == SQLDialect.DELTALAKE

    def test_parquet_lakehouse_introspection_and_query(self, sample_parquet_lake):
        url = f"lakehouse://{sample_parquet_lake}"
        schema = introspect_db(url)

        assert "products" in schema["tables"]
        assert "inventory" in schema["tables"]

        # Run query joining parquet datasets
        query = (
            "SELECT p.sku, p.price, i.quantity, (p.price * i.quantity) AS inventory_value "
            "FROM products p "
            "JOIN inventory i ON p.sku = i.sku "
            "ORDER BY inventory_value DESC"
        )
        rows = execute_query_with_pooling(url, query)
        assert len(rows) == 2
        assert rows[0]["sku"] == "PROD-01"


class TestCloudWarehousesConnectors:
    """Test configuration, engine arguments, and safety for Cloud Warehouses."""

    def test_snowflake_connector(self):
        conn = SnowflakeConnector()
        args = conn.get_engine_args("snowflake://user:pass@account/db/schema", {})
        assert "poolclass" in args
        assert conn.test_connection_query() == "SELECT CURRENT_VERSION()"
        assert conn.get_introspection_config()["supports_foreign_keys"] is True
        assert "snowflake-sqlalchemy" in conn.get_required_packages()

    def test_bigquery_connector(self):
        conn = BigQueryConnector()
        args = conn.get_engine_args("bigquery://my-project/my_dataset", {})
        assert "poolclass" in args
        assert conn.test_connection_query() == "SELECT 1"
        assert "sqlalchemy-bigquery" in conn.get_required_packages()

    def test_databricks_connector(self):
        conn = DatabricksConnector()
        args = conn.get_engine_args("databricks://token:dapi123@host?http_path=path", {})
        assert "poolclass" in args
        assert conn.test_connection_query() == "SELECT 1"
        assert "databricks-sql-connector" in conn.get_required_packages()

    def test_redshift_connector(self):
        conn = RedshiftConnector()
        norm_url = conn.normalize_url("redshift://user:pass@host:5439/db")
        assert norm_url.startswith("redshift+psycopg2://")
        assert conn.test_connection_query() == "SELECT version()"

    def test_clickhouse_connector(self):
        conn = ClickHouseConnector()
        norm_url = conn.normalize_url("clickhouse://default:pass@localhost:9000/default")
        assert norm_url.startswith("clickhouse+native://")
        assert conn.test_connection_query() == "SELECT version()"

    def test_trino_connector(self):
        conn = TrinoConnector()
        assert conn.test_connection_query() == "SELECT 1"
        assert conn.get_introspection_config()["information_schema_available"] is True
        assert "trino" in conn.get_required_packages()

    def test_presto_connector(self):
        conn = PrestoConnector()
        assert conn.test_connection_query() == "SELECT 1"
        assert "pyhive" in conn.get_required_packages()

    def test_athena_connector(self):
        conn = AthenaConnector()
        norm = conn.normalize_url("athena://:@athena.us-east-1.amazonaws.com/default")
        assert norm.startswith("awsathena+rest://")
        assert conn.test_connection_query() == "SELECT 1"


class TestDataEngineeringExports:
    """Test high-performance data engineering exports (Parquet, JSONL)."""

    def test_save_to_parquet(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        target_file = tmp_path / "test.parquet"

        save_to_parquet(df, str(target_file))
        assert target_file.exists()

        # Check permission mode (0o600: rw-------)
        file_mode = stat.S_IMODE(os.stat(target_file).st_mode)
        assert file_mode & 0o077 == 0, f"Permissions too open: {oct(file_mode)}"

        # Verify content
        loaded = pd.read_parquet(str(target_file))
        assert len(loaded) == 3
        assert list(loaded["id"]) == [1, 2, 3]

    def test_save_to_jsonl(self, tmp_path):
        df = pd.DataFrame({"sku": ["S1", "S2"], "qty": [10, 20]})
        target_file = tmp_path / "test.jsonl"

        save_to_jsonl(df, str(target_file))
        assert target_file.exists()

        file_mode = stat.S_IMODE(os.stat(target_file).st_mode)
        assert file_mode & 0o077 == 0, f"Permissions too open: {oct(file_mode)}"

        loaded = pd.read_json(str(target_file), lines=True)
        assert len(loaded) == 2
        assert list(loaded["sku"]) == ["S1", "S2"]

    def test_validation_accepts_parquet_and_jsonl(self):
        valid, _ = CLIValidator.validate_output_format("parquet")
        assert valid
        valid, _ = CLIValidator.validate_output_format("jsonl")
        assert valid

        valid, _ = CLIValidator.validate_save_path("data.parquet", "parquet")
        assert valid
        valid, _ = CLIValidator.validate_save_path("data.jsonl", "jsonl")
        assert valid

        invalid, _ = CLIValidator.validate_save_path("data.txt", "parquet")
        assert not invalid


class TestDialectRulesAndHints:
    """Test dialect SQL generation rules and hints for Lakehouses & Warehouses."""

    def test_delta_lake_dialect_rules_and_hints(self):
        rules = get_dialect_rules(SQLDialect.DELTALAKE)
        assert "Delta Lake-specific rules" in rules
        assert "LIMIT N" in rules

        hints = get_dialect_hints(SQLDialect.DELTALAKE)
        assert "Delta Lake function reference" in hints
        assert "DATEDIFF" in hints

    def test_trino_dialect_rules_and_hints(self):
        rules = get_dialect_rules(SQLDialect.TRINO)
        assert "Trino-specific rules" in rules
        assert "catalog.schema.table" in rules

        hints = get_dialect_hints(SQLDialect.TRINO)
        assert "Trino function reference" in hints

    def test_presto_and_athena_rules(self):
        rules_presto = get_dialect_rules(SQLDialect.PRESTO)
        assert "Presto-specific rules" in rules_presto

        rules_athena = get_dialect_rules(SQLDialect.ATHENA)
        assert "Amazon Athena-specific rules" in rules_athena
        assert "partition" in rules_athena

    def test_warehouse_credentials_masking(self):
        # Databricks PAT token
        masked = mask_database_url("databricks://token:dapi_super_secret_pat_token@workspace.cloud")
        assert "dapi_super_secret_pat_token" not in masked
        assert "***" in masked

        # Snowflake account and password
        masked_sf = mask_database_url("snowflake://wh_user:MySecurePass123!@xy12345.us-east-1/DB/SCHEMA")
        assert "MySecurePass123!" not in masked_sf
        assert "***" in masked_sf
