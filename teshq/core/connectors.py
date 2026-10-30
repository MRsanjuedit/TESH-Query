"""
Unified database connector interface for enhanced database compatibility.

This module provides a standardized interface for connecting to various database systems
with optimized configurations and error handling for each database type.
"""

from abc import ABC, abstractmethod

from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool, StaticPool

from teshq.utils.logging import logger


class DatabaseConnector(ABC):
    """Abstract base class for database connectors."""

    @abstractmethod
    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get SQLAlchemy engine arguments specific to this database type."""
        pass

    @abstractmethod
    def test_connection_query(self) -> str:
        """Get a simple query to test database connectivity."""
        pass

    @abstractmethod
    def get_introspection_config(self) -> Dict[str, Any]:
        """Get configuration for database introspection."""
        pass

    @abstractmethod
    def normalize_url(self, url: str) -> str:
        """Normalize the database URL for this connector."""
        pass

    @abstractmethod
    def get_required_packages(self) -> List[str]:
        """Get list of required packages for this database."""
        pass

    def create_custom_engine(
        self,
        url: str,
        config: Dict[str, Any],
        **kwargs: Any,
    ) -> Optional[Engine]:
        """Optionally create a custom Engine (e.g. for Delta Lake or file-based Lakehouse engines)."""
        return None


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get PostgreSQL-specific engine arguments."""
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 10),
            "max_overflow": config.get("max_overflow", 20),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_recycle": config.get("pool_recycle", 3600),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
            "connect_args": {
                "connect_timeout": config.get("connect_timeout", 10),
                "options": "-c statement_timeout=300000",  # 5 minutes
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT version()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": True,
            "supports_check_constraints": True,
            "supports_sequences": True,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        """Ensure proper PostgreSQL URL format."""
        if url.startswith("postgres://"):
            # Update deprecated postgres:// to postgresql://
            url = url.replace("postgres://", "postgresql://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["psycopg2-binary"]


class MySQLConnector(DatabaseConnector):
    """MySQL database connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get MySQL-specific engine arguments."""
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 10),
            "max_overflow": config.get("max_overflow", 20),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_recycle": config.get("pool_recycle", 3600),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
            "connect_args": {
                "connect_timeout": config.get("connect_timeout", 10),
                "autocommit": True,
                "charset": "utf8mb4",
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT VERSION()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": True,
            "supports_check_constraints": True,
            "supports_sequences": False,  # MySQL 5.7+ supports sequences but limited
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        """Ensure proper MySQL URL format."""
        # Handle mysql+pymysql:// and mysql+mysqldb:// variants
        if url.startswith("mysql://"):
            # Default to mysql+pymysql:// for better compatibility
            url = url.replace("mysql://", "mysql+pymysql://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["PyMySQL"]


class SQLiteConnector(DatabaseConnector):
    """SQLite database connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get SQLite-specific engine arguments."""
        from sqlalchemy.pool import NullPool
        is_memory = ":memory:" in url
        return {
            "poolclass": StaticPool if is_memory else NullPool,
            "echo": config.get("echo", False),
            "connect_args": {
                "check_same_thread": False,
                "timeout": config.get("connect_timeout", 20),
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT sqlite_version()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,  # If enabled
            "supports_indexes": True,
            "supports_check_constraints": True,
            "supports_sequences": False,
            "information_schema_available": False,  # Uses pragma statements
        }

    def normalize_url(self, url: str) -> str:
        """Ensure proper SQLite URL format."""
        return url

    def get_required_packages(self) -> List[str]:
        return []  # SQLite is built into Python


class OracleConnector(DatabaseConnector):
    """Oracle database connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get Oracle-specific engine arguments."""
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 10),
            "max_overflow": config.get("max_overflow", 20),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_recycle": config.get("pool_recycle", 3600),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
            "connect_args": {
                "timeout": config.get("connect_timeout", 10),
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT 1 FROM DUAL"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": True,
            "supports_check_constraints": True,
            "supports_sequences": True,
            "information_schema_available": False,  # Uses Oracle data dictionary
        }

    def normalize_url(self, url: str) -> str:
        """Ensure proper Oracle URL format."""
        # Handle oracle+cx_oracle:// variants
        if url.startswith("oracle://"):
            url = url.replace("oracle://", "oracle+cx_oracle://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["cx_Oracle", "oracledb"]


class CassandraConnector(DatabaseConnector):
    """Cassandra database connector (via SQLAlchemy CQL)."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Get Cassandra-specific engine arguments."""
        return {
            "echo": config.get("echo", False),
            "connect_args": {
                "timeout": config.get("connect_timeout", 10),
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT release_version FROM system.local"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": True,  # Secondary indexes
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": False,  # Uses Cassandra system tables
        }

    def normalize_url(self, url: str) -> str:
        """Ensure proper Cassandra URL format."""
        if url.startswith("cassandra://"):
            url = url.replace("cassandra://", "cassandra+cqlalchemy://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["cassandra-driver", "cqlalchemy"]


class BigQueryConnector(DatabaseConnector):
    """Google BigQuery connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["sqlalchemy-bigquery", "google-cloud-bigquery"]


class SnowflakeConnector(DatabaseConnector):
    """Snowflake data warehouse connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
            "connect_args": {
                "client_session_keep_alive": True,
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT CURRENT_VERSION()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": True,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["snowflake-sqlalchemy", "snowflake-connector-python"]


class DuckDBConnector(DatabaseConnector):
    """DuckDB embedded analytical database connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT version()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": True,
            "supports_check_constraints": True,
            "supports_sequences": True,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["duckdb-engine", "duckdb"]


class ClickHouseConnector(DatabaseConnector):
    """ClickHouse analytical database connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import QueuePool
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 5),
            "pool_timeout": config.get("pool_timeout", 30),
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT version()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        if url.startswith("clickhouse://"):
            url = url.replace("clickhouse://", "clickhouse+native://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["clickhouse-connect", "clickhouse-sqlalchemy"]


class RedshiftConnector(DatabaseConnector):
    """Amazon Redshift connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 10),
            "max_overflow": config.get("max_overflow", 20),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
            "connect_args": {
                "connect_timeout": config.get("connect_timeout", 10),
            }
        }

    def test_connection_query(self) -> str:
        return "SELECT version()"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        if url.startswith("redshift://"):
            url = url.replace("redshift://", "redshift+psycopg2://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["sqlalchemy-redshift", "psycopg2-binary"]


class DatabricksConnector(DatabaseConnector):
    """Databricks SQL connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import QueuePool
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 5),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["databricks-sql-connector", "databricks-sqlalchemy"]


class MSSQLConnector(DatabaseConnector):
    """Microsoft SQL Server connector (via pymssql)."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 10),
            "max_overflow": config.get("max_overflow", 20),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT @@VERSION"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": True,
            "supports_check_constraints": True,
            "supports_sequences": True,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        """Ensure proper MSSQL URL format (default to pymssql if no driver specified)."""
        if url.startswith("mssql://"):
            url = url.replace("mssql://", "mssql+pymssql://", 1)
        elif url.startswith("sqlserver://"):
            url = url.replace("sqlserver://", "mssql+pymssql://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["pymssql"]


class TrinoConnector(DatabaseConnector):
    """Trino distributed query engine connector for data lakehouses and federated warehouses."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import QueuePool
        return {
            "poolclass": QueuePool,
            "pool_size": config.get("pool_size", 5),
            "max_overflow": config.get("max_overflow", 10),
            "pool_timeout": config.get("pool_timeout", 30),
            "pool_pre_ping": config.get("pool_pre_ping", True),
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["trino", "sqlalchemy-trino"]


class PrestoConnector(DatabaseConnector):
    """Presto distributed SQL query engine connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["pyhive", "presto-python-client"]


class AthenaConnector(DatabaseConnector):
    """Amazon Athena lakehouse query engine connector."""

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        if url.startswith("athena://"):
            url = url.replace("athena://", "awsathena+rest://", 1)
        return url

    def get_required_packages(self) -> List[str]:
        return ["PyAthena"]


class DeltaLakeConnector(DatabaseConnector):
    """Delta Lake table and lakehouse connector.

    Mounts Delta Lake tables (local directories or cloud object storage)
    into a high-performance in-memory DuckDB query engine, enabling full SQL
    queries, schema introspection, and LLM query generation over Delta tables.
    """

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["duckdb", "deltalake"]

    @staticmethod
    def _parse_lake_path(url: str) -> str:
        raw = url
        for prefix in ("deltalake:///", "delta:///", "deltalake://", "delta://"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                if prefix.endswith("///") and not raw.startswith("/"):
                    raw = "/" + raw
                break
        return raw

    def create_custom_engine(
        self,
        url: str,
        config: Dict[str, Any],
        **kwargs: Any,
    ) -> Optional[Engine]:
        import os
        import re
        from pathlib import Path
        from sqlalchemy import create_engine as _sqla_create_engine, event

        raw_path = self._parse_lake_path(url)
        delta_tables: Dict[str, str] = {}
        is_cloud = raw_path.startswith(("s3://", "gcs://", "http://", "https://", "abfss://", "azure://"))

        if is_cloud:
            tbl_name = re.sub(r"[^a-zA-Z0-9_]", "_", raw_path.rstrip("/").split("/")[-1]) or "delta_table"
            delta_tables[tbl_name] = raw_path
        else:
            p = Path(raw_path).resolve()
            if (p / "_delta_log").exists():
                safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name) or "delta_table"
                delta_tables[safe_name] = str(p)
            elif p.is_dir():
                for sub in p.iterdir():
                    if sub.is_dir() and (sub / "_delta_log").exists():
                        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", sub.name)
                        delta_tables[safe_name] = str(sub.resolve())
            if not delta_tables:
                safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name) or "delta_table"
                delta_tables[safe_name] = str(p)

        eng = _sqla_create_engine("duckdb:///:memory:", echo=config.get("echo", False))

        @event.listens_for(eng, "connect")
        def _setup_delta(dbapi_con, rec):
            has_delta_ext = False
            try:
                dbapi_con.execute("LOAD delta;")
                has_delta_ext = True
            except Exception:
                try:
                    dbapi_con.execute("INSTALL delta; LOAD delta;")
                    has_delta_ext = True
                except Exception:
                    has_delta_ext = False

            if is_cloud:
                try:
                    dbapi_con.execute("LOAD httpfs;")
                except Exception:
                    try:
                        dbapi_con.execute("INSTALL httpfs; LOAD httpfs;")
                    except Exception:
                        pass

            for tbl_name, tbl_path in delta_tables.items():
                mounted = False
                if has_delta_ext:
                    try:
                        dbapi_con.execute(f"CREATE OR REPLACE VIEW {tbl_name} AS SELECT * FROM delta_scan('{tbl_path}')")
                        mounted = True
                    except Exception:
                        mounted = False

                if not mounted:
                    try:
                        from deltalake import DeltaTable
                        dt = DeltaTable(tbl_path)
                        arrow_ds = dt.to_pyarrow_dataset()
                        if hasattr(dbapi_con, "register"):
                            dbapi_con.register(tbl_name, arrow_ds)
                            mounted = True
                        elif hasattr(dbapi_con, "cursor"):
                            dbapi_con.cursor().register(tbl_name, arrow_ds)
                            mounted = True
                    except Exception:
                        mounted = False

                if not mounted:
                    try:
                        p_glob = os.path.join(tbl_path, "*.parquet")
                        dbapi_con.execute(f"CREATE OR REPLACE VIEW {tbl_name} AS SELECT * FROM read_parquet('{p_glob}')")
                    except Exception:
                        pass

        return eng


class LakehouseConnector(DatabaseConnector):
    """Data lakehouse connector for Parquet datasets and directories.

    Mounts Parquet datasets and directories into DuckDB as relational views
    for instant schema reflection, querying, and AI SQL generation.
    """

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool
        return {
            "poolclass": NullPool,
            "echo": config.get("echo", False),
        }

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": False,
            "supports_indexes": False,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": True,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return ["duckdb", "pyarrow"]

    @staticmethod
    def _parse_path(url: str) -> str:
        raw = url
        for prefix in ("lakehouse:///", "parquet:///", "lakehouse://", "parquet://"):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                if prefix.endswith("///") and not raw.startswith("/"):
                    raw = "/" + raw
                break
        return raw

    def create_custom_engine(
        self,
        url: str,
        config: Dict[str, Any],
        **kwargs: Any,
    ) -> Optional[Engine]:
        import re
        from pathlib import Path
        from sqlalchemy import create_engine as _sqla_create_engine, event

        raw_path = self._parse_path(url)
        parquet_tables: Dict[str, str] = {}
        is_cloud = raw_path.startswith(("s3://", "gcs://", "http://", "https://", "abfss://"))

        if is_cloud:
            tbl_name = re.sub(r"[^a-zA-Z0-9_]", "_", raw_path.rstrip("/").split("/")[-1]) or "lakehouse_data"
            parquet_tables[tbl_name] = raw_path
        else:
            p = Path(raw_path).resolve()
            if p.is_file() or raw_path.endswith(".parquet"):
                safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.stem) or "dataset"
                parquet_tables[safe_name] = str(p)
            elif p.is_dir():
                for item in p.glob("*.parquet"):
                    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", item.stem)
                    parquet_tables[safe_name] = str(item.resolve())
                for sub in p.iterdir():
                    if sub.is_dir() and any(sub.glob("*.parquet")):
                        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", sub.name)
                        parquet_tables[safe_name] = str((sub / "*.parquet").resolve())
            if not parquet_tables:
                safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", p.name) or "lakehouse_data"
                parquet_tables[safe_name] = str(p)

        eng = _sqla_create_engine("duckdb:///:memory:", echo=config.get("echo", False))

        @event.listens_for(eng, "connect")
        def _setup_parquet(dbapi_con, rec):
            if is_cloud:
                try:
                    dbapi_con.execute("LOAD httpfs;")
                except Exception:
                    try:
                        dbapi_con.execute("INSTALL httpfs; LOAD httpfs;")
                    except Exception:
                        pass
            for tbl_name, tbl_path in parquet_tables.items():
                try:
                    dbapi_con.execute(
                        f"CREATE OR REPLACE VIEW {tbl_name} AS SELECT * FROM read_parquet('{tbl_path}')"
                    )
                except Exception as e:
                    logger.warning(f"Could not mount parquet view for {tbl_name} from {tbl_path}: {e}")

        return eng


class GenericSQLAlchemyConnector(DatabaseConnector):
    """Fallback connector for any SQLAlchemy-supported database.

    Provides sensible defaults (QueuePool, standard timeouts, SELECT 1 health check)
    for databases that don't have a specialized connector registered. This allows
    BigQuery, Snowflake, DuckDB, Redshift, ClickHouse, CockroachDB, etc. to work
    out of the box as long as the user has the appropriate SQLAlchemy dialect driver
    installed (e.g. ``pip install sqlalchemy-bigquery``).
    """

    def get_engine_args(self, url: str, config: Dict[str, Any]) -> Dict[str, Any]:
        from sqlalchemy.pool import NullPool, StaticPool
        is_embedded = ":memory:" in url or "sqlite" in url or "duckdb" in url
        pool_cls = StaticPool if ":memory:" in url else (NullPool if is_embedded else QueuePool)
        args: Dict[str, Any] = {
            "echo": config.get("echo", False),
        }
        if pool_cls == QueuePool:
            args.update({
                "poolclass": QueuePool,
                "pool_size": config.get("pool_size", 5),
                "max_overflow": config.get("max_overflow", 10),
                "pool_timeout": config.get("pool_timeout", 30),
                "pool_recycle": config.get("pool_recycle", 3600),
                "pool_pre_ping": config.get("pool_pre_ping", True),
            })
        else:
            args["poolclass"] = pool_cls
            if "sqlite" in url:
                args["connect_args"] = {"check_same_thread": False}
        return args

    def test_connection_query(self) -> str:
        return "SELECT 1"

    def get_introspection_config(self) -> Dict[str, Any]:
        return {
            "supports_foreign_keys": True,
            "supports_indexes": True,
            "supports_check_constraints": False,
            "supports_sequences": False,
            "information_schema_available": False,
        }

    def normalize_url(self, url: str) -> str:
        return url

    def get_required_packages(self) -> List[str]:
        return []


class UnifiedDatabaseConnector:
    """
    Unified interface for connecting to various database systems.

    Provides a consistent API while handling database-specific configurations,
    optimizations, and error handling.
    """

    # Registry of supported database connectors (ordered by analytical relevance)
    _connectors = {
        # Tier 1: Cloud Data Warehouses
        "bigquery": BigQueryConnector(),
        "snowflake": SnowflakeConnector(),
        "databricks": DatabricksConnector(),
        "redshift": RedshiftConnector(),
        "trino": TrinoConnector(),
        "presto": PrestoConnector(),
        "athena": AthenaConnector(),

        # Tier 2: Lakehouses, Lakebases & OLAP
        "delta": DeltaLakeConnector(),
        "deltalake": DeltaLakeConnector(),
        "lakehouse": LakehouseConnector(),
        "parquet": LakehouseConnector(),
        "duckdb": DuckDBConnector(),
        "clickhouse": ClickHouseConnector(),

        # Tier 3: Operational RDBMS
        "postgresql": PostgreSQLConnector(),
        "postgres": PostgreSQLConnector(),  # Alias
        "mysql": MySQLConnector(),
        "mssql": MSSQLConnector(),
        "sqlserver": MSSQLConnector(),      # Alias
        "oracle": OracleConnector(),

        # Tier 4: Local development & NoSQL
        "sqlite": SQLiteConnector(),
        "cassandra": CassandraConnector(),
    }

    @classmethod
    def get_categorized_connectors(cls) -> Dict[str, List[str]]:
        """Get supported databases grouped by category."""
        return {
            "Cloud Data Warehouses": ["bigquery", "snowflake", "databricks", "redshift", "trino", "presto", "athena"],
            "Lakehouses & Lakebases": ["delta", "lakehouse", "duckdb", "clickhouse"],
            "Relational Databases": ["postgresql", "mysql", "mssql", "oracle", "sqlite"],
            "NoSQL & Specialized": ["cassandra"],
        }

    @classmethod
    def get_supported_databases(cls) -> List[str]:
        """Get list of supported database types."""
        return list(cls._connectors.keys())

    @classmethod
    def detect_database_type(cls, url: str) -> str:
        """Detect database type from URL for any SQLAlchemy database."""
        try:
            from sqlalchemy.engine import make_url
            scheme = make_url(url).get_backend_name().lower()
        except Exception:
            if "://" in url:
                scheme = url.split("://")[0].lower().split("+")[0]
            else:
                scheme = urlparse(url).scheme.lower().split("+")[0]

        # Handle aliases
        if scheme in ("postgres", "cockroachdb"):
            scheme = "postgresql"
        elif scheme in ("sqlserver", "mssql"):
            scheme = "mssql"
        elif scheme == "mariadb":
            scheme = "mysql"
        elif scheme in ("delta", "deltalake"):
            scheme = "delta"
        elif scheme in ("lakehouse", "parquet"):
            scheme = "lakehouse"
        elif scheme in ("awsathena", "athena"):
            scheme = "athena"

        return scheme

    @classmethod
    def get_connector(cls, url: str) -> DatabaseConnector:
        """Get appropriate connector for database URL.

        Returns the optimized connector for known database types, or a
        GenericSQLAlchemyConnector for any unknown database — allowing
        SQLAlchemy-supported databases to work out of the box.
        """
        db_type = cls.detect_database_type(url)

        if db_type not in cls._connectors:
            logger.info(
                f"No optimized connector for '{db_type}' — using generic SQLAlchemy defaults. "
                f"Install the appropriate SQLAlchemy dialect driver for full support."
            )
            return GenericSQLAlchemyConnector()

        return cls._connectors[db_type]

    @classmethod
    def create_engine(
        cls,
        url: str,
        config: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Engine:
        """
        Create a SQLAlchemy engine with database-specific optimizations.

        Args:
            url: Database connection URL
            config: Configuration dictionary
            **kwargs: Additional engine arguments

        Returns:
            Configured SQLAlchemy engine

        Raises:
            ValueError: If database type is not supported
            ImportError: If required database driver is not installed
        """
        config = config or {}
        connector = cls.get_connector(url)

        # Check for custom engine provider (e.g. Delta Lake, Parquet Lakehouse)
        custom_engine = connector.create_custom_engine(url, config, **kwargs)
        if custom_engine is not None:
            logger.info(
                "Creating specialized data engine",
                database_type=cls.detect_database_type(url),
            )
            return custom_engine

        # Check for required packages
        required_packages = connector.get_required_packages()
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
            except ImportError:
                logger.warning(
                    f"Optional database driver not installed: {package}. "
                    f"Install with: pip install {package}"
                )

        # Normalize URL
        normalized_url = connector.normalize_url(url)

        # Get database-specific engine arguments
        engine_args = connector.get_engine_args(normalized_url, config)

        # Merge with any additional kwargs
        engine_args.update(kwargs)

        logger.info(
            "Creating database engine",
            database_type=cls.detect_database_type(url),
            pool_class=engine_args.get("poolclass", "default").__name__ if engine_args.get("poolclass") else "default"
        )

        return create_engine(normalized_url, **engine_args)

    @classmethod
    def test_connection(cls, url: str, config: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
        """
        Test database connectivity.

        Args:
            url: Database connection URL
            config: Configuration dictionary

        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            connector = cls.get_connector(url)
            engine = cls.create_engine(url, config)

            test_query = connector.test_connection_query()

            with engine.connect() as conn:
                try:
                    result = conn.execute(text(test_query))
                    version_info = result.fetchone()
                except Exception:
                    # Fallback to alternative dialect test queries
                    for fallback in ("SELECT 1 FROM DUAL", "SELECT 1", "SELECT version()", "SELECT sqlite_version()"):
                        try:
                            result = conn.execute(text(fallback))
                            version_info = result.fetchone()
                            break
                        except Exception:
                            continue
                    else:
                        raise

            engine.dispose()

            return True, f"Connection successful. Database version: {version_info[0] if version_info else 'Unknown'}"

        except ValueError as e:
            return False, f"Configuration error: {e}"
        except ImportError as e:
            return False, f"Missing database driver: {e}"
        except SQLAlchemyError as e:
            return False, f"Database connection failed: {e}"
        except Exception as e:
            return False, f"Unexpected error: {e}"

    @classmethod
    def get_introspection_capabilities(cls, url: str) -> Dict[str, Any]:
        """Get introspection capabilities for the database type."""
        connector = cls.get_connector(url)
        return connector.get_introspection_config()

    @classmethod
    def get_database_info(cls, url: str) -> Dict[str, Any]:
        """Get comprehensive information about the database and connection."""
        try:
            db_type = cls.detect_database_type(url)
            connector = cls.get_connector(url)

            return {
                "database_type": db_type,
                "supported": True,
                "required_packages": connector.get_required_packages(),
                "introspection_capabilities": connector.get_introspection_config(),
                "normalized_url": connector.normalize_url(url),
            }
        except ValueError:
            return {
                "database_type": cls.detect_database_type(url),
                "supported": False,
                "error": "Database type not supported",
                "supported_types": cls.get_supported_databases(),
            }
