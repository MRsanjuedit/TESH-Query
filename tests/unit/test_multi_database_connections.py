"""
Unit tests for TESH-Query Multi-Database Connection Management.

Verifies named connection lifecycle, independent SQLAlchemy engine pools,
credential masking, environment variable resolution, and engine disposal.
"""

import pytest

from teshq.core.connection import (
    ConnectionManager,
    DatabaseConnection,
    mask_database_url,
    expand_connection_url,
)


class TestConnectionUrlSecurity:
    """Test URL parsing, masking, and environment variable expansion."""

    def test_mask_password_in_postgresql_url(self):
        url = "postgresql://myuser:SuperSecretPassword123@db.prod.internal:5432/orders_db"
        masked = mask_database_url(url)
        assert "SuperSecretPassword123" not in masked
        assert "myuser" in masked
        assert "db.prod.internal" in masked
        assert "orders_db" in masked

    def test_mask_password_in_mysql_url(self):
        url = "mysql+pymysql://admin:P@ssw0rd!@10.0.0.15:3306/analytics"
        masked = mask_database_url(url)
        assert "P@ssw0rd!" not in masked
        assert "admin" in masked

    def test_sqlite_url_preserves_path(self):
        url = "sqlite:///testing/databases/ecommerce.sqlite"
        masked = mask_database_url(url)
        assert masked == "sqlite:///testing/databases/ecommerce.sqlite"

    def test_expand_connection_url_from_environ(self, monkeypatch):
        monkeypatch.setenv("TEST_DB_HOST", "db.example.org")
        monkeypatch.setenv("TEST_DB_PORT", "5433")
        monkeypatch.setenv("TEST_DB_NAME", "warehouse")

        raw_url = "postgresql://user:pass@${TEST_DB_HOST}:${TEST_DB_PORT}/${TEST_DB_NAME}"
        expanded = expand_connection_url(raw_url)
        assert "db.example.org:5433/warehouse" in expanded

    def test_expand_connection_url_with_default_fallback(self):
        raw_url = "postgresql://user:pass@${UNDEFINED_VAR_XYZ:-localhost}:5432/mydb"
        expanded = expand_connection_url(raw_url)
        assert "localhost:5432/mydb" in expanded


class TestDatabaseConnectionDataclass:
    """Test DatabaseConnection representation and serialization safety."""

    def test_repr_never_leaks_password(self):
        secret_url = "postgresql://app_user:ultra_confidential_pass@host:5432/dbname"
        conn = DatabaseConnection(
            name="prod_pg",
            db_url=secret_url,
            dialect="postgresql",
            description="Production PG"
        )
        repr_str = repr(conn)
        str_str = str(conn)

        assert "ultra_confidential_pass" not in repr_str
        assert "ultra_confidential_pass" not in str_str
        assert "prod_pg" in repr_str

    def test_to_dict_masks_secret_by_default(self):
        secret_url = "postgresql://app_user:ultra_confidential_pass@host:5432/dbname"
        conn = DatabaseConnection(name="prod_pg", db_url=secret_url, dialect="postgresql")
        data = conn.to_dict(mask_secret=True)

        assert "ultra_confidential_pass" not in data["url"]
        assert data["name"] == "prod_pg"


class TestConnectionManagerLifecycle:
    """Test adding, retrieving, listing, testing, and removing connections."""

    @pytest.fixture
    def manager(self):
        """Create an isolated ConnectionManager instance."""
        mgr = ConnectionManager()
        # Clean existing state for clean test run
        mgr.clear_all()
        return mgr

    def test_add_and_retrieve_named_connection(self, manager, tmp_path):
        db_file = tmp_path / "source1.sqlite"
        db_file.touch()
        url = f"sqlite:///{db_file}"

        conn = manager.add_connection(name="source1", db_url=url, description="Test Source 1")
        assert conn.name == "source1"
        assert str(conn.dialect).lower() == "sqlite"

        retrieved = manager.get_named_connection("source1")
        assert retrieved.name == "source1"
        assert retrieved.db_url == url

    def test_add_duplicate_connection_raises_error(self, manager, tmp_path):
        db_file = tmp_path / "dup.sqlite"
        db_file.touch()
        url = f"sqlite:///{db_file}"

        manager.add_connection(name="dup_test", db_url=url)
        with pytest.raises(ValueError, match="already exists"):
            manager.add_connection(name="dup_test", db_url=url)

    def test_invalid_url_raises_value_error(self, manager):
        with pytest.raises(ValueError, match="Invalid database URL"):
            manager.add_connection(name="invalid", db_url="not_a_valid_url")

    def test_get_nonexistent_connection_raises_key_error(self, manager):
        with pytest.raises(KeyError, match="not found"):
            manager.get_named_connection("missing")

    def test_list_connections_returns_all_metadata(self, manager, tmp_path):
        db1 = tmp_path / "db1.sqlite"
        db2 = tmp_path / "db2.sqlite"
        db1.touch()
        db2.touch()

        manager.add_connection(name="db1", db_url=f"sqlite:///{db1}", description="DB 1")
        manager.add_connection(name="db2", db_url=f"sqlite:///{db2}", description="DB 2")

        conns = manager.list_connections()
        names = [c["name"] for c in conns]
        assert "db1" in names
        assert "db2" in names

    def test_set_and_get_default_connection(self, manager, tmp_path):
        db1 = tmp_path / "db1.sqlite"
        db1.touch()
        url = f"sqlite:///{db1}"

        manager.add_connection(name="primary", db_url=url)
        manager.set_default_connection("primary")

        default_conn = manager.get_named_connection("default")
        assert default_conn.name == "primary"

    def test_engine_pool_isolation_and_disposal(self, manager, tmp_path):
        db1 = tmp_path / "pool1.sqlite"
        db2 = tmp_path / "pool2.sqlite"
        db1.touch()
        db2.touch()

        url1 = f"sqlite:///{db1}"
        url2 = f"sqlite:///{db2}"
        manager.add_connection(name="c1", db_url=url1)
        manager.add_connection(name="c2", db_url=url2)

        engine1 = manager.get_engine(url1, engine_name="c1")
        engine2 = manager.get_engine(url2, engine_name="c2")

        # Independent engines
        assert engine1 is not engine2

        # Engine disposal on connection removal
        assert f"c1:{url1}" in manager._engines
        manager.remove_connection("c1")
        assert f"c1:{url1}" not in manager._engines

    def test_test_named_connection_success_and_failure(self, manager, tmp_path):
        valid_db = tmp_path / "valid.sqlite"
        valid_db.touch()
        manager.add_connection(name="valid_conn", db_url=f"sqlite:///{valid_db}")

        success, msg = manager.test_named_connection("valid_conn")
        assert success is True
        assert "successful" in msg.lower()

        # Nonexistent name
        success_missing, msg_missing = manager.test_named_connection("non_existent")
        assert success_missing is False
        assert "not found" in msg_missing.lower()
