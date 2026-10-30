# Universal Database Dialects, Warehouses & Lakehouses

TESH-Query provides enterprise-grade support for **every database supported by SQLAlchemy**, as well as native in-process engines for **Delta Lake**, **Parquet Lakehouses**, and **Distributed Cloud Warehouses**. Whether connecting to an enterprise relational database, an in-process analytical engine, a cloud data warehouse, or cloud storage buckets, TESH-Query automatically identifies the backend, selects the appropriate dialect rules, applies optimized connection pooling, redacts sensitive credentials, and handles dialect-specific query execution.

---

## 🌐 Supported Dialects & Connection Matrix

| Category | Database / Engine | Driver / Packages | Example Connection URL Format |
| :--- | :--- | :--- | :--- |
| **Cloud Data Warehouses** | **Databricks SQL** | `databricks-sql-connector`, `databricks-sqlalchemy` | `databricks://token:<PAT>@<host>:443/<schema>?http_path=<path>&catalog=<catalog>` |
| | **Snowflake** | `snowflake-sqlalchemy` | `snowflake://user:pass@account/dbname/schemaname?warehouse=wh` |
| | **Google BigQuery** | `sqlalchemy-bigquery` | `bigquery://project-id/dataset_name` |
| | **Amazon Redshift** | `sqlalchemy-redshift` | `redshift+psycopg2://user:pass@cluster:5439/dbname` |
| | **Amazon Athena** | `PyAthena` | `athena://:@athena.<region>.amazonaws.com:443/<db>?s3_staging_dir=s3://...` |
| | **Trino / Starburst** | `sqlalchemy-trino`, `trino` | `trino://user@host:8080/catalog/schema` |
| | **Presto** | `pyhive`, `presto-python-client` | `presto://user@host:8080/catalog/schema` |
| **Lakehouses & Formats** | **Delta Lake** | `deltalake`, `duckdb` | `delta:///path/to/table` or `delta:///path/to/lakehouse_root` |
| | **Parquet Lakehouse** | `duckdb`, `pyarrow` | `lakehouse:///path/to/folder` or `parquet:///path/to/folder` |
| **Real-Time OLAP** | **DuckDB / MotherDuck**| `duckdb-engine` | `duckdb:///analytics.duckdb` or `duckdb:///md:my_db` |
| | **ClickHouse** | `clickhouse-connect` | `clickhouse+native://user:pass@host:9000/default` |
| **Relational Databases** | **PostgreSQL** | `asyncpg` / `psycopg2` | `postgresql+asyncpg://user:pass@host:5432/dbname` |
| | **MySQL / MariaDB** | `PyMySQL` | `mysql+pymysql://user:pass@host:3306/dbname` |
| | **MS SQL Server** | `pymssql` / `pyodbc` | `mssql+pymssql://user:pass@host:1433/dbname` |
| | **Oracle Database** | `oracledb` / `cx_oracle` | `oracle+oracledb://user:pass@host:1521/?service_name=XEPDB1` |
| | **SQLite** | Built-in | `sqlite:///path/to/database.db` or `sqlite:///:memory:` |
| **Distributed SQL** | **CockroachDB** | `cockroachdb` | `cockroachdb://user:pass@host:26257/defaultdb` |
| **Generic / Others** | **Any SQLAlchemy DB**| Any 3rd party driver | `<driver_scheme>://[user:pass@]host[:port]/database` |

---

## 🧱 Cloud Data Warehouses

### 1. Databricks SQL & Unity Catalog
TESH-Query connects to Databricks SQL Warehouses and compute clusters using the official `databricks-sql-connector` and `databricks-sqlalchemy` dialect.

```bash
# Connection URL format
databricks://token:<PAT>@<server_hostname>:443/<schema>?http_path=<http_path>&catalog=<catalog>
```

#### Connection Details:
- **Server Hostname**: Your workspace host (e.g., `dbc-xxxx-yyyy.cloud.databricks.com` or `adb-xxxx.12.azuredatabricks.net`).
- **HTTP Path**: Found under **SQL Warehouses** $\rightarrow$ **Connection Details** (e.g., `/sql/1.0/warehouses/36e71f1d332b5189`).
- **Personal Access Token (PAT)**: Generated in Databricks **User Settings** $\rightarrow$ **Developer** $\rightarrow$ **Access Tokens** (`dapi...`).
- **Unity Catalog 3-Level Names**: TESH-Query natively parses 3-level catalog paths (`catalog.schema.table`) like `ipl_analytics.gold.team_standings`.
- **Token Masking**: Credentials and tokens (`token:dapi...`) are automatically redacted in logs and CLI outputs.

```bash
# Configure interactively
teshq config --db
# Select 'databricks' and follow the guided prompts

# Validate and explore schema
teshq config validate
teshq db explore
teshq db preview gold.team_standings -n 5
```

---

### 2. Trino & Presto
TESH-Query connects to distributed SQL federation engines via `trino://` and `presto://`:

```bash
# Trino connection format
trino://<user>@<host>:8080/<catalog>/<schema>

# Example with SSL and LDAP authentication:
trino://analyst:secret@trino.internal.net:8443/iceberg/analytics?ssl=true
```

- **Features**: Generates Trino-specific functions (`DATE_ADD('day', N, d)`, `REGEXP_EXTRACT`, `JSON_EXTRACT_SCALAR`), supports cross-catalog joins, and automatically disables transactional statement timeouts.

---

### 3. Amazon Athena
Query Amazon S3 data lakes serverlessly using AWS Athena:

```bash
# Athena connection format
athena://:@athena.<region>.amazonaws.com:443/<database>?s3_staging_dir=s3://<query-results-bucket>/results/

# Example
athena://:@athena.us-east-1.amazonaws.com:443/default?s3_staging_dir=s3://my-athena-bucket/staging/
```

---

## 🌊 Lakehouses & Data Engineering Formats

Standard SQLAlchemy expects a live SQL server and cannot parse file-based Lakehouses or Delta transaction logs. TESH-Query bridges this gap by bundling an **in-process vectorized analytical engine** powered by **DuckDB + delta-rs (`deltalake`)**.

```
                   ┌──────────────────────────────────────────────┐
                   │           TESH-Query Engine / CLI            │
                   └──────────────────────┬───────────────────────┘
                                          │
                  ┌───────────────────────┴───────────────────────┐
                  ▼                                               ▼
    ┌───────────────────────────┐                   ┌───────────────────────────┐
    │     Delta Lake Engine     │                   │     Parquet Lakehouse     │
    │   (delta://, deltalake://)│                   │   (lakehouse://, parquet://)│
    ├───────────────────────────┤                   ├───────────────────────────┤
    │ • Inspects _delta_log/    │                   │ • Auto-discovers *.parquet│
    │ • ACID transaction logs  │                   │ • Scans Hive partitions   │
    │ • Vectorized Delta scans  │                   │ • Vectorized DuckDB views │
    │ • Local or S3/ADLS/GCS    │                   │ • Zero-copy Arrow export  │
    └───────────────────────────┘                   └───────────────────────────┘
```

### 1. Delta Lake Tables (`delta://`, `deltalake://`)
Mount Delta Lake tables directly into TESH-Query without running Spark or Databricks clusters:

```bash
# Point to a single Delta table
teshq config --db-url "delta:///path/to/orders_delta"

# Point to a directory lake containing multiple Delta tables
teshq config --db-url "delta:///data/lakehouse"

# Point to remote cloud object storage
teshq config --db-url "delta://s3://my-analytics-bucket/delta_lake/sales"
```

- **Automatic Table Discovery**: TESH-Query scans for `_delta_log/` directories, discovers all tables in the hierarchy, and exposes them as queryable SQL views.
- **ACID & Time-Travel Aware**: Leverages native Rust `deltalake` (delta-rs) and DuckDB `delta_scan()` for zero-copy Arrow dataset evaluation.

---

### 2. Parquet Lakehouses (`lakehouse://`, `parquet://`)
Query directories of raw `.parquet` files and Hive-partitioned trees:

```bash
# Multi-table Parquet lakehouse
teshq config --db-url "lakehouse:///path/to/parquet_directory"

# Direct parquet folder
teshq config --db-url "parquet:///data/warehouse"
```

- **Partition Pruning**: Automatically recognizes Hive-partitioned structures (`year=2024/month=10/*.parquet`) and maps folder hierarchies into table schemas.

---

## 📤 High-Performance Modern Exporters

TESH-Query supports exporting query results across 5 production data formats with secure `0o600` permissions and automated DataFrame/dictionary coercion:

| Format | CLI Option | Python SDK Method | Best Used For |
| :--- | :--- | :--- | :--- |
| **Parquet** | `--save-parquet <path>` | `save_to_parquet(df, path)` | Columnar analytical storage, compressed size, Arrow compatibility |
| **JSON Lines** | `--save-jsonl <path>` | `save_to_jsonl(df, path)` | Streaming data pipelines, BigQuery/Snowflake ingestion |
| **CSV** | `--save-csv <path>` | `save_to_csv(df, path)` | Ad-hoc spreadsheet sharing |
| **Excel** | `--save-excel <path>` | `save_to_excel(df, path)` | Multi-sheet business reporting (`.xlsx`) |
| **SQLite** | `--save-sqlite <path>` | `save_to_sqlite(df, path)` | Local embedded relational analysis |

```bash
# Run natural language query and export directly to Parquet
teshq query "Show the top 10 batting averages in gold schema" --save-parquet ./top_batters.parquet --explain
```

---

## 🔍 Dynamic Dialect Detection

TESH-Query utilizes SQLAlchemy's `make_url()` parser to extract the canonical backend name from standard and compound connection URLs:

```python
from teshq.core.dialect import detect_dialect, SQLDialect

dialect = detect_dialect("databricks://token:***@host:443/default?http_path=...")
assert dialect == SQLDialect.DATABRICKS

delta_dialect = detect_dialect("delta:///path/to/lake")
assert delta_dialect == SQLDialect.DELTALAKE
```

### Dialect Mapping Logic
1. **Direct Mappings:** `databricks`, `delta`, `lakehouse`, `trino`, `presto`, `athena`, `snowflake`, `bigquery`, `duckdb`, `clickhouse`, `postgresql`, `mysql`, `sqlite`, `oracle`, `mssql`.
2. **Derivative Mappings:**
   - `cockroachdb` $\rightarrow$ maps to `SQLDialect.POSTGRESQL`.
   - `mariadb` $\rightarrow$ maps to `SQLDialect.MYSQL`.
   - `awsathena` $\rightarrow$ maps to `SQLDialect.ATHENA`.
3. **Universal Fallback:** Any unrecognized dialect dynamically defaults to `SQLDialect.GENERIC`, applying standard ANSI-SQL quoting and cross-platform constructs.

---

## 🏊 Dynamic Connection Pooling Strategy

Different database architectures require distinct pooling configurations:

| Topology | Target Databases | Pool Implementation | Configuration Details |
| :--- | :--- | :--- | :--- |
| **In-Memory** | `sqlite:///:memory:`, `duckdb:///:memory:` | `StaticPool` | Preserves table states across queries within the session. |
| **Lakehouses & Embedded** | Delta Lake, Parquet Lakehouses, local DuckDB/SQLite | `NullPool` | Disables persistent pooling to prevent thread-locking and file descriptor contention. Enables `check_same_thread=False` for thread concurrency. |
| **Cloud Warehouses & HTTP**| Databricks, Trino, Presto, Athena | `NullPool` / `QueuePool` | Configured with `pool_pre_ping=True` and `NullPool` on stateless HTTP/Thrift endpoints to prevent connection drops. |
| **Client-Server Relational** | PostgreSQL, MySQL, Oracle, MSSQL | `QueuePool` | Reusable connection pools (`pool_size=5`, `max_overflow=10`, `pool_pre_ping=True`, `pool_recycle=3600`). |

---

## ⏱️ Analytical Statement Timeout Handling

TESH-Query intelligently applies query timeouts:
- **Server RDBMS (Postgres, MySQL):** Issues `SET statement_timeout = 30000;` on connection checkout.
- **Analytical & Lakehouse Engines (Databricks, Delta Lake, DuckDB, Lakehouse, SQLite):** Automatically excluded from session timeout statements to prevent transaction aborts on embedded and HTTP engines.

---

## 🧭 Next Steps

- **[CLI Reference](07_cli_reference.md)** — Learn how to explore schemas, preview records, and export to Parquet.
- **[Python SDK Guide](08_python_sdk.md)** — Query Databricks and Lakehouses programmatically.
- **[Multi-Database Federation](05_multi_database_federation.md)** — Join queries across multiple database dialects.
