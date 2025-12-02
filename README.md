# Postgres to Databricks CDC Pipeline

A production-grade data ingestion pipeline that replicates data from **PostgreSQL** (e.g., Neon, RDS) to **Databricks** (Delta Lake) using **Change Data Capture (CDC)**.

Built with [dlt (Data Load Tool)](https://dlthub.com/) and designed for orchestration with **Databricks Lakeflow Jobs**.

## 🚀 Features

*   **Real-Time Replication**: Streams `INSERT`, `UPDATE`, and `DELETE` operations continuously using PostgreSQL logical replication (`pgoutput`).
*   **Dual-Mode Operation**:
    *   **Snapshot Mode**: High-performance initial load of historical data.
    *   **CDC Mode**: Low-latency incremental updates with exactly-once processing.
*   **Databricks Native**:
    *   Leverages Unity Catalog Volumes for efficient staging.
    *   Writes directly to Delta Tables with schema evolution.
*   **Developer Friendly**:
    *   Fully functional locally (no Spark required for ingestion).
    *   Includes simulation scripts to generate transactions and test the pipeline end-to-end.
    *   Orchestrated via `pipeline_main.py` for easy integration with jobs.

## 🏗️ Architecture

The pipeline operates in two mutually exclusive modes to ensure reliability and clean separation of concerns:

```mermaid
graph LR
    PG[(PostgreSQL)] -->|WAL / pgoutput| CDC[CDC Pipeline]
    PG -->|SELECT *| Snapshot[Snapshot Pipeline]
    
    CDC -->|Parquet| VolCDC[Staging Volume]
    Snapshot -->|Parquet| VolSnap[Staging Volume]
    
    VolCDC -->|MERGE| Delta[(Databricks Delta Lake)]
    VolSnap -->|REPLACE| Delta
```

## 🛠️ Prerequisites

*   **Python 3.11+**
*   **uv** (Fast Python package manager) - [Install Guide](https://github.com/astral-sh/uv)
*   **Databricks Workspace** (Unity Catalog enabled)
*   **PostgreSQL Database** with `wal_level=logical`

## ⚡ Quick Start (Local Execution)

You can run the entire pipeline from your local machine. This is ideal for development, testing, and POCs.

### 1. Install Dependencies
```bash
uv sync
```

### 2. Configure Secrets
Create a `.dlt/secrets.toml` file in the project root with your credentials:

```toml
[sources.pg_replication.credentials]
database = "your_db"
password = "your_password"
host = "your_host"
port = 5432
username = "your_user"

[destination.databricks.credentials]
server_hostname = "dbc-xxxx.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/xxxx"
access_token = "dapi..." # Or use CLI profile if configured
```

> **Tip:** If you have the Databricks CLI configured, `dlt` can automatically use your `DEFAULT` profile credentials without putting them in `secrets.toml`.

### 3. Run Snapshot Load (Initialize)
Perform the initial full load of your data.

```bash
uv run full_load.py
```

### 4. Simulate Transactions (Optional)
Generate some fake sales data in your Postgres database to test CDC.

```bash
# Generate 5 inserts, 2 updates, 1 delete
uv run scripts/simulate_transactions.py 5 2 1
```

### 5. Run CDC Load (Incremental)
Capture the changes and merge them into Databricks.

```bash
uv run cdc_load.py
```

## ☁️ Deployment to Databricks (Production)

For production, deploy the pipeline as a Databricks Job using **Databricks Asset Bundles (DABs)**.

### 1. Setup Secrets in Databricks
The job uses Databricks Secrets to securely access the database.

```bash
databricks secrets create-scope dlt_scope
databricks secrets put-secret dlt_scope pg_connection_string --string-value "postgresql://user:pass@host:port/db"
```

### 2. Deploy Bundle
Builds the Python wheel and uploads the job definition.

```bash
databricks bundle deploy --profile DEFAULT
```

### 3. Run Jobs
Trigger the pipeline modes using parameters:

**Full Load Job:**
```bash
databricks bundle run postgres_cdc_job_definition --python-params "--mode,snapshot" --profile DEFAULT
```

**CDC Stream Job:**
```bash
databricks bundle run postgres_cdc_job_definition --python-params "--mode,cdc" --profile DEFAULT
```

> **Note on Serverless:** If using Databricks Serverless, ensure your Network Policies allow egress to `us-east-2.storage.cloud.databricks.com` (or your region's storage endpoint), otherwise the job may fail with `Connection refused`.

## 📂 Project Structure

```
.
├── pipeline_main.py           # Entry point for Databricks Jobs
├── full_load.py               # Logic for Snapshot Mode
├── cdc_load.py                # Logic for CDC Mode
├── pg_replication/            # Custom dlt source for Postgres Logical Replication
├── scripts/                   # Helper tools
│   ├── cleanup_databricks.py  # Resets the environment (Drops tables/schemas)
│   └── simulate_transactions.py # Generates test data
├── resources/                 # Databricks Job Definitions (YAML)
└── .dlt/                      # Local config and secrets
```

## 📜 License
MIT