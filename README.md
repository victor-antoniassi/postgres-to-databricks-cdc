# Postgres to Databricks CDC Pipeline

This project implements a robust data ingestion pipeline that replicates data from a **PostgreSQL** database (e.g., Neon) to **Databricks** (Delta Lake) using **Change Data Capture (CDC)**. It leverages the `dlt` library to handle both the initial historical load and continuous change replication.

## Features

*   **Dual-Mode Pipeline** (exclusive execution):
    *   **Snapshot Mode** (`full_load.py`): Performs a full load of existing data.
    *   **CDC Mode** (`cdc_load.py`): Continuously streams `INSERT`, `UPDATE`, and `DELETE` operations using PostgreSQL logical replication (`pgoutput`).
    *   **Orchestrator** (`pipeline_main.py`): Main entry point for Databricks Lakeflow Jobs.
*   **Databricks Integration**:
    *   Writes to Unity Catalog Volumes for staging.
    *   Loads data into Delta tables in the `bronze` schema.
    *   Uses `COPY INTO` for efficient loading.
*   **Robustness**:
    *   Automatic schema evolution.
    *   State management to resume from where it left off.
    *   Dedicated cleanup scripts for environment resets.

## Prerequisites

*   Python 3.11+
*   PostgreSQL database with logical replication enabled (`wal_level=logical`).
*   Databricks workspace with Unity Catalog enabled.

## Setup

1.  **Install Dependencies**:
    ```bash
    uv sync
    ```

2.  **Configure Secrets**:
    Create `.dlt/secrets.toml` with your credentials:
    ```toml
    [sources.pg_replication.credentials]
    drivername = "postgresql"
    database = "your_db"
    username = "your_user"
    password = "your_password"
    host = "your_host"
    port = 5432

    [sources.pg_replication]
    plugin_name = "pgoutput"
    slot_name = "dlt_cdc_slot"
    pub_name = "dlt_cdc_pub"

    [destination.databricks.credentials]
    server_hostname = "dbc-xxxx.cloud.databricks.com"
    http_path = "/sql/1.0/warehouses/xxxx"
    access_token = "dapi..."
    catalog = "chinook_lakehouse"
    ```

    > **Note**: `dlt` automatically creates staging volumes (`_dlt_staging_load_volume`) in both the destination schema (`bronze`) and staging schema (`bronze_staging`) as needed for snapshot and CDC operations respectively.

## Usage

### 1. Initial Snapshot (Full Load)
Run this once to backfill historical data.
```bash
# Direct execution
uv run full_load.py

# Or via orchestrator
uv run pipeline_main.py --mode snapshot
```

### 2. Continuous CDC
Run this to start streaming changes. It will run indefinitely until stopped.
```bash
# Direct execution
uv run cdc_load.py

# Or via orchestrator
uv run pipeline_main.py --mode cdc
```

### 3. Databricks Lakeflow Jobs
When configuring Lakeflow Jobs (Workflows), use the orchestrator:
```bash
# Full Load Job
uv run pipeline_main.py --mode snapshot

# CDC Job (continuous)
uv run pipeline_main.py --mode cdc
```

### 4. Simulation (Optional)
To verify CDC, you can simulate transactions in the source database:
```bash
# Generate 5 inserts, 2 updates, 1 delete
uv run scripts/simulate_transactions.py 5 2 1
```

### 5. Cleanup (Reset Environment)
**WARNING**: This deletes all tables in the target schema and files in the staging volume.
```bash
uv run scripts/cleanup_databricks.py
```

## Project Structure

*   **`pipeline_main.py`**: Main orchestrator for Databricks Lakeflow Jobs (routes to full_load or cdc_load).
*   **`full_load.py`**: Snapshot/initial load pipeline (replace mode).
*   **`cdc_load.py`**: CDC continuous pipeline (merge mode).
*   **`pg_replication/`**: `dlt` source code for Postgres CDC.
*   **`scripts/`**: Helper scripts:
    *   `cleanup_databricks.py`: Environment reset utility.
    *   `simulate_transactions.py`: Transaction simulation for testing.
    *   `list_volumes.py`: Volume verification utility.
*   **`.dlt/`**: Configuration and secrets.

## Architecture

The pipeline operates in two **mutually exclusive** modes:

1. **Snapshot Mode** (Full Load):
   - Uses `dlt.sources.sql_database`
   - Write disposition: `replace`
   - Target: `bronze` schema
   - Staging: `bronze._dlt_staging_load_volume`

2. **CDC Mode** (Incremental):
   - Uses `pg_replication` (PostgreSQL logical replication)
   - Write disposition: `merge`
   - Target: `bronze` schema
   - Staging: `bronze_staging._dlt_staging_load_volume`
   - State tracking: LSN (Log Sequence Number) in `dlt` state tables

> **Note**: Each Lakeflow Job run executes ONLY one mode, never both together.
