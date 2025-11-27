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

## Dependencies
- Python 3.12+ (Matched to Databricks Serverless Runtime 17.3+)
- `dlt[databricks,postgres,sql-database]>=1.18.2`
- `uv` for dependency management

## Deployment to Databricks

This project uses **Databricks Asset Bundles (DABs)** for deployment.

### 1. Setup Secrets
Before running the job, create the secret scope and connection string in Databricks:

```bash
# Create scope
databricks secrets create-scope dlt_scope

# Add Postgres connection string
# Format: postgresql://user:password@host:port/database
databricks secrets put-secret dlt_scope pg_connection_string --string-value "postgresql://..."
```

### 2. Deploy
Deploy the code as a wheel to your workspace:

```bash
databricks bundle deploy --profile DEFAULT
```

### 3. Run
Trigger the job manually or wait for the schedule:

```bash
databricks bundle run postgres_cdc_job_definition --profile DEFAULT
```

## Structure

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
