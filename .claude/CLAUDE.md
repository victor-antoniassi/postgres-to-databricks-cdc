# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Postgres to Databricks CDC Pipeline** - A production-ready data ingestion pipeline that replicates PostgreSQL data to Databricks Delta Lake using Change Data Capture (CDC) via the `dlt` library. Designed for orchestration with Databricks Lakeflow Jobs.

## Core Architecture

### Data Flow
```
PostgreSQL (WAL/pgoutput) → dlt → Parquet → Unity Catalog Volumes → Delta Tables
```

### Two Mutually Exclusive Execution Modes

The pipeline operates in **exactly one mode per execution** - never both:

1. **Snapshot Mode** (`full_load.py`)
   - Entry points: `uv run full_load.py` OR `uv run pipeline_main.py --mode snapshot`
   - Source: `dlt.sources.sql_database`
   - Write disposition: `replace` (destructive - deletes existing data)
   - Target: `bronze` schema
   - Staging: `bronze._dlt_staging_load_volume` (auto-created by dlt)
   - Use case: Initial load, recovery, or full refresh

2. **CDC Mode** (`cdc_load.py`)
   - Entry points: `uv run cdc_load.py` OR `uv run pipeline_main.py --mode cdc`
   - Source: Custom `pg_replication` module (PostgreSQL logical replication)
   - Write disposition: `merge` (handles INSERT/UPDATE/DELETE)
   - Target: `bronze` schema
   - Staging: `bronze_staging._dlt_staging_load_volume` (auto-created by dlt)
   - State tracking: LSN (Log Sequence Number) persisted in dlt state
   - Use case: Continuous incremental replication

### Schema & Volume Structure

**Schemas:**
- `bronze`: Final destination for all Delta tables
- `bronze_staging`: Temporary staging area for CDC merge operations (auto-managed by dlt)

**Volumes (both auto-created by dlt):**
- `bronze._dlt_staging_load_volume`: Used for snapshot operations
- `bronze_staging._dlt_staging_load_volume`: Used for CDC operations

> **Important**: Both volumes exist by design to support the two modes. This is intentional, not a bug.

## Development Commands

### Running Pipelines

```bash
# Snapshot mode (full load - destructive)
uv run full_load.py
uv run pipeline_main.py --mode snapshot

# CDC mode (incremental)
uv run cdc_load.py
uv run pipeline_main.py --mode cdc

# With debug logging
LOG_LEVEL=DEBUG uv run full_load.py
LOG_LEVEL=DEBUG uv run cdc_load.py
```

### Testing & Utilities

```bash
# Simulate database changes for CDC testing
uv run scripts/simulate_transactions.py 5 2 1  # 5 inserts, 2 updates, 1 delete

# List volumes in Databricks
uv run scripts/list_volumes.py

# Reset environment (DESTRUCTIVE - deletes all bronze schema data)
uv run scripts/cleanup_databricks.py
```

### Lakeflow Jobs Configuration

For Databricks workflows, always use `pipeline_main.py` as the entry point:

**Snapshot Job (one-time or scheduled):**
```python
{
  "task_key": "full_load",
  "python_wheel_task": {
    "entry_point": "pipeline_main",
    "parameters": ["--mode", "snapshot"]
  }
}
```

**CDC Job (continuous):**
```python
{
  "task_key": "cdc_stream",
  "python_wheel_task": {
    "entry_point": "pipeline_main",
    "parameters": ["--mode", "cdc"]
  }
}
```

## Key Implementation Details

### Pipeline Orchestration (`pipeline_main.py`)
- Single entry point for Lakeflow Jobs
- Accepts `--mode` parameter (snapshot/cdc) or `PIPELINE_MODE` env var
- Routes to appropriate module - enforces exclusive execution
- Never runs both modes together

### CDC Source (`pg_replication/`)
- **Purpose**: Consumes PostgreSQL Write-Ahead Log (WAL)
- **Plugin**: `pgoutput` (native PostgreSQL logical replication)
- **Replication Slot**: `dlt_cdc_slot` (configurable in secrets.toml)
- **Publication**: `dlt_cdc_pub` (configurable in secrets.toml)
- **State Management**: Tracks LSN in dlt state, persisted across runs
- **Key Files**:
  - `__init__.py`: `replication_resource` definition
  - `helpers.py`: Slot/publication management, LSN tracking
  - `decoders.py`: WAL message decoding
  - `schema_types.py`: PostgreSQL to Delta type mappings

### Table Discovery
Both modes dynamically discover tables from PostgreSQL `public` schema and filter out dlt internal tables (prefix `_dlt`).

### Logging (`utils/logger.py`)
- Framework: Python `logging` + `rich` for formatted console output
- Configurable via `LOG_LEVEL` env var (DEBUG, INFO, WARNING, ERROR)
- Default: INFO
- Features: Colored output, timestamps, beautiful exception tracebacks

## Configuration Files

### `.dlt/secrets.toml`
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
catalog = "your_catalog"
```

> **Note**: `staging_volume_name` is NOT needed - dlt auto-creates volumes.

### `.dlt/config.toml`
Minimal configuration for log level and telemetry settings.

## Critical Design Decisions

### Why Mutually Exclusive Modes?
- **Rationale**: Clearer Lakeflow Jobs orchestration, better monitoring/debugging, simpler failure isolation
- **Alternative Rejected**: Hybrid mode in single run (overly complex, less flexible)
- Each mode can be scheduled independently (snapshot: one-time/periodic, CDC: continuous)

### Why Two Volumes?
- **Rationale**:
  - Snapshot needs staging in destination schema (`bronze`)
  - CDC needs staging in staging schema (`bronze_staging`)
  - Auto-creation by dlt simplifies deployment (no manual setup)
  - Empty volumes consume minimal resources
- **Alternative Rejected**: Manual volume configuration via `staging_volume_name` (adds complexity, no benefit)

### Why `merge` for CDC?
- **Rationale**: Correctly handles INSERT/UPDATE/DELETE operations, ensures Delta tables reflect current state, atomic operations via staging tables

## PostgreSQL Requirements

- `wal_level=logical` (required for CDC)
- User must have `REPLICATION` privilege for CDC operations
- Verify replication slot: `SELECT * FROM pg_replication_slots;`

## State Management

- **Pipeline State**: Stored in `bronze._dlt_pipeline_state` table
- **CDC LSN**: Stored in resource state (`dlt.current.resource_state()`), persisted across runs
- **Replication Slot**: Server-side state in PostgreSQL

## Common Issues

### "Volume does not exist" error
- **Cause**: Volume manually deleted or permissions issue
- **Solution**: dlt auto-creates on next run. Verify Databricks CREATE VOLUME permissions.

### Tables persist in `bronze_staging`
- **Cause**: Pipeline interrupted before cleanup
- **Solution**: Normal behavior - reused on next run. Use `cleanup_databricks.py` to force clean.

### CDC not capturing changes
- **Cause**: Replication slot/publication misconfigured or permissions issue
- **Solution**: Verify PostgreSQL user has REPLICATION privilege. Check slot exists: `SELECT * FROM pg_replication_slots;`

### Duplicate volumes in both schemas
- **Cause**: Expected behavior (see "Why Two Volumes?" above)
- **Solution**: No action needed - this is by design.

## Important Development Notes

1. **Exclusive Execution**: Each pipeline run executes ONLY one mode - never both
2. **Lakeflow Jobs**: Create separate jobs for snapshot and CDC with different schedules
3. **Slot Cleanup**: If abandoning CDC, manually drop replication slot to avoid disk bloat:
   ```sql
   SELECT pg_drop_replication_slot('dlt_cdc_slot');
   ```
4. **Schema Evolution**: dlt handles automatically (new columns, new tables)
5. **No Manual Schema Creation**: `bronze_staging` schema is auto-managed by dlt
6. **No Manual Volume Creation**: Both staging volumes are auto-created by dlt

## Dependencies

- Python 3.11+
- `dlt[databricks,postgres,sql-database]>=1.18.2`
- `rich>=13.7.0` (for enhanced logging)
- PostgreSQL with `wal_level=logical`
- Databricks with Unity Catalog
