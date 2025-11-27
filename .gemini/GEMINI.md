# GEMINI.md - Technical Documentation for LLMs

## Project Overview
**Postgres to Databricks CDC Pipeline** - A production-ready dual-mode data ingestion pipeline that replicates data from PostgreSQL to Databricks using Change Data Capture (CDC) via the `dlt` library. Designed for orchestration with Databricks Lakeflow Jobs (formerly Workflows).

## Architecture

### High-Level Flow
```
PostgreSQL (Neon) → WAL (pgoutput) → dlt CDC Source → Databricks Delta Lake
```

### Two Operational Modes (Mutually Exclusive)

1. **Snapshot Mode** (Initial/Full Load)
   - Triggered by: `pipeline_main.py --mode snapshot` or direct `full_load.py` execution
   - Uses: `dlt.sources.sql_database`
   - Write disposition: `replace`
   - Data flow: Source → Parquet → Volume (`bronze._dlt_staging_load_volume`) → Delta Tables (`bronze` schema)

2. **CDC Mode** (Continuous Incremental)
   - Triggered by: `pipeline_main.py --mode cdc` or direct `cdc_load.py` execution
   - Uses: Custom `pg_replication` source (PostgreSQL logical replication)
   - Write disposition: `merge`
   - Data flow: WAL → Parquet → Volume (`bronze_staging._dlt_staging_load_volume`) → Staging Tables (`bronze_staging`) → MERGE → Delta Tables (`bronze`)

> **Important**: These modes are **mutually exclusive**. Each pipeline run executes ONLY one mode, never both together.

### Schema Structure

- **`bronze`**: Destination schema containing final Delta tables
- **`bronze_staging`**: Temporary schema for merge operations (auto-created by `dlt`)

### Volume Structure

**Both volumes are necessary:**
- **`bronze._dlt_staging_load_volume`**: Used for snapshot operations (replace mode)
- **`bronze_staging._dlt_staging_load_volume`**: Used for CDC operations (merge mode)

> **Important**: The existence of both volumes is by design to support hybrid operation.

## Key Components

### 1. Pipeline Orchestration

**`pipeline_main.py`** - Main orchestrator for Lakeflow Jobs
- Accepts `--mode` parameter: `snapshot` or `cdc` (required, mutually exclusive)
- Routes execution to appropriate pipeline module
- Single entry point for Databricks Lakeflow Jobs

**`full_load.py`** - Snapshot/Full Load Pipeline
- Configures `dlt.pipeline` with destination=databricks, dataset_name=bronze
- Dynamically discovers tables from PostgreSQL `public` schema
- Filters out `dlt` internal tables (prefix: `_dlt`)
- Executes with `write_disposition="replace"`

**`cdc_load.py`** - CDC Incremental Pipeline
- Configures `dlt.pipeline` with destination=databricks, dataset_name=bronze
- Initializes replication slot and publication
- Dynamically discovers tables from PostgreSQL `public` schema
- Filters out `dlt` internal tables (prefix: `_dlt`)
- Executes with `write_disposition="merge"`

### 2. CDC Source (`pg_replication/`)
- **Purpose**: Consume PostgreSQL Write-Ahead Log (WAL)
- **Plugin**: `pgoutput` (native PostgreSQL logical replication)
- **State Management**: Tracks LSN (Log Sequence Number) in `dlt` state
- **Replication Slot**: `dlt_cdc_slot` (configurable via secrets)
- **Publication**: `dlt_cdc_pub` (configurable via secrets)

### 3. Configuration Files

#### `.dlt/secrets.toml`
```toml
[sources.pg_replication.credentials]
# PostgreSQL connection (requires wal_level=logical)

[sources.pg_replication]
plugin_name = "pgoutput"
slot_name = "dlt_cdc_slot"
pub_name = "dlt_cdc_pub"

[destination.databricks.credentials]
# No staging_volume_name needed - dlt auto-creates volumes
```

#### `.dlt/config.toml`
- Minimal configuration (log level, telemetry)

### 4. Logging Configuration (`utils/logger.py`)
- **Framework**: Python standard `logging` library with `rich` console formatting
- **Features**:
  - Colored, formatted console output with timestamps
  - Configurable log levels via `LOG_LEVEL` environment variable
  - Beautiful exception tracebacks with local variables
  - Rich markup support in log messages
- **Log Levels**: DEBUG, INFO, WARNING, ERROR
- **Default Level**: INFO
- **Usage Example**:
  ```python
  from utils.logger import setup_logger
  logger = setup_logger(__name__)
  logger.info("Pipeline started")
  logger.warning("High memory usage")
  logger.error("Connection failed")
  ```

## Design Decisions

### Why Two Volumes?
**Decision**: Allow `dlt` to auto-create volumes in both schemas.

**Rationale**:
- Snapshot mode needs staging in destination schema (`bronze`)
- CDC mode needs staging in staging schema (`bronze_staging`)
- Auto-creation simplifies deployment (no manual volume setup required)
- Empty volumes consume minimal resources

**Alternative Considered**: Manual volume configuration via `staging_volume_name` → Rejected (adds complexity, no benefit)

### Why Exclusive Execution?
**Decision**: Enforce mutually exclusive execution modes (snapshot OR CDC, never both).

**Rationale**:
- Clear separation of concerns for Lakeflow Jobs orchestration
- Snapshot: Run once for initial load or recovery (scheduled or manual)
- CDC: Run continuously for incremental updates (scheduled infinitely)
- Prevents accidental hybrid execution that could cause confusion
- Simpler Lakeflow Jobs configuration with explicit mode selection

**Alternative Considered**: Hybrid mode in single run → Rejected (overly complex for orchestration, less flexible)

### Why NOT Hybrid Pipeline?
**Decision**: Remove hybrid execution capability from single pipeline run.

**Rationale**:
- Lakeflow Jobs work best with clear, single-purpose tasks
- Easier monitoring and debugging with separate jobs
- Snapshot jobs can be scheduled differently (one-time/periodic) vs CDC (continuous)
- Clearer failure isolation and retry logic



### Why `merge` Write Disposition for CDC?
**Decision**: Use `write_disposition="merge"` for CDC operations.

**Rationale**:
- Handles INSERT, UPDATE, DELETE operations correctly
- Ensures Delta tables reflect current source state
- Atomic operations via staging tables


## Deployment & Build System

### Build Stack
- **Frontend**: `uv` (Dependency management, environment pinning, build frontend)
- **Backend**: `hatchling` (Build backend for wheel generation)
- **Artifact**: Python Wheel (`.whl`) containing `pipeline_main.py` and packages.

### Databricks Asset Bundles (DABs)
- **Config**: `databricks.yml` references `uv build --wheel`.
- **Runtime**: Databricks Serverless (Python 3.12+).
- **Dependencies**: Installed via `requirements` in the Job definition, matching `pyproject.toml`.

### Critical Workarounds (Serverless)

#### 1. DLT Module Conflict
**Issue**: Databricks Serverless pre-loads internal `dlt` (Delta Live Tables) modules which conflict with the `dlt` library (dlthub).
**Fix**: In `pipeline_main.py`, `sys.meta_path` and `sys.modules` are patched *before* importing `dlt` to remove Databricks' internal hooks.

#### 2. Secret Injection
**Issue**: `python_wheel_task` in Serverless often ignores `spark_env_vars` or `environment_variables` injection from YAML.
**Fix**: 
- Secrets are stored in Databricks Secrets (`dlt_scope/pg_connection_string`).
- Code (`cdc_load.py`) explicitly retrieves secrets using `dbutils.secrets.get` (via `pyspark`).
- Secrets are manually injected into `os.environ` at runtime so `dlt` configuration helpers can find them.

## Common Operations

### Initial Setup
```bash
uv sync
# Configure .dlt/secrets.toml
```

### Logging
Control log verbosity with the `LOG_LEVEL` environment variable:
```bash
# Default (INFO level)
uv run pipeline_main.py --mode snapshot

# Debug mode (verbose output)
LOG_LEVEL=DEBUG uv run full_load.py

# Quiet mode (errors only)
LOG_LEVEL=ERROR uv run cdc_load.py

# Available levels: DEBUG, INFO, WARNING, ERROR
```

### Full Load (Snapshot)
```bash
# Direct execution
uv run full_load.py

# Or via orchestrator (recommended for Lakeflow Jobs)
uv run pipeline_main.py --mode snapshot
```

### Continuous CDC
```bash
# Direct execution
uv run cdc_load.py

# Or via orchestrator (recommended for Lakeflow Jobs)
uv run pipeline_main.py --mode cdc
```

### Lakeflow Jobs Configuration

**Full Load Job** (one-time or scheduled):
```python
# Task configuration
{
  "task_key": "full_load",
  "python_wheel_task": {
    "entry_point": "pipeline_main",
    "parameters": ["--mode", "snapshot"]
  }
}
```

**CDC Job** (continuous):
```python
# Task configuration
{
  "task_key": "cdc_stream",
  "python_wheel_task": {
    "entry_point": "pipeline_main",
    "parameters": ["--mode", "cdc"]
  }
}
```

### Simulate Transactions (Testing)
```bash
uv run scripts/simulate_transactions.py 5 2 1  # 5 inserts, 2 updates, 1 delete
```

### Environment Reset
```bash
uv run scripts/cleanup_databricks.py  # WARNING: Destructive
```

## Troubleshooting

### Issue: Pipeline fails with "Volume does not exist"
**Cause**: Volume was manually deleted or permissions issue.
**Solution**: `dlt` will auto-create on next run. Verify Databricks permissions (CREATE VOLUME).

### Issue: Tables persist in `bronze_staging`
**Cause**: Pipeline interrupted before cleanup completed.
**Solution**: Normal behavior. They'll be dropped/reused on next run. Use `cleanup_databricks.py` to force clean.

### Issue: CDC not capturing changes
**Cause**: Replication slot/publication misconfigured or permissions issue.
**Solution**: Verify PostgreSQL user has `REPLICATION` privilege. Check slot exists: `SELECT * FROM pg_replication_slots;`

### Issue: Duplicate volumes in both schemas
**Cause**: This is expected behavior (see "Why Two Volumes?" above).
**Solution**: No action needed.

## Dependencies
- Python 3.11+
- `dlt[databricks,postgres,sql-database]>=1.18.2`
- `rich>=13.7.0` (for enhanced logging)
- PostgreSQL with `wal_level=logical`
- Databricks with Unity Catalog

## Project Structure
```
.
├── pipeline_main.py           # Main orchestrator (routes to full_load or cdc_load)
├── full_load.py               # Snapshot/full load pipeline
├── cdc_load.py                # CDC incremental pipeline
├── utils/                     # Utility modules
│   ├── __init__.py
│   └── logger.py             # Centralized logging configuration
├── pg_replication/            # Custom CDC source (official dlt module)
│   ├── __init__.py           # replication_resource definition
│   ├── helpers.py            # Slot/pub management, item generation
│   ├── decoders.py           # WAL message decoding
│   └── schema_types.py       # Type mappings
├── scripts/
│   ├── cleanup_databricks.py # Environment reset
│   ├── simulate_transactions.py # Data generation
│   └── list_volumes.py       # Volume verification
├── .dlt/
│   ├── secrets.toml          # Credentials
│   └── config.toml           # Runtime config
└── README.md                 # User documentation
```

## State Management
- **Pipeline State**: Stored in `bronze._dlt_pipeline_state` table
- **CDC LSN**: Stored in resource state, persisted across runs
- **Replication Slot**: Server-side state in PostgreSQL

## Important Notes for Future Development
1. Each pipeline run executes ONLY one mode (snapshot OR CDC), enforcing clear separation of concerns
2. For Lakeflow Jobs: Create separate jobs for full_load and cdc_load with different schedules
3. Slot cleanup: If CDC pipeline is abandoned, manually drop the replication slot to avoid disk bloat
4. Schema evolution: `dlt` handles this automatically (new columns, new tables)
5. The staging schema (`bronze_staging`) is never explicitly created by our code - `dlt` manages it
6. Manual volume creation code was removed - rely on `dlt` defaults
