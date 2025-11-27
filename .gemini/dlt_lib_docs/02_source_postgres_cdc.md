# Source: PostgreSQL CDC (pg_replication)

## 1. Initialization
Unlike generic SQL sources, `pg_replication` has its own initialization command which downloads specific logic for WAL management.

```bash
dlt init pg_replication databricks
````

  * **Result:** Creates a local folder named `pg_replication/` containing `__init__.py` (the source logic).
  * **Importing:** You must import the source from this **local folder**, not just the library.

## 2\. Architecture: The "Hybrid" Strategy

CDC (Change Data Capture) via Logical Replication reads the WAL (Write-Ahead Log). It captures events from the moment the slot is created. It does **not** inherently read historical data existing before the slot.

**Recommended Production Strategy:**

1.  **Snapshot (Backfill):** Use the standard `sql_database` source to perform a `SELECT *` for historical data (using `write_disposition='replace'`).
2.  **Replication (CDC):** Use `pg_replication` to capture ongoing changes (using `write_disposition='merge'`).

## 3\. Configuration (`secrets.toml`)

The `dlt init` command will generate a structure similar to this. You usually need to configure the specific replication slot settings.

```toml
[sources.pg_replication]
# Connection string (User needs REPLICATION permissions)
credentials = "postgresql://loader:password@host:5432/db_name"

# Logical Replication Settings
plugin_name = "pgoutput"     # Standard for PG 10+
slot_name = "dlt_cdc_slot"   # dlt can create this if it doesn't exist
```

## 4\. Behavior Notes

  * **Deletes:** `pg_replication` captures `DELETE` events. If `write_disposition="merge"` is used, rows deleted in Postgres are deleted in Databricks.
  * **Transactions:** Events are yielded respecting transaction boundaries.