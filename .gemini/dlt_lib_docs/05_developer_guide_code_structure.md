# Developer Guide: Generic Implementation Steps

## Step 1: Install & Initialize
To start the project, you must set up the environment and download the specific source logic for CDC.

1.  **Install:**
    ```bash
    pip install "dlt[databricks,postgres]"
    ```

2.  **Initialize:**
    Run the command to download the CDC logic locally. This is crucial because `pg_replication` requires custom logic files.
    ```bash
    dlt init pg_replication databricks
    ```
    *Result: This creates a `pg_replication/` folder in your root directory containing `__init__.py` and `helpers.py`.*

## Step 2: The Hybrid Script Strategy
You should implement a script that can handle both the **Initial Snapshot** (Backfill) and the **Continuous CDC**.

Create a file named `pipeline_main.py` at the root level:

```python
import dlt
import time

# Import 1: Standard SQL Source for Snapshot (from library)
from dlt.sources.sql_database import sql_database

# Import 2: CDC Source (from the LOCAL folder created by init)
# CRITICAL: Do NOT import from dlt.sources.sql_database.pg_replication
from pg_replication import pg_replication

def run_hybrid_pipeline():
    # 1. Configure the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="postgres_prod_to_databricks",
        destination="databricks",
        dataset_name="bronze_postgres_raw" # Unity Catalog Schema
    )

    # --- Phase A: Initial Snapshot (Run once or on demand) ---
    # This reads existing data using SELECT *
    # Uncomment this block if you need to backfill history
    """
    print("Starting Initial Snapshot...")
    snapshot_source = sql_database(
        # Credentials will be loaded from secrets.toml [sources.sql_database]
    )
    pipeline.run(
        snapshot_source, 
        write_disposition="replace", # Replaces old tables
        loader_file_format="parquet"
    )
    print("Snapshot complete.")
    """

    # --- Phase B: Continuous CDC (Run in a loop or scheduled) ---
    print("Starting CDC Stream...")
    
    # This source reads the WAL. 
    # Ensure [sources.pg_replication] is in secrets.toml
    cdc_source = pg_replication()

    # We use 'merge' so that Inserts/Updates/Deletes are applied to the Delta Table
    info = pipeline.run(
        cdc_source,
        write_disposition="merge",
        loader_file_format="parquet"
    )
    
    print(info)

if __name__ == "__main__":
    # In a real scenario, you might run this inside a loop or via an orchestrator
    run_hybrid_pipeline()
````

## Step 3: Verification Checklist

Instruct the user to verify the following points to ensure success:

1.  **Postgres Slot:** Run `SELECT * FROM pg_replication_slots;` to confirm `dlt` created the slot named in `secrets.toml`.
2.  **Databricks State:** Check if the table `_dlt_pipeline_state` exists in the destination schema. This proves `dlt` is tracking the WAL position (LSN).
3.  **Volume Usage:** Check the Unity Catalog Volume (or S3 bucket) to ensure Parquet files are being staged and then cleaned up after loading.