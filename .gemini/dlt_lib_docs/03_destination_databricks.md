# Destination: Databricks (Delta Lake)

## 1. Architecture: The Staging Requirement
`dlt` does **NOT** insert data directly into Databricks row-by-row. It uses a **Staging** architecture.

**The Flow:**
1.  **Extract/Normalize:** `dlt` creates Parquet/JSONL files locally or in memory.
2.  **Staging:** `dlt` uploads these files to a "Staging Area" (Unity Catalog Volume, S3, or ADLS Gen2).
3.  **Load:** `dlt` executes a `COPY INTO` or `MERGE INTO` SQL command on the Databricks SQL Warehouse to load data from the files to the Delta Table.

## 2. Configuration (`secrets.toml`)
You need credentials for Databricks **AND** the Staging area.

**Modern Approach (Unity Catalog Volumes):**
```toml
[destination.databricks]
location = "my_volume_name" # The name of the UC Volume
credentials.host = "adb-xxxx.xx.azuredatabricks.net"
credentials.http_path = "/sql/1.0/warehouses/xxxx"
credentials.token = "dapi..." # PAT Token
````

**Legacy/External Approach (S3/ADLS):**
If not using Volumes, you must add a `[destination.databricks.staging]` block with AWS/Azure credentials.

## 3\. Naming Conventions

  * `dlt` normalizes table and column names to **snake\_case** (e.g., `CustomerName` -\> `customer_name`).
  * Internal tables (state, loads) are prefixed with `_dlt`.