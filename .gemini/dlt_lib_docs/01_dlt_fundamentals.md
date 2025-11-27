# DLT (Data Load Tool) - Core Concepts & Architecture

## 1. CRITICAL DISAMBIGUATION
**dlt** refers to the open-source Python library `dlt`.
It is **NOT** "Databricks Delta Live Tables".
It is a client-side library that runs anywhere Python runs (local, Airflow, Dagster).

## 2. Core Hierarchy
The library operates on the following hierarchy:
1.  **Pipeline (`dlt.pipeline`)**: The main execution unit. It manages connections, destination loading, and state preservation.
2.  **Source (`@dlt.source`)**: A logical grouping of data streams (e.g., a Database Schema).
3.  **Resource (`@dlt.resource`)**: A specific stream of data (e.g., a Database Table).
4.  **Destination**: Where the data is loaded (e.g., Databricks/Delta Lake).

## 3. The Extraction Protocol
`dlt` extracts data using Python generators (`yield`).
* **Normalization:** `dlt` automatically normalizes nested JSON into relational tables (child tables) unless configured otherwise.
* **Schema Evolution:** `dlt` infers schema from data. New columns in the source are automatically added to the destination.

## 4. State Management
`dlt` maintains a state JSON to track incremental loading (e.g., last `updated_at` or WAL LSN).
* **Location:** This state is stored **at the destination** in a `_dlt_pipeline_state` table.
* **Usage:** It ensures that if a pipeline fails and restarts, it resumes from the last checkpoint.