# Postgres to Databricks CDC Pipeline

![CI Status](https://github.com/victor-antoniassi/postgres-to-databricks-cdc/actions/workflows/deploy.yml/badge.svg)
![Python Version](https://img.shields.io/badge/python-3.11%2B-blue)
![Code Style](https://img.shields.io/badge/code%20style-ruff-000000.svg)
![Type Checker](https://img.shields.io/badge/type%20checker-mypy-blue)
![License](https://img.shields.io/badge/license-MIT-green)

A high-performance **Data Ingestion Project** built with the **Python dlt library**. It is designed to **move data from PostgreSQL to Databricks** using **CDC** (Change Data Capture) for efficient synchronization.

Orchestrated natively by **Databricks Lakeflow Jobs**, this project serves as a robust blueprint for enterprise data replication.

## 🎯 Project Scope: Ingestion (EL)

This project focuses strictly on the **Extract & Load (EL)** phases of modern data engineering. Its primary goal is to establish a reliable **Bronze Layer (Raw Data)** in the Lakehouse.

*   **Role**: Ingestion Engine (Source to Bronze).
*   **Architecture Philosophy**: Decouples **Ingestion** from **Transformation**. By isolating the ingestion logic, we ensure that upstream failures do not break downstream business logic (Silver/Gold transformations), which can be handled separately by tools like dbt or Spark SQL.

## 🛠️ Tech Stack

*   **Core**: Python 3.11+, `dlt` (Data Load Tool), PySpark.
    *   **CDC Engine**: [dlt verified `pg_replication` source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication) (Wal2Json/pgoutput).
*   **Infrastructure**: Databricks (Unity Catalog, Volumes, Delta Lake), PostgreSQL.
*   **DevOps & CI/CD**: GitHub Actions, Databricks Asset Bundles (DABs), OAuth Service Principals.
*   **Quality Engineering**: `uv` (Package Mgmt), `ruff` (Linting), `mypy` (Static Analysis), `pytest` (Testing).

## 🚀 Features

*   **Near Real-Time Replication**: Streams `INSERT`, `UPDATE`, and `DELETE` operations continuously using PostgreSQL logical replication (`pgoutput`) with low latency.
*   **Dual-Mode Operation**:
    *   **Full Load Mode**: High-performance initial load of historical data.
    *   **CDC Mode**: Incremental updates with exactly-once processing semantics.
*   **Databricks Native**:
    *   Leverages Unity Catalog Volumes for efficient staging.
    *   Writes directly to Delta Tables with schema evolution.
*   **Enterprise CI/CD**:
    *   **Service Principal Authentication**: Secure OAuth M2M authentication for deployments.
    *   **Environment Isolation**: Strict separation of concerns with dedicated catalogs (`dev_`, `qa_`, `prod_`).
    *   **Automated Quality Gates**: Integrated Linting (Ruff), Type Checking (Mypy), and Unit Testing (Pytest).

## ⏱️ Scheduling & Triggers

The current Databricks Job definition is configured with **Manual Triggers** by default.

*   **Why?** Designed for **On-Demand Demonstration**. This allows reviewers to trigger execution immediately and verify results without waiting for scheduled windows or consuming idle compute resources in the demo environment.
*   **Production Recommendation:**
    *   **Hourly (1 hour):** Ideal balance for batching accumulated CDC data.
    *   **Continuous:** For low-latency requirements, switching the job to "Continuous" mode enables stream-like processing.

## 🏗️ Architecture

The pipeline operates in two mutually exclusive modes to ensure reliability and clean separation of concerns. It leverages Databricks Unity Catalog for data governance and utilizes **Volumes for intermediate data staging**.

*   **Destination Schema** (e.g., `bronze`): Holds the final, queryable Delta Tables.
*   **Staging Schema Pattern** (e.g., `bronze_staging`): During **CDC (`MERGE`) operations**, `dlt` automatically creates and manages a separate schema suffixed with `_staging` (e.g., `bronze_staging`). This schema holds temporary tables and volumes for atomic merge transactions. For **Full Load (`REPLACE`) operations**, staging predominantly occurs directly within volumes associated with the final `bronze` schema.

```mermaid
flowchart LR
    subgraph Source [Source System]
        PG[(PostgreSQL)]
    end

    subgraph Pipeline [dlt Orchestrator]
        direction TB
        FullLoad[Full Load Mode]
        CDC[CDC Mode]
    end
    
    subgraph Target [Databricks Unity Catalog]
        Volume[Staging Volume]
        Delta[(Delta Table)]
    end

    %% Data Flow
    PG -->|Snapshot| FullLoad
    PG -->|WAL Stream| CDC
    
    FullLoad -->|Parquet to Volume| Volume
    CDC -->|Parquet to Volume| Volume
    
    Volume -- REPLACE (Full Load) --> Delta
    Volume -- MERGE (CDC) --> Delta
    
    %% Styling
    style Source fill:#f9f,stroke:#333,stroke-width:2px
    style Target fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style Delta fill:#0277bd,stroke:#fff,color:#fff
```

> **Note regarding Terminology:** This documentation uses the term **Full Load** to describe the initial bulk load of data. Internally, this utilizes `dlt`'s `write_disposition="replace"` strategy. While `dlt` internally handles some state using "snapshots" (especially for logical replication), we strictly use "Full Load" to describe the user-facing operation of replacing the destination dataset with the source state.

## 📋 Prerequisites

*   **Python 3.11+**
*   **uv** (Fast Python package manager) - [Install Guide](https://github.com/astral-sh/uv)
*   **Databricks Workspace** (Unity Catalog enabled)
*   **PostgreSQL Database** with `wal_level=logical`

> **🐳 Docker Tip:** Don't have a Postgres instance handy? Spin one up in seconds:
> ```bash
> docker run -d --name postgres-cdc -e POSTGRES_PASSWORD=postgres -p 5432:5432 -c wal_level=logical postgres:15
> ```

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

### 3. Run Full Load (Initialize)
Perform the initial full load of your data.

```bash
uv run run_pipeline --mode full_load
```

### 4. Simulate Transactions (Optional)
Generate some fake sales data in your Postgres database to test CDC.

> **Requirement:** This script wraps an external generator. You must clone [day-1_sales_data_generator](https://github.com/victor-antoniassi/day-1_sales_data_generator) locally for it to work.

```bash
# Generate 5 inserts, 2 updates, 1 delete
uv run scripts/simulate_transactions.py 5 2 1
```

### 5. Run CDC Load (Incremental)
Capture the changes and merge them into Databricks.

```bash
uv run run_pipeline --mode cdc
```

### 6. Verify Data
Validate that inserts, updates, and deletes were correctly applied to the Delta tables.

```bash
uv run scripts/verify_data.py
```

## 🔄 CI/CD & Quality Engineering

This project implements a robust Continuous Integration and Continuous Deployment pipeline using **GitHub Actions**.

### Automated Quality Gates
Every Push and Pull Request triggers a strict validation pipeline:
1.  **Linting**: `uv run ruff check .` (Enforces PEP-8 and clean code style).
2.  **Type Checking**: `uv run mypy src/` (Static analysis for type safety).
3.  **Unit Testing**: `uv run pytest` (Ensures logic correctness).
    *   **Coverage**: Enforces minimum code coverage metrics (configured in `pyproject.toml`).

### Environment Strategy
Deployments are managed via **Databricks Asset Bundles (DABs)**.

> **Architecture Note:** This project implements a **Logical Isolation Strategy**. Instead of using separate Databricks Workspaces for Dev/QA/Prod (which is common in large enterprises for physical isolation), we simulate these environments within a **Single Workspace** using distinct **Unity Catalog Catalogs**. This provides strong data separation while keeping infrastructure lean.

| Environment | Catalog | Trigger | Authentication |
| :--- | :--- | :--- | :--- |
| **Development** | `dev_chinook_lakehouse` | Local CLI | User Credentials |
| **QA** | `qa_chinook_lakehouse` | Push to `main` | Service Principal |
| **Production** | `prod_chinook_lakehouse` | GitHub Release | Service Principal |

## ☁️ Deployment to Databricks

For production and QA, the pipeline is deployed automatically via CI/CD. For manual deployments or testing:

### 1. Setup Infrastructure (Catalogs)
Because this project simulates isolated environments using Unity Catalog, you must provision the catalogs manually before the first deployment:

```bash
databricks catalogs create dev_chinook_lakehouse
databricks catalogs create qa_chinook_lakehouse
databricks catalogs create prod_chinook_lakehouse
```

### 2. Setup Secrets in Databricks
The job uses Databricks Secrets to securely access the database.

```bash
databricks secrets create-scope dlt_scope
databricks secrets put-secret dlt_scope pg_connection_string --string-value "postgresql://user:pass@host:port/db"
```

### 3. Deploy Bundle (Manual / Dev)
Builds the Python wheel and uploads the job definition to your development environment.

```bash
databricks bundle deploy -t dev --profile DEFAULT
```

### 4. Run Jobs
Trigger the pipeline modes using parameters:

**Full Load Job:**
```bash
databricks bundle run postgres_cdc_job_definition --task-key full_load_task --profile DEFAULT
```

**CDC Stream Job:**
```bash
databricks bundle run postgres_cdc_job_definition --task-key cdc_load_task --profile DEFAULT
```

> **Note on Serverless:** If using Databricks Serverless, ensure your Network Policies allow egress to `us-east-2.storage.cloud.databricks.com` (or your region's storage endpoint), otherwise the job may fail with `Connection refused`.

## 📂 Project Structure

```
.
├── pyproject.toml             # Project definition, dependencies, and tool configs (Ruff, Mypy, Coverage)
├── databricks.yml             # Databricks Asset Bundle (DABs) definition for multi-env deployment
├── .github/workflows/         # CI/CD Pipeline definitions
├── src/
│   └── postgres_cdc/         # Main package
│       ├── __init__.py
│       ├── pipeline_main.py   # Main orchestrator (entry point)
│       ├── full_load.py       # Full Load pipeline logic
│       ├── cdc_load.py        # CDC incremental pipeline logic
│       ├── utils/             # Utility modules (logger)
│       └── pg_replication/    # Custom CDC source module
├── tests/                     # Unit tests
├── scripts/                   # Helper tools (outside package)
├── resources/                 # Databricks Job Definitions (YAML)
└── .dlt/                      # Local config and secrets
```

## 📜 License
MIT
