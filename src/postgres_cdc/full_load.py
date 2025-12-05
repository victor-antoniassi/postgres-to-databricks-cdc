"""
Full Load Pipeline

This module handles full load operations from PostgreSQL to Databricks.
Uses the standard SQL database source to perform complete table replication.

Usage:
    uv run full_load.py
    
Environment Variables:
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
    All credentials are loaded from:
    - Local: .dlt/secrets.toml
    - Databricks: dbutils.secrets (scope: dlt_scope)
"""

import dlt
import os
from dlt.sources.sql_database import sql_database
import psycopg2
from dlt.sources.credentials import ConnectionStringCredentials
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

from .utils.logger import setup_logger

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

logger = setup_logger(__name__)
console = Console()


def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        return None

def get_secret(scope, key):
    """Get secret from Databricks dbutils or dlt secrets"""
    # Try dbutils first (for Databricks environment)
    if SparkSession:
        try:
            spark = SparkSession.builder.getOrCreate()
            dbutils = get_dbutils(spark)
            if dbutils:
                return dbutils.secrets.get(scope=scope, key=key)
        except Exception as e:
            logger.debug(f"Could not get dbutils secret: {e}")
    
    # Fallback to dlt secrets (local or env vars)
    return None


def run_full_load():
    """
    Execute full load from PostgreSQL to Databricks.
    
    This function:
    1. Creates a dlt pipeline targeting the 'bronze' schema in Databricks
    2. Discovers all tables in PostgreSQL 'public' schema
    3. Filters out dlt internal tables (those starting with '_dlt')
    4. Loads all tables using 'replace' write disposition
    5. Uses parquet as the loader file format for efficiency
    """
    console.print(Panel.fit(
        "[bold yellow]FULL LOAD PIPELINE[/bold yellow]\n"
        "[italic]Mode: Full Load - Replace Disposition[/italic]",
        border_style="yellow"
    ))
    
    # 1. Load Configuration and Secrets
    # Try to get connection string from Databricks secrets first
    pg_connection_string = get_secret("dlt_scope", "pg_connection_string")
    
    # Get target configuration from environment (injected by DABs/CI)
    target_catalog = os.environ.get("TARGET_CATALOG", "chinook_lakehouse")
    target_dataset = os.environ.get("TARGET_DATASET", "bronze")
    
    # Allow overriding connection details if needed (e.g. different warehouses for different envs)
    server_hostname = os.environ.get("DATABRICKS_SERVER_HOSTNAME", "dbc-2b79b085-f261.cloud.databricks.com")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/981a241885c8c6df")
    
    if pg_connection_string:
        logger.info("Loaded credentials from Databricks secrets")
        # Inject into environment so dlt config system can pick it up automatically
        os.environ["SOURCES__PG_REPLICATION__CREDENTIALS__CONNECTION_STRING"] = pg_connection_string
        os.environ["SOURCES__PG_REPLICATION__CREDENTIALS__DRIVERNAME"] = "postgresql"
        
        # Inject Databricks destination configuration
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = target_catalog
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = server_hostname
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = http_path
    else:
        logger.info("Attempting to load credentials from existing dlt secrets/env vars")

    # Configure the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="postgres_prod_to_databricks",
        destination="databricks",
        dataset_name=target_dataset
    )
    
    logger.info(f"Pipeline configured: [cyan]{pipeline.pipeline_name}[/cyan]")
    logger.info(f"Destination: [cyan]{pipeline.destination}[/cyan]")
    logger.info(f"Dataset: [cyan]{pipeline.dataset_name}[/cyan]")
    
    # Get credentials to discover tables
    if pg_connection_string:
        creds = ConnectionStringCredentials(pg_connection_string)
    else:
        creds = dlt.secrets.get("sources.pg_replication.credentials", ConnectionStringCredentials)
    
    if not creds:
         raise ValueError("Could not load PostgreSQL credentials. Check secrets or env vars.")

    # Connect and list tables from public schema
    logger.info("Discovering tables in PostgreSQL [cyan]'public'[/cyan] schema...")
    with psycopg2.connect(creds.to_native_representation()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            tables = [row[0] for row in cur.fetchall() if not row[0].startswith("_dlt")]
    
    logger.info(f"Found [bold green]{len(tables)}[/bold green] table(s) to replicate")
    logger.debug(f"Tables: {', '.join(tables)}")
    
    # Create SQL database source
    logger.info("Starting full load with [bold red]REPLACE[/bold red] disposition...")
    logger.warning("This will [bold red]DELETE[/bold red] all existing data in bronze schema tables!")
    
    full_load_source = sql_database(credentials=creds)
    
    # Run the pipeline with replace disposition
    info = pipeline.run(
        full_load_source,
        write_disposition="replace",
        loader_file_format="parquet"
    )
    
    # Display completion summary
    console.print(Panel.fit(
        "[bold green]✓ FULL LOAD COMPLETED[/bold green]",
        border_style="green"
    ))
    
    # Create summary table
    summary_table = Table(title="Pipeline Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Property", style="cyan")
    summary_table.add_column("Value", style="yellow")
    
    summary_table.add_row("Pipeline", pipeline.pipeline_name)
    summary_table.add_row("Destination", str(pipeline.destination))
    summary_table.add_row("Dataset", pipeline.dataset_name)
    summary_table.add_row("Tables Loaded", str(len(tables)))
    summary_table.add_row("Write Disposition", "replace")
    
    console.print(summary_table)
    logger.debug(f"Pipeline info: {info}")


if __name__ == "__main__":
    run_full_load()