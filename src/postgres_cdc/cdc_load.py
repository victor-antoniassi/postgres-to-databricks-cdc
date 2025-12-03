"""
CDC Load Pipeline - Continuous Change Data Capture

This module handles CDC (Change Data Capture) operations from PostgreSQL to Databricks.
Uses PostgreSQL logical replication to capture and apply incremental changes.

Usage:
    uv run cdc_load.py
    
Environment Variables:
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
    All credentials are loaded from .dlt/secrets.toml
    
Configuration:
    slot_name: Replication slot name (default: dlt_cdc_slot)
    pub_name: Publication name (default: dlt_cdc_pub)
"""

import dlt
import psycopg2
import os  # Added os import
from dlt.sources.credentials import ConnectionStringCredentials
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

from .pg_replication import replication_resource
from .pg_replication.helpers import init_replication
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


def run_cdc_load():
    """
    Execute CDC load from PostgreSQL to Databricks.
    """
    console.print(Panel.fit(
        "[bold blue]CDC LOAD PIPELINE[/bold blue]\n"
        "[italic]Continuous Change Data Capture - Merge Disposition[/italic]",
        border_style="blue"
    ))
    
    # 1. Load Configuration and Secrets
    # Try to get connection string from Databricks secrets first
    pg_connection_string = get_secret("dlt_scope", "pg_connection_string")
    
    if pg_connection_string:
        logger.info("Loaded credentials from Databricks secrets")
        # Inject into environment so dlt config system can pick it up automatically
        os.environ["SOURCES__PG_REPLICATION__CREDENTIALS__CONNECTION_STRING"] = pg_connection_string
        os.environ["SOURCES__PG_REPLICATION__CREDENTIALS__DRIVERNAME"] = "postgresql"
        
        # Inject Databricks destination configuration
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__CATALOG"] = "chinook_lakehouse"
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__SERVER_HOSTNAME"] = "dbc-2b79b085-f261.cloud.databricks.com"
        os.environ["DESTINATION__DATABRICKS__CREDENTIALS__HTTP_PATH"] = "/sql/1.0/warehouses/981a241885c8c6df"
    else:
        logger.info("Attempting to load credentials from existing dlt secrets/env vars")

    # Configure the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="postgres_prod_to_databricks",
        destination="databricks",
        dataset_name="bronze"
    )
    
    # Get replication configuration (will pick up from env vars or defaults)
    try:
        slot_name = dlt.config.get("sources.pg_replication.slot_name", str) or "dlt_cdc_slot"
    except (KeyError, ValueError):
        slot_name = "dlt_cdc_slot"
    
    try:
        pub_name = dlt.config.get("sources.pg_replication.pub_name", str) or "dlt_cdc_pub"
    except (KeyError, ValueError):
        pub_name = "dlt_cdc_pub"
        
    # Ensure defaults are set in env if missing, for consistency
    os.environ["SOURCES__PG_REPLICATION__SLOT_NAME"] = slot_name
    os.environ["SOURCES__PG_REPLICATION__PUB_NAME"] = pub_name

    # Display CDC configuration
    config_table = Table(title="CDC Configuration", show_header=False, box=None)
    config_table.add_column("Property", style="cyan")
    config_table.add_column("Value", style="yellow")
    config_table.add_row("Replication Slot", slot_name)
    config_table.add_row("Publication", pub_name)
    config_table.add_row("Pipeline", pipeline.pipeline_name)
    config_table.add_row("Dataset", pipeline.dataset_name)
    console.print(config_table)
    
    # Verify credentials explicitly for logging/discovery
    # If we loaded from secrets (pg_connection_string is set), instantiate directly
    if pg_connection_string:
        creds = ConnectionStringCredentials(pg_connection_string)
    else:
        # Fallback: try to load from dlt secrets/env vars if not found above
        creds = dlt.secrets.get("sources.pg_replication.credentials", ConnectionStringCredentials)

    if not creds:
         raise ValueError("Could not load PostgreSQL credentials. Check secrets or env vars.")

    # Connect and list tables from public schema
    logger.info("Discovering tables in PostgreSQL [cyan]'public'[/cyan] schema...")
    with psycopg2.connect(creds.to_native_representation()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            tables = [row[0] for row in cur.fetchall() if not row[0].startswith("_dlt")]
    
    logger.info(f"Found [bold green]{len(tables)}[/bold green] table(s) to monitor")
    logger.debug(f"Tables: {', '.join(tables)}")
    
    # Initialize replication slot and publication
    # We pass names explicitly but leave credentials to be picked up from env vars
    logger.info("Initializing replication slot and publication...")
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name="public",
        table_names=tables,
        credentials=creds
    )
    logger.info("[bold green]✓[/bold green] Replication initialized successfully")
    
    # Create CDC source that reads the WAL
    logger.info("Starting CDC stream from PostgreSQL WAL...")
    cdc_source = replication_resource(
        slot_name=slot_name,
        pub_name=pub_name,
        credentials=creds
    )
    
    # Run the pipeline with merge disposition
    info = pipeline.run(
        cdc_source,
        write_disposition="merge",
        loader_file_format="parquet"
    )
    
    # Display completion summary
    console.print(Panel.fit(
        "[bold green]✓ CDC LOAD COMPLETED[/bold green]",
        border_style="green"
    ))
    
    summary_table = Table(title="Pipeline Summary", show_header=True, header_style="bold cyan")
    summary_table.add_column("Property", style="cyan")
    summary_table.add_column("Value", style="yellow")
    
    summary_table.add_row("Pipeline", pipeline.pipeline_name)
    summary_table.add_row("Destination", str(pipeline.destination))
    summary_table.add_row("Dataset", pipeline.dataset_name)
    summary_table.add_row("Tables Monitored", str(len(tables)))
    summary_table.add_row("Write Disposition", "merge")
    
    console.print(summary_table)
    logger.debug(f"Pipeline info: {info}")


if __name__ == "__main__":
    run_cdc_load()

