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
from dlt.sources.credentials import ConnectionStringCredentials
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

# Import CDC source from local pg_replication module
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()


def run_cdc_load():
    """
    Execute CDC load from PostgreSQL to Databricks.
    
    This function:
    1. Creates a dlt pipeline targeting the 'bronze' schema in Databricks
    2. Discovers all tables in PostgreSQL 'public' schema
    3. Initializes replication slot and publication if not already created
    4. Consumes changes from PostgreSQL WAL (Write-Ahead Log)
    5. Applies changes using 'merge' write disposition
    6. Uses parquet as the loader file format for efficiency
    """
    console.print(Panel.fit(
        "[bold blue]CDC LOAD PIPELINE[/bold blue]\n"
        "[italic]Continuous Change Data Capture - Merge Disposition[/italic]",
        border_style="blue"
    ))
    
    # Configure the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="postgres_prod_to_databricks",
        destination="databricks",
        dataset_name="bronze"  # Target Schema in Unity Catalog
    )
    
    # Get replication configuration from secrets
    try:
        slot_name = dlt.secrets.get("sources.pg_replication.slot_name", str)
    except (KeyError, ValueError):
        slot_name = "dlt_cdc_slot"
    
    try:
        pub_name = dlt.secrets.get("sources.pg_replication.pub_name", str)
    except (KeyError, ValueError):
        pub_name = "dlt_cdc_pub"
    
    # Display CDC configuration
    config_table = Table(title="CDC Configuration", show_header=False, box=None)
    config_table.add_column("Property", style="cyan")
    config_table.add_column("Value", style="yellow")
    config_table.add_row("Replication Slot", slot_name)
    config_table.add_row("Publication", pub_name)
    config_table.add_row("Pipeline", pipeline.pipeline_name)
    config_table.add_row("Dataset", pipeline.dataset_name)
    console.print(config_table)
    
    # Get credentials to discover tables
    creds = dlt.secrets.get("sources.pg_replication.credentials", ConnectionStringCredentials)
    
    # Connect and list tables from public schema
    logger.info("Discovering tables in PostgreSQL [cyan]'public'[/cyan] schema...")
    with psycopg2.connect(creds.to_native_representation()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            # Filter out dlt internal tables (start with _dlt)
            tables = [row[0] for row in cur.fetchall() if not row[0].startswith("_dlt")]
    
    logger.info(f"Found [bold green]{len(tables)}[/bold green] table(s) to monitor")
    logger.debug(f"Tables: {', '.join(tables)}")
    
    # Initialize replication (creates slot/publication if needed)
    logger.info("Initializing replication slot and publication...")
    init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name="public",
        table_names=tables
    )
    logger.info("[bold green]✓[/bold green] Replication initialized successfully")
    
    # Create CDC source that reads the WAL
    logger.info("Starting CDC stream from PostgreSQL WAL...")
    cdc_source = replication_resource(
        slot_name=slot_name,
        pub_name=pub_name
    )
    
    # Run the pipeline with merge disposition
    # This ensures INSERT, UPDATE, DELETE operations are correctly applied
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
    
    # Create summary table
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

