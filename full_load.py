"""
Full Load Pipeline - Snapshot Operations

This module handles snapshot/initial load operations from PostgreSQL to Databricks.
Uses the standard SQL database source to perform complete table replication.

Usage:
    uv run full_load.py
    
Environment Variables:
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
    All credentials are loaded from .dlt/secrets.toml
"""

import dlt
from dlt.sources.sql_database import sql_database
import psycopg2
from dlt.sources.credentials import ConnectionStringCredentials
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

from utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()


def run_full_load():
    """
    Execute full snapshot load from PostgreSQL to Databricks.
    
    This function:
    1. Creates a dlt pipeline targeting the 'bronze' schema in Databricks
    2. Discovers all tables in PostgreSQL 'public' schema
    3. Filters out dlt internal tables (those starting with '_dlt')
    4. Loads all tables using 'replace' write disposition
    5. Uses parquet as the loader file format for efficiency
    """
    console.print(Panel.fit(
        "[bold yellow]FULL LOAD PIPELINE[/bold yellow]\n"
        "[italic]Snapshot Mode - Replace Disposition[/italic]",
        border_style="yellow"
    ))
    
    # Configure the Pipeline
    pipeline = dlt.pipeline(
        pipeline_name="postgres_prod_to_databricks",
        destination="databricks",
        dataset_name="bronze"
    )
    
    logger.info(f"Pipeline configured: [cyan]{pipeline.pipeline_name}[/cyan]")
    logger.info(f"Destination: [cyan]{pipeline.destination}[/cyan]")
    logger.info(f"Dataset: [cyan]{pipeline.dataset_name}[/cyan]")
    
    # Get credentials to discover tables
    creds = dlt.secrets.get("sources.pg_replication.credentials", ConnectionStringCredentials)
    
    # Connect and list tables from public schema
    logger.info("Discovering tables in PostgreSQL [cyan]'public'[/cyan] schema...")
    with psycopg2.connect(creds.to_native_representation()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            tables = [row[0] for row in cur.fetchall() if not row[0].startswith("_dlt")]
    
    logger.info(f"Found [bold green]{len(tables)}[/bold green] table(s) to replicate")
    logger.debug(f"Tables: {', '.join(tables)}")
    
    # Create SQL database source
    logger.info("Starting full snapshot load with [bold red]REPLACE[/bold red] disposition...")
    logger.warning("This will [bold red]DELETE[/bold red] all existing data in bronze schema tables!")
    
    snapshot_source = sql_database()
    
    # Run the pipeline with replace disposition
    info = pipeline.run(
        snapshot_source,
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

