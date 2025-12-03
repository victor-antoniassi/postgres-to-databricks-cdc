"""
Databricks Environment Cleanup Script

WARNING: This script performs destructive operations!
It will delete schemas, tables, and volumes in the Databricks environment.

Usage:
    uv run scripts/cleanup_databricks.py
    
Environment Variables:
    LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR) - defaults to INFO
    All credentials are loaded from .dlt/secrets.toml
"""

import os
import sys
import dlt
from databricks.sdk import WorkspaceClient
from databricks.sql import connect
from rich.panel import Panel
from rich.console import Console

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from postgres_cdc.utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()


def cleanup():
    """
    Cleanup Databricks environment by removing schemas, tables, and volumes.
    
    WARNING: This is a destructive operation that will:
    - Drop bronze_postgres_raw and bronze_staging schemas (CASCADE)
    - Drop all tables in the bronze schema
    - Drop staging and raw volumes
    """
    console.print(Panel.fit(
        "[bold red]⚠️  DATABRICKS CLEANUP[/bold red]\n"
        "[yellow]Destructive Operation - Proceed with Caution[/yellow]",
        border_style="red"
    ))
    
    # Load credentials from secrets.toml
    host = dlt.secrets.get("destination.databricks.credentials.server_hostname", str)
    http_path = dlt.secrets.get("destination.databricks.credentials.http_path", str)
    token = dlt.secrets.get("destination.databricks.credentials.access_token", str)
    
    logger.info(f"Connecting to Databricks at [cyan]{host}[/cyan]...")
    
    # 1. Drop Tables and Extra Schemas using SQL Connector
    with connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            # 1. Drop extra schemas entirely
            extra_schemas = ["chinook_lakehouse.bronze_postgres_raw", "chinook_lakehouse.bronze_staging"]
            for schema in extra_schemas:
                logger.warning(f"Dropping schema (CASCADE): [red]{schema}[/red]")
                try:
                    cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                    logger.info(f"[green]✓[/green] Dropped schema: {schema}")
                except Exception as e:
                    logger.error(f"Error dropping schema {schema}: {e}")

            # 2. Clean 'bronze' schema (Drop tables and extra volumes)
            target_schema = "chinook_lakehouse.bronze"
            logger.info(f"Cleaning schema: [cyan]{target_schema}[/cyan]")
            
            # Drop all tables in bronze
            try:
                cursor.execute(f"SHOW TABLES IN {target_schema}")
                tables = cursor.fetchall()
                if tables:
                    for table in tables:
                        table_name = table.tableName
                        full_name = f"{target_schema}.{table_name}"
                        logger.warning(f"Dropping table: [red]{full_name}[/red]")
                        cursor.execute(f"DROP TABLE IF EXISTS {full_name}")
                    logger.info(f"[green]✓[/green] Dropped {len(tables)} table(s)")
                else:
                    logger.info("No tables found in bronze schema")
            except Exception as e:
                logger.error(f"Error listing/dropping tables in {target_schema}: {e}")

            # Drop extra volume in bronze if exists
            extra_volume = f"{target_schema}._dlt_staging_load_volume"
            logger.warning(f"Dropping volume: [red]{extra_volume}[/red]")
            try:
                cursor.execute(f"DROP VOLUME IF EXISTS {extra_volume}")
                logger.info(f"[green]✓[/green] Dropped volume: {extra_volume}")
            except Exception as e:
                logger.error(f"Error dropping volume {extra_volume}: {e}")

            # Drop the 'raw' volume so dlt has to recreate it
            raw_volume = "chinook_lakehouse.bronze.raw"
            logger.warning(f"Dropping volume: [red]{raw_volume}[/red]")
            try:
                cursor.execute(f"DROP VOLUME IF EXISTS {raw_volume}")
                logger.info(f"[green]✓[/green] Dropped volume: {raw_volume}")
            except Exception as e:
                logger.error(f"Error dropping volume {raw_volume}: {e}")

    # 2. Clean 'raw' Volume content using Databricks CLI
    volume_path = "dbfs:/Volumes/chinook_lakehouse/bronze/raw"
    logger.info(f"Cleaning volume content: [cyan]{volume_path}[/cyan]")
    
    import subprocess
    try:
        env = os.environ.copy()
        env["DATABRICKS_HOST"] = f"https://{host}"
        env["DATABRICKS_TOKEN"] = token
        
        cmd = ["databricks", "fs", "rm", "-r", volume_path]
        logger.debug(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        logger.info(f"[green]✓[/green] Volume cleaned successfully")
        
    except subprocess.CalledProcessError as e:
        logger.warning(f"Could not clean volume {volume_path}: {e.stderr}")
    except FileNotFoundError:
        logger.error("databricks CLI not found. Install it with: pip install databricks-cli")
    except Exception as e:
        logger.error(f"Unexpected error cleaning volume: {e}")
    
    console.print(Panel.fit(
        "[bold green]✓ CLEANUP COMPLETED[/bold green]",
        border_style="green"
    ))


if __name__ == "__main__":
    cleanup()

