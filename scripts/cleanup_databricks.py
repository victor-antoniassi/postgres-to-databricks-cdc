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
from databricks.sql import connect
from rich.panel import Panel
from rich.console import Console
import subprocess

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
    # Use DATABRICKS_TOKEN from env or from secrets.toml (uncommented)
    token = os.environ.get("DATABRICKS_TOKEN") or dlt.secrets.get("destination.databricks.credentials.access_token", str)
    
    if not token:
        logger.warning("Databricks access token not found. SQL cleanup might fail if authentication relies on it. CLI cleanup will use 'DEFAULT' profile.")

    logger.info(f"Connecting to Databricks at [cyan]{host}[/cyan]...")
    
    # Define relevant catalogs to clean
    relevant_catalogs = [
        "dev_chinook_lakehouse",
        "prd_chinook_lakehouse",
        "qa_chinook_lakehouse"
    ]

    # Try connecting. If token is missing, this might fail unless other auth methods are configured.
    try:
        with connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
            with conn.cursor() as cursor:
                for catalog in relevant_catalogs:
                    console.print(f"\n[bold blue]Processing Catalog: {catalog}[/bold blue]")

                    # 1. Drop extra schemas entirely
                    extra_schemas_to_drop = [
                        f"{catalog}.bronze_postgres_raw",
                        f"{catalog}.bronze_staging"
                    ]
                    for schema_full_name in extra_schemas_to_drop:
                        logger.warning(f"Dropping schema (CASCADE): [red]{schema_full_name}[/red]")
                        try:
                            cursor.execute(f"DROP SCHEMA IF EXISTS {schema_full_name} CASCADE")
                            logger.info(f"[green]✓[/green] Dropped schema: {schema_full_name}")
                        except Exception as e:
                            logger.error(f"Error dropping schema {schema_full_name}: {e}")

                    # 2. Clean 'bronze' schema (Drop tables and extra volumes)
                    target_bronze_schema = f"{catalog}.bronze"
                    logger.info(f"Cleaning schema: [cyan]{target_bronze_schema}[/cyan]")
                    
                    # Drop all tables in bronze
                    try:
                        cursor.execute(f"SHOW TABLES IN {target_bronze_schema}")
                        tables = cursor.fetchall()
                        if tables:
                            for table in tables:
                                table_name = table.tableName
                                full_name = f"{target_bronze_schema}.{table_name}"
                                logger.warning(f"Dropping table: [red]{full_name}[/red]")
                                cursor.execute(f"DROP TABLE IF EXISTS {full_name}")
                            logger.info(f"[green]✓[/green] Dropped {len(tables)} table(s) in {target_bronze_schema}")
                        else:
                            logger.info(f"No tables found in {target_bronze_schema}")
                    except Exception as e:
                        logger.error(f"Error listing/dropping tables in {target_bronze_schema}: {e}")

                    # Drop extra volume in bronze if exists
                    extra_volume = f"{target_bronze_schema}._dlt_staging_load_volume"
                    logger.warning(f"Dropping volume: [red]{extra_volume}[/red]")
                    try:
                        cursor.execute(f"DROP VOLUME IF EXISTS {extra_volume}")
                        logger.info(f"[green]✓[/green] Dropped volume: {extra_volume}")
                    except Exception as e:
                        logger.error(f"Error dropping volume {extra_volume}: {e}")

                    # Drop the 'raw' volume so dlt has to recreate it
                    raw_volume = f"{target_bronze_schema}.raw"
                    logger.warning(f"Dropping volume: [red]{raw_volume}[/red]")
                    try:
                        cursor.execute(f"DROP VOLUME IF EXISTS {raw_volume}")
                        logger.info(f"[green]✓[/green] Dropped volume: {raw_volume}")
                    except Exception as e:
                        logger.error(f"Error dropping volume {raw_volume}: {e}")
    except Exception as e:
        logger.error(f"SQL Connection failed: {e}. Ensure authentication credentials are valid.")

    # 2. Clean 'raw' Volume content using Databricks CLI
    # This section uses the Databricks CLI. Ensure it's configured for OAuth or DATABRICKS_TOKEN is set.
    # We iterate again through relevant catalogs for the CLI commands.
    for catalog in relevant_catalogs:
        volume_path_cli = f"dbfs:/Volumes/{catalog}/bronze/raw"
        logger.info(f"Cleaning volume content (CLI): [cyan]{volume_path_cli}[/cyan]")
        
        try:
            # Use --profile DEFAULT since user confirmed OAuth login
            cmd = ["databricks", "fs", "rm", "-r", volume_path_cli, "--profile", "DEFAULT"]
            logger.debug(f"Running command: {' '.join(cmd)}")
            subprocess.run(cmd, check=True, capture_output=True, text=True) # No need for env vars if using profile
            logger.info("[green]✓[/green] Volume cleaned successfully (CLI)")
            
        except subprocess.CalledProcessError as e:
            # Check if error is "not found" which is fine
            if "not found" in e.stderr.lower() or "no such file" in e.stderr.lower():
                logger.info(f"Volume path {volume_path_cli} not found (already clean).")
            else:
                logger.warning(f"Could not clean volume {volume_path_cli} via CLI: {e.stderr}")
        except FileNotFoundError:
            logger.error("databricks CLI not found. This error should not happen as CLI was verified.")
        except Exception as e:
            logger.error(f"Unexpected error cleaning volume {volume_path_cli} via CLI: {e}")
    
    console.print(Panel.fit(
        "[bold green]✓ CLEANUP COMPLETED[/bold green]",
        border_style="green"
    ))


if __name__ == "__main__":
    cleanup()

