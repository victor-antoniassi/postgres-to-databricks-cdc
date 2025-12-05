"""
List Databricks Volumes

Displays volumes in bronze and bronze_staging schemas with rich formatted output.

Usage:
    uv run scripts/list_volumes.py
    
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
from rich.table import Table

# Updated import to match project structure
from postgres_cdc.utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()


def list_volumes():
    """List all volumes in bronze and bronze_staging schemas."""
    console.print(Panel.fit(
        "[bold cyan]DATABRICKS VOLUMES LISTING[/bold cyan]",
        border_style="cyan"
    ))
    
    # Load credentials
    host = dlt.secrets.get("destination.databricks.credentials.server_hostname", str)
    http_path = dlt.secrets.get("destination.databricks.credentials.http_path", str)
    token = dlt.secrets.get("destination.databricks.credentials.access_token", str)
    
    logger.info(f"Connecting to Databricks at [cyan]{host}[/cyan]...")
    
    with connect(server_hostname=host, http_path=http_path, access_token=token) as conn:
        with conn.cursor() as cursor:
            schemas_to_check = [
                ("chinook_lakehouse.bronze", "bronze"),
                ("chinook_lakehouse.bronze_staging", "bronze_staging")
            ]
            
            for schema_full, schema_name in schemas_to_check:
                # Create table for this schema
                volumes_table = Table(
                    title=f"Schema: {schema_full}",
                    show_header=True,
                    header_style="bold cyan"
                )
                volumes_table.add_column("Volume Name", style="yellow")
                
                try:
                    cursor.execute(f"SHOW VOLUMES IN {schema_full}")
                    volumes = cursor.fetchall()
                    
                    if volumes:
                        for vol in volumes:
                            volumes_table.add_row(vol.volume_name)
                        logger.info(f"Found [green]{len(volumes)}[/green] volume(s) in {schema_name}")
                    else:
                        volumes_table.add_row("[dim](No volumes)[/dim]")
                        logger.info(f"No volumes found in {schema_name}")
                    
                    console.print(volumes_table)
                    
                except Exception as e:
                    logger.error(f"Error listing volumes in {schema_full}: {e}")
                
                console.print()  # Add spacing between schemas
    
    console.print(Panel.fit(
        "[bold green]✓ LISTING COMPLETED[/bold green]",
        border_style="green"
    ))


if __name__ == "__main__":
    list_volumes()

