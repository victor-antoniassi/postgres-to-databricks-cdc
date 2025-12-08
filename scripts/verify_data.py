"""
Databricks Data Verification Script

Verifies that CDC transactions (Inserts, Updates, Deletes) were correctly
applied to the Databricks Delta tables in APPEND-ONLY mode.

Note: IDs are updated for the simulation run on 2025-12-08 (Second Run).
Inserts: 1615-1624
Updates: 1603, 562
Deletes: 513

Usage:
    uv run scripts/verify_data.py
"""

import os
import sys
import dlt
from databricks.sql import connect
from rich.panel import Panel
from rich.console import Console
from rich.table import Table

# Add parent directory to path to import utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from postgres_cdc.utils.logger import setup_logger

logger = setup_logger(__name__)
console = Console()

def verify():
    console.print(Panel.fit(
        "[bold cyan]DATABRICKS DATA VERIFICATION[/bold cyan]\n"
        "[italic]Checking Bronze Tables for CDC Changes (Append-Only Mode)[/italic]",
        border_style="cyan"
    ))

    # Load credentials
    try:
        host = dlt.secrets.get("destination.databricks.credentials.server_hostname", str)
        http_path = dlt.secrets.get("destination.databricks.credentials.http_path", str)
        # Try to get token, but don't fail if missing - connector might use other auth
        token = os.environ.get("DATABRICKS_TOKEN") or dlt.secrets.get("destination.databricks.credentials.access_token", str)
    except Exception as e:
        logger.error(f"Failed to load secrets: {e}")
        return

    target_table = "dev_chinook_lakehouse.bronze.invoice"

    # Prepare connection arguments
    connect_args = {
        "server_hostname": host,
        "http_path": http_path,
    }
    if token:
        connect_args["access_token"] = token
    else:
        logger.info("No access token provided. Attempting to connect using default credentials (e.g. .databrickscfg)...")

    try:
        with connect(**connect_args) as conn:
            with conn.cursor() as cursor:
                
                # 1. Verify INSERTS (IDs 1615 - 1624)
                logger.info("Verifying [bold green]INSERTS[/bold green] (Expected IDs: 1615-1624)...")
                query_inserts = f"""
                    SELECT invoice_id, customer_id, invoice_date, total, _dlt_load_id
                    FROM {target_table}
                    WHERE invoice_id BETWEEN 1615 AND 1624
                    ORDER BY invoice_id
                """
                cursor.execute(query_inserts)
                rows = cursor.fetchall()
                
                if rows:
                    table = Table(title="New Invoices Found", show_header=True)
                    table.add_column("InvoiceId", style="cyan")
                    table.add_column("Total", style="green")
                    table.add_column("Date", style="yellow")
                    for row in rows:
                        table.add_row(str(row.invoice_id), str(row.total), str(row.invoice_date))
                    console.print(table)
                    
                    if len(rows) >= 10:
                        logger.info(f"[bold green]✓ Success:[/bold green] Found {len(rows)} inserted records (Expected >= 10).")
                    else:
                        logger.warning(f"[bold yellow]![/bold yellow] Found {len(rows)}/10 records.")
                else:
                    logger.error("[bold red]✗ Failure:[/bold red] No inserted records found!")

                # 2. Verify DELETES (ID 513) - Append Mode Check
                # In append mode, the record should STILL exist (history preserved).
                logger.info("\nVerifying [bold red]DELETES[/bold red] (ID 513)...")
                logger.info("Strategy: Append-Only -> Record should still exist in bronze.")
                
                # Added deleted_ts to query to verify soft delete marker
                query_delete = f"SELECT invoice_id, total, deleted_ts, _dlt_load_id FROM {target_table} WHERE invoice_id = 513 ORDER BY _dlt_load_id"
                cursor.execute(query_delete)
                deleted_rows = cursor.fetchall()
                
                if deleted_rows:
                    logger.info(f"[bold green]✓ Success:[/bold green] Invoice 513 found ({len(deleted_rows)} versions). History preserved.")
                    
                    table = Table(title="Deleted Invoice History", show_header=True)
                    table.add_column("InvoiceId", style="cyan")
                    table.add_column("Total", style="green")
                    table.add_column("Deleted TS", style="red")
                    
                    for row in deleted_rows:
                        # Check if deleted_ts is present
                        is_deleted = row.deleted_ts is not None
                        style = "bold red" if is_deleted else "white"
                        ts_display = str(row.deleted_ts) if row.deleted_ts else "[dim]None[/dim]"
                        table.add_row(str(row.invoice_id), str(row.total), f"[{style}]{ts_display}[/{style}]")
                    
                    console.print(table)
                else:
                    logger.error("[bold red]✗ Failure:[/bold red] Invoice 513 NOT found! It should exist in append mode.")

                # 3. Verify UPDATES (IDs 1603, 562) - Append Mode Check
                # We expect multiple versions (rows) for these IDs if an update occurred.
                logger.info("\nVerifying [bold blue]UPDATES[/bold blue] (IDs 1603, 562)...")
                logger.info("Strategy: Append-Only -> Should find multiple versions of the record.")
                
                query_updates = f"""
                    SELECT invoice_id, count(*) as version_count
                    FROM {target_table}
                    WHERE invoice_id IN (1603, 562)
                    GROUP BY invoice_id
                """
                cursor.execute(query_updates)
                update_rows = cursor.fetchall()
                
                if update_rows:
                    table = Table(title="Updated Invoices Versions", show_header=True)
                    table.add_column("InvoiceId", style="cyan")
                    table.add_column("Version Count", style="white")
                    
                    success_count = 0
                    for row in update_rows:
                        count = row.version_count
                        style = "green" if count > 1 else "red"
                        table.add_row(str(row.invoice_id), f"[{style}]{count}[/{style}]")
                        if count > 1:
                            success_count += 1
                            
                    console.print(table)
                    
                    if success_count == len(update_rows):
                         logger.info(f"[bold green]✓ Success:[/bold green] All checked invoices have multiple versions.")
                    else:
                         logger.warning("[bold yellow]![/bold yellow] Some invoices do not have multiple versions (update might not have been captured).")
                else:
                    logger.error("[bold red]✗ Failure:[/bold red] Updated records not found.")

    except Exception as e:
        logger.error(f"Database connection or query failed: {e}")

if __name__ == "__main__":
    verify()
